# ***************************************************************************************
# Title: LabAdvComp/parcel
# Author: Joshua S. Miller
# Date: May 26, 2016
# Code version: 0.1.13
# Availability: https://github.com/LabAdvComp/parcel
# ***************************************************************************************

import logging
import math
import os
import pickle
import random
import string
import tempfile
import time
import sys

from intervaltree import Interval, IntervalTree

from gdc_client.parcel.portability import OS_WINDOWS
from gdc_client.parcel.utils import (
    get_file_transfer_pbar,
    get_percentage_pbar,
    md5sum,
    mmap_open,
    STRIP,
    check_file_existence_and_size,
    validate_file_md5sum,
)
from gdc_client.parcel.const import SAVE_INTERVAL

if OS_WINDOWS:
    WINDOWS = True
    from queue import Queue
else:
    # if we are running on a posix system, then we will be
    # communicating across processes, and will need
    # multiprocessing manager
    from multiprocessing import Manager

    WINDOWS = False

log = logging.getLogger("segment")


class SegmentProducer(object):

    save_interval = SAVE_INTERVAL

    def __init__(self, download, n_procs):

        assert (
            download.size is not None
        ), "Segment producer passed uninitizalied Download!"

        self.download = download
        self.n_procs = n_procs
        self.pbar = None
        self.done = False
        # Initialize producer
        self.load_state()
        if not self.done:
            self._setup_pbar()
            self._setup_queues()
            self._setup_work()
            self.schedule()

    def _setup_pbar(self):
        self.pbar = get_file_transfer_pbar(self.download.url, self.download.size)

    def _setup_work(self):
        work_size = self.integrate(self.work_pool)
        self.block_size = work_size // self.n_procs
        self.total_tasks = math.ceil(work_size / self.block_size)
        log.debug("Total number of tasks: {0}".format(self.total_tasks))

    def _setup_queues(self):
        if WINDOWS:
            self.q_work = Queue()
            self.q_complete = Queue()
        else:
            manager = Manager()
            self.q_work = manager.Queue()
            self.q_complete = manager.Queue()

    def integrate(self, itree):
        return sum([i.end - i.begin for i in itree.items()])

    def validate_segment_md5sums(self, path=None):
        if not self.download.check_segment_md5sums:
            return True
        corrupt_segments = 0
        intervals = sorted(self.completed.items())

        log.debug("Checksumming {0}:".format(self.download.url))

        pbar = get_percentage_pbar(len(intervals))

        with mmap_open(path or self.download.path) as data:
            for interval in pbar(intervals):
                log.debug("Checking segment md5: {0}".format(interval))
                if not interval.data or "md5sum" not in interval.data:
                    log.error(
                        STRIP(
                            """User opted to check segment md5sums on restart.
                        Previous download did not record segment
                        md5sums (--no-segment-md5sums)."""
                        )
                    )
                    return
                chunk = data[interval.begin : interval.end]
                checksum = md5sum(chunk)
                if checksum != interval.data.get("md5sum"):
                    log.debug(
                        "Redownloading corrupt segment {0}, {1}.".format(
                            interval, checksum
                        )
                    )
                    corrupt_segments += 1
                    self.completed.remove(interval)

        if corrupt_segments:
            log.warning("Redownloading {0} corrupt segments.".format(corrupt_segments))

    def recover_intervals(self):
        """Recreate list of completed intervals and calculate remaining work pool"""
        state_file_exists = os.path.isfile(self.download.state_path)
        download_file_exists = os.path.isfile(self.download.path)
        temporary_file_exists = os.path.isfile(self.download.temp_path)

        # If the state file does not exist, treat as first time download
        if not state_file_exists:
            log.debug(
                "State file {0} does not exist. Beginning new download...".format(
                    self.download.state_path
                )
            )
            return False

        log.debug(
            "Found state file {0}, attempting to resume download".format(
                self.download.state_path
            )
        )
        # Attempt to load completed segments from state file
        try:
            with open(self.download.state_path, "rb") as f:
                self.completed = pickle.load(f)
            assert isinstance(
                self.completed, IntervalTree
            ), "Bad save state: {0}".format(self.download.state_path)
        except Exception as e:
            # An error has occured while loading state file.
            # Treat as entire file download and recreate temporary file
            log.error(
                "Unable to resume file state: {0}, will restart entire download".format(
                    str(e)
                )
            )
            return False

        # If the downloaded file exists, validate the downloaded file
        # If the file is not complete and correct, retry the entire download
        # Recreate the temporary file and return
        if download_file_exists:
            log.debug(
                "A file named {0} found, will attempt to validate file".format(
                    self.download.path
                )
            )

            if not self.is_complete(self.download.path):
                log.warning(
                    "Downloaded file is not complete, proceeding to restart entire download"
                )
                return False
            # check md5 sum
            try:
                validate_file_md5sum(self.download, self.download.path)
            except Exception as e:
                log.error(
                    "MD5 check of downloaded file failed due to following reason: {0}. Proceeding to restart entire download".format(
                        str(e)
                    )
                )
                return False

            log.debug("File is complete, will not attempt to re-download file.")
            # downloaded file is correct, set done flag in SegmentProducer
            self.done = True
            self.work_pool = IntervalTree()
            return True

        if not temporary_file_exists:
            log.debug(
                "State file exists but no previous partial file {0} detected. Restarting entire download.".format(
                    self.download.temp_path
                )
            )
            return False

        log.debug(
            "Partial file {0} detected. Validating already downloaded segments".format(
                self.download.temp_path
            )
        )

        # If temporary file exists, means that a previous download of the file
        # failed or was interrupted.
        # Check completed segments md5 sums of each completed segment
        self.validate_segment_md5sums(self.download.temp_path)
        log.debug("Segments checksum validation complete")
        self.size_complete = self.integrate(self.completed)
        log.debug("size complete: {0}".format(self.size_complete))
        # Remove already completed intervals from work_pool
        for interval in self.completed:
            self.work_pool.chop(interval.begin, interval.end)

        # If work_pool is empty by this point, something bad has happened with the download
        # In this case, retry entire download
        if self.work_pool.is_empty():
            log.debug("Partial file has been corrupted. Restarting entire download.")
            return False

        return True

    def set_default_intervals(self):
        """Establish default intervals and variables for dividing up work"""
        self.work_pool = IntervalTree([Interval(0, self.download.size)])
        self.completed = IntervalTree()
        self.size_complete = 0
        self.total_tasks = 0

    def load_state(self):
        self.set_default_intervals()

        if not self.recover_intervals():
            # Recovery failed, treat as new download
            self.download.setup_file()
            self.set_default_intervals()
            return

        log.debug("State loaded successfully")

    def save_state(self):
        try:
            # Grab a temp file in the same directory (hopefully avoud
            # cross device links) in order to atomically write our save file
            temp = tempfile.NamedTemporaryFile(
                prefix=".parcel_",
                dir=os.path.abspath(self.download.state_directory),
                delete=False,
            )
            # Write completed state
            pickle.dump(self.completed, temp)
            # Make sure all data is written to disk
            temp.flush()
            os.fsync(temp.fileno())
            temp.close()

            # Rename temp file as our save file, this could fail if
            # the state file and the temp directory are on different devices
            if OS_WINDOWS and os.path.exists(self.download.state_path):
                # If we're on windows, there's not much we can do here
                # except stash the old state file, rename the new one,
                # and back up if there is a problem.
                old_path = os.path.join(
                    tempfile.gettempdir(),
                    "".join(
                        random.choice(string.ascii_lowercase + string.digits)
                        for _ in range(10)
                    ),
                )
                try:
                    # stash the old state file
                    os.rename(self.download.state_path, old_path)
                    # move the new state file into place
                    os.rename(temp.name, self.download.state_path)
                    # if no exception, then delete the old stash
                    os.remove(old_path)
                except Exception as msg:
                    log.error("Unable to write state file: {0}".format(msg))
                    try:
                        os.rename(old_path, self.download.state_path)
                    except:
                        pass
                    raise
            else:
                # If we're not on windows, then we'll just try to
                # atomically rename the file
                os.rename(temp.name, self.download.state_path)

        except KeyboardInterrupt:
            log.warning("Keyboard interrupt. removing temp save file".format(temp.name))
            temp.close()
            os.remove(temp.name)
        except Exception as e:
            log.error("Unable to save state: {0}".format(str(e)))
            raise

    def schedule(self):
        while True:
            interval = self._get_next_interval()
            log.debug("Returning interval: {0}".format(interval))
            if not interval:
                return
            self.q_work.put(interval)

    def _get_next_interval(self):
        intervals = sorted(self.work_pool.items())
        if not intervals:
            return None
        interval = intervals[0]
        start = interval.begin
        end = min(interval.end, start + self.block_size)
        self.work_pool.chop(start, end)
        return Interval(start, end)

    def print_progress(self):
        if not self.pbar:
            return

        pbar_value = min(self.pbar.max_value, self.size_complete)

        try:
            self.pbar.update(pbar_value)
        except Exception as e:
            log.debug("Unable to update pbar: {}".format(str(e)))

    def check_file_exists_and_size(self, file_path):
        if self.download.is_regular_file:
            return check_file_existence_and_size(file_path, self.download.size)
        else:
            log.debug("File is not a regular file, refusing to check size.")
            return os.path.exists(file_path)

    def is_complete(self, file_path):
        return self.integrate(
            self.completed
        ) == self.download.size and self.check_file_exists_and_size(file_path)

    def finish_download(self):
        # Tell the children there is no more work, each child should
        # pull one NoneType from the queue and exit
        for i in range(self.n_procs):
            self.q_work.put(None)

        # Wait for all the children to exit by checking to make sure
        # that everyone has taken their NoneType from the queue.
        # Otherwise, the segment producer will exit before the
        # children return, causing them to read from a closed queue
        log.debug("Waiting for children to report")
        while not self.q_work.empty():
            time.sleep(0.1)

        # Finish the progressbar
        self.pbar.finish()

    def wait_for_completion(self):
        try:
            since_save = 0
            num_tasks_completed = 0
            while num_tasks_completed != self.total_tasks:
                while since_save < self.save_interval:
                    interval = self.q_complete.get()
                    # Once a process completes a tasks (sucess or failure),
                    # it will return a sentinel value (None) to main process
                    # to indicate that a task was completed
                    if interval is None:
                        num_tasks_completed += 1
                        if num_tasks_completed == self.total_tasks:
                            break
                        continue

                    self.completed.add(interval)

                    # Get bytes downloaded and update progress bar
                    this_size = interval.end - interval.begin
                    self.size_complete += this_size
                    since_save += this_size

                    self.print_progress()

                since_save = 0
                self.save_state()
        finally:
            self.finish_download()
