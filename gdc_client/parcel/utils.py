# ***************************************************************************************
# Title: LabAdvComp/parcel
# Author: Joshua S. Miller
# Date: May 26, 2016
# Code version: 0.1.13
# Availability: https://github.com/LabAdvComp/parcel
# ***************************************************************************************

from contextlib import contextmanager
import hashlib
import logging
import mmap
import os
import requests
import stat

from tqdm import tqdm

# Logging
log = logging.getLogger("utils")

# Silence warnings from requests
try:
    requests.packages.urllib3.disable_warnings()
except Exception as e:
    log.debug("Unable to silence requests warnings: {0}".format(str(e)))


def check_transfer_size(actual, expected):
    """Simple validation on any expected versus actual sizes.

    :param int actual: The size that was actually transferred
    :param int actual: The size that was expected to be transferred

    """

    return actual == expected


def get_pbar(file_id, maxval, start_val=0, desc="Downloading"):
    """Create and initialize a custom progressbar

    Args:
        file_id: file_id to include info about
        maxval: maximum value for the progress bar
        start_val: initial value for the progress bar
        desc: display message next to the file_id

    Returns:
        tqdm: progress bar instance
    """
    log.debug("Downloading {0}:".format(file_id))
    pbar = tqdm(
        total=maxval,
        initial=start_val,
        unit_scale=True,
        unit="B",
        desc="{} {}".format(desc, file_id),
        ascii=True,
    )
    return pbar


def print_opening_header(file_id):
    log.debug("")
    log.debug(
        "v{0}v".format("{s:{c}^{n}}".format(s=" {0} ".format(file_id), n=50, c="-"))
    )


def print_closing_header(file_id):
    log.debug(
        "^{0}^".format("{s:{c}^{n}}".format(s=" {0} ".format(file_id), n=50, c="-"))
    )


def write_offset(path, data, offset):
    with open(path, "r+b") as f:
        f.seek(offset)
        f.write(data)


def read_offset(path, offset, size):
    with open(path, "r+b") as f:
        f.seek(offset)
        data = f.read(size)
        return data


def set_file_length(path, length):
    if os.path.isfile(path) and os.path.getsize(path) == length:
        return
    with open(path, "wb") as f:
        f.seek(length - 1)
        f.write(b"\0")
        f.truncate()


def remove_partial_extension(path):
    try:
        if not path.endswith(".partial"):
            log.warning("No partial extension found")
            log.warning("Got {0}".format(path))
            return
        log.debug("renaming to {0}".format(path.replace(".partial", "")))
        os.rename(path, path.replace(".partial", ""))
    except Exception as e:
        raise Exception("Unable to remove partial extension: {0}".format(str(e)))


def check_file_existence_and_size(path, size):
    return os.path.isfile(path) and os.path.getsize(path) == size


def get_file_type(path):
    try:
        mode = os.stat(path).st_mode
        if stat.S_ISDIR(mode):
            return "directory"
        elif stat.S_ISCHR(mode):
            return "character device"
        elif stat.S_ISBLK(mode):
            return "block device"
        elif stat.S_ISREG(mode):
            return "regular"
        elif stat.S_ISFIFO(mode):
            return "fifo"
        elif stat.S_ISLNK(mode):
            return "link"
        elif stat.S_ISSOCK(mode):
            return "socket"
        else:
            return "unknown"
    except Exception as e:
        raise RuntimeError("Unable to get file type: {0}".format(str(e)))


def calculate_segments(start, stop, block):
    """return a list of blocks in sizes no larger than `block`, the last
    block can be smaller.

    """
    return [(a, min(stop, a + block) - 1) for a in range(start, stop, block)]


def md5sum(block):
    m = hashlib.md5()
    m.update(block)
    return m.hexdigest()


def md5sum_whole_file(fname):
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f, get_pbar(fname, None, desc="Validating md5sum") as pbar:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
            pbar.update(len(chunk))

    return hash_md5.hexdigest()


@contextmanager
def mmap_open(path):
    try:
        with open(path, "r+b") as f:
            mm = mmap.mmap(f.fileno(), 0)
            yield mm
    except Exception as e:
        raise RuntimeError("Unable to get file type: {0}".format(str(e)))


def STRIP(comment):
    return " ".join(comment.split())
