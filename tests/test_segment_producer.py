import collections
import logging
import os
import pathlib
import pickle
import pytest
import typing

import intervaltree as itree

import gdc_client.parcel.segment as segment
import gdc_client.parcel.download_stream as stream
import gdc_client.parcel.utils as utils

directories_tuple = collections.namedtuple(
    "directories", ["base_directory", "data_directory", "state_directory"]
)

data_info = collections.namedtuple("data_info", ["data", "md5sum"])


@pytest.fixture()
def setup_directories(tmp_path: pathlib.Path):
    data_directory = tmp_path.joinpath("test")
    state_directory = tmp_path.joinpath("test/logs")
    tmp_path.mkdir(exist_ok=True)
    data_directory.mkdir(exist_ok=True)
    state_directory.mkdir(exist_ok=True)

    return directories_tuple(tmp_path, data_directory, state_directory)


@pytest.fixture()
def complete_data():
    data = b"A" * 1024
    return data_info(data, utils.md5sum(data))


@pytest.fixture()
def incomplete_data():
    data = b"A" * 512
    return data_info(data, utils.md5sum(data))


@pytest.fixture()
def mock_download_stream(monkeypatch, setup_directories: typing.NamedTuple):
    def mock_init(self):
        self.initialized = True
        self.name = "test.txt"
        self.size = 1024
        self.md5sum = "d47b127bc2de2d687ddc82dac354c415"

    monkeypatch.setattr(stream.DownloadStream, "init", mock_init)

    download_stream = stream.DownloadStream(
        url="https://localhost:80/data/test",
        directory=str(setup_directories.base_directory.resolve()),
        token=None,
    )
    download_stream.init()

    return download_stream


@pytest.fixture(autouse=True)
def mock_schedule(monkeypatch):
    """Mock SegmentProducer.schedule() to not immediately fill up work queue with Intervals"""

    def mock_schedule(self):
        return

    monkeypatch.setattr(segment.SegmentProducer, "schedule", mock_schedule)


@pytest.fixture()
def mock_complete_download_file(
    setup_directories: typing.NamedTuple, complete_data: typing.NamedTuple
):
    write_data_file(setup_directories.data_directory, "test.txt", complete_data.data)


@pytest.fixture()
def mock_incomplete_download_file(
    setup_directories: typing.NamedTuple, incomplete_data: typing.NamedTuple
):
    write_data_file(setup_directories.data_directory, "test.txt", incomplete_data.data)


@pytest.fixture()
def mock_temporary_file(
    setup_directories: typing.NamedTuple, incomplete_data: typing.NamedTuple
):
    write_data_file(
        setup_directories.data_directory, "test.txt.partial", incomplete_data.data
    )


@pytest.fixture()
def mock_complete_state_file(
    setup_directories: typing.NamedTuple, complete_data: typing.NamedTuple
):
    write_state_file(
        setup_directories.state_directory, "test.txt.parcel", complete_data
    )


@pytest.fixture()
def mock_incomplete_state_file(
    setup_directories: typing.NamedTuple, incomplete_data: typing.NamedTuple
):
    write_state_file(
        setup_directories.state_directory, "test.txt.parcel", incomplete_data
    )


def write_data_file(directory: pathlib.Path, file_name: str, data: bytes):
    file_path = directory.joinpath(file_name)
    with file_path.open("wb") as f:
        f.write(data)


def write_state_file(directory: pathlib.Path, file_name: str, data: typing.NamedTuple):
    file_path = directory.joinpath(file_name)
    interval = itree.Interval(0, len(data.data), {"md5sum": data.md5sum})
    completed = itree.IntervalTree([interval])
    with file_path.open("wb") as f:
        pickle.dump(completed, f)


def test_load_state_no_state_file(
    # test when no state file is present
    mock_download_stream: stream.DownloadStream,
    complete_data: typing.NamedTuple,
):
    producer = segment.SegmentProducer(mock_download_stream, 2)
    assert os.path.isfile(mock_download_stream.temp_path)
    assert len(producer.completed.items()) == 0
    intervals = list(producer.work_pool.items())
    assert len(intervals) == 1
    assert intervals[0].begin == 0
    assert intervals[0].end == len(complete_data.data)
    assert producer.done == False


@pytest.mark.usefixtures("mock_complete_state_file", "mock_complete_download_file")
def test_load_state_complete_download_exists(
    mock_download_stream: stream.DownloadStream, complete_data: typing.NamedTuple
):
    # test when state file is present and download file is present and complete
    producer = segment.SegmentProducer(mock_download_stream, 2)
    assert not os.path.isfile(mock_download_stream.temp_path)
    assert len(producer.work_pool.items()) == 0
    intervals = list(producer.completed.items())
    assert len(intervals) == 1
    assert intervals[0].begin == 0
    assert intervals[0].end == len(complete_data.data)
    assert intervals[0].data["md5sum"] == complete_data.md5sum
    assert producer.done == True


@pytest.mark.usefixtures("mock_incomplete_state_file", "mock_incomplete_download_file")
def test_load_state_incomplete_download_exists(
    # test when state file is present and download file is present and is incomplete
    mock_download_stream: stream.DownloadStream,
    complete_data: typing.NamedTuple,
    incomplete_data: typing.NamedTuple,
):
    producer = segment.SegmentProducer(mock_download_stream, 2)
    assert os.path.isfile(mock_download_stream.temp_path)
    assert len(producer.completed.items()) == 0
    intervals = list(producer.work_pool.items())
    assert len(intervals) == 1
    assert intervals[0].begin == 0
    assert intervals[0].end == len(complete_data.data)
    assert producer.done == False


@pytest.mark.usefixtures("mock_incomplete_state_file", "mock_temporary_file")
def test_load_state_state_temp_exist(
    mock_download_stream: stream.DownloadStream,
    complete_data: typing.NamedTuple,
    incomplete_data: typing.NamedTuple,
):
    # test when state file is present and temporary file is present
    producer = segment.SegmentProducer(mock_download_stream, 2)
    assert os.path.isfile(mock_download_stream.temp_path)
    # check that completed intervals are correct
    intervals = list(producer.completed.items())
    assert len(intervals) == 1
    assert intervals[0].begin == 0
    assert intervals[0].end == len(incomplete_data.data)
    assert intervals[0].data["md5sum"] == incomplete_data.md5sum
    intervals = list(producer.work_pool.items())
    assert len(intervals) == 1
    assert intervals[0].begin == len(incomplete_data.data)
    assert intervals[0].end == len(complete_data.data)
    assert producer.done == False
