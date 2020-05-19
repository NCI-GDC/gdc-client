import pytest
from typing import List, Iterable, Mapping

from conftest import uuids
from gdc_client.parcel.const import HTTP_CHUNK_SIZE
from gdc_client.query.index import GDCIndexClient
from gdc_client.query.versions import _chunk_list, get_latest_versions

# default values for flask
BASE_URL = "http://127.0.0.1:5000"


@pytest.mark.usefixtures("setup_mock_server")
class TestQueryIndex:
    def setup_method(self):
        self.index = GDCIndexClient(uri=BASE_URL)

    def assert_index_with_uuids(self, uuid: str) -> None:
        assert self.index.get_access(uuid) == uuids[uuid]["access"]
        assert self.index.get_filesize(uuid) == uuids[uuid]["file_size"]
        assert self.index.get_md5sum(uuid) == uuids[uuid]["md5sum"]
        assert self.index.get_related_files(uuid) == uuids[uuid]["related_files"]
        assert self.index.get_annotations(uuid) == uuids[uuid]["annotations"]

    def assert_invalid_index(self):
        self.assert_index_with_uuids("invalid")

    ############ mock metadata ############
    @pytest.mark.parametrize(
        "uuid", ["small", "small_no_friends", "small_ann", "small_rel"]
    )
    def test_full_mock_get_metadata(self, uuid: str) -> None:
        self.index._get_metadata([uuid])

        self.assert_index_with_uuids(uuid)

    ############ mock separate files ############
    @pytest.mark.parametrize(
        "input_uuids,expected_bigs,expected_smalls",
        [
            (["small"], ["small"], []),
            (["small_no_friends"], [], [["small_no_friends"]]),
            (["big"], ["big"], []),
            (["big", "small"], ["big", "small"], []),
            (
                ["big_no_friends", "small_no_friends"],
                ["big_no_friends"],
                [["small_no_friends"]],
            ),
        ],
    )
    def test_full_separate_files(
        self,
        input_uuids: List[str],
        expected_bigs: List[str],
        expected_smalls: List[List[str]],
    ) -> None:
        """ Currently if a file has related or annotation files
        the dtt processes it as if it were a big file so that
        it goes through the old method of downloading,
        regardless of size.

        NOTE: This will probably change in the future.
        """

        bigs, smalls = self.index.separate_small_files(input_uuids, HTTP_CHUNK_SIZE)

        for uuid in input_uuids:
            self.assert_index_with_uuids(uuid)

        assert set(bigs) == set(expected_bigs)
        assert smalls == expected_smalls

    ############ not set ############
    def test_no_metadata(self) -> None:
        self.assert_invalid_index()

    def test_small_invalid_separate_small_files(self) -> None:
        """ If no metadata can be found about a file, attempt a
        download using the big file method
        """

        invalid = "invalid uuid"

        bigs, smalls = self.index.separate_small_files([invalid], HTTP_CHUNK_SIZE)

        self.assert_invalid_index()

        assert bigs == [invalid]
        assert smalls == []


@pytest.mark.parametrize("case", [range(1), range(499), range(500), range(1000),])
def test_chunk_list(case: Iterable[int]) -> None:
    assert all(len(chunk) <= 500 for chunk in _chunk_list(case))


@pytest.mark.parametrize(
    "ids, latest_ids, expected",
    [
        (["foo", "bar"], ["foo", "baz"], {"foo": "foo", "bar": "baz"}),
        (["1", "2", "3"], ["a", "b", "c"], {"1": "a", "2": "b", "3": "c"}),
        (["1", "2", "3"], ["a", "b", None], {"1": "a", "2": "b", "3": "3"}),
    ],
)
def test_get_latest_versions(
    versions_response,
    ids: List[str],
    latest_ids: List[str],
    expected: Mapping[str, str],
) -> None:
    url = "https://example.com"
    versions_response(url + "/files/versions", ids, latest_ids)

    result = get_latest_versions(url, ids)

    assert result == expected
