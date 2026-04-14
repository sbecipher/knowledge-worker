from types import SimpleNamespace

import pytest
from google.api_core.exceptions import NotFound

from scripts.rename_prices_end_date_partitions import (
    LegacyPricePartitionFolder,
    destination_folder_id,
    parse_legacy_partition_folder,
    run_migration,
)


class FakeBucket:
    def __init__(self, hns_enabled: bool = True) -> None:
        self._properties = {"hierarchicalNamespace": {"enabled": hns_enabled}}

    def reload(self) -> None:
        return None


class FakeStorageClient:
    def __init__(self, bucket: FakeBucket) -> None:
        self._bucket = bucket

    def bucket(self, bucket_name: str) -> FakeBucket:
        return self._bucket


class FakeOperation:
    def __init__(self, destination_resource_name: str, operation_name: str) -> None:
        self.operation = SimpleNamespace(name=operation_name)
        self._response = SimpleNamespace(name=destination_resource_name)

    def result(self) -> SimpleNamespace:
        return self._response


class FakeControlClient:
    def __init__(self, folders: list[str], existing_destinations: set[str] | None = None) -> None:
        self._folders = folders
        self._existing_destinations = set(existing_destinations or set())
        self.rename_requests: list[object] = []
        self.list_requests: list[object] = []

    def list_folders(self, request: object):
        self.list_requests.append(request)
        for folder_name in self._folders:
            yield SimpleNamespace(name=folder_name)

    def get_folder(self, name: str) -> SimpleNamespace:
        if name in self._existing_destinations:
            return SimpleNamespace(name=name)
        raise NotFound("missing")

    def rename_folder(self, request: object) -> FakeOperation:
        self.rename_requests.append(request)
        return FakeOperation(
            destination_resource_name=f"projects/_/buckets/sbecipher-intelligence/folders/{request.destination_folder_id}",
            operation_name=f"operations/{len(self.rename_requests)}",
        )


def test_parse_legacy_partition_folder_extracts_date() -> None:
    folder = parse_legacy_partition_folder("source/prices/granularity=day/end_date=2014-10-06/")

    assert folder == LegacyPricePartitionFolder(
        source_folder_id="source/prices/granularity=day/end_date=2014-10-06/",
        date="2014-10-06",
    )


def test_destination_folder_id_renames_partition_key() -> None:
    folder = LegacyPricePartitionFolder(
        source_folder_id="dev/source/prices/granularity=day/end_date=2014-10-06/",
        date="2014-10-06",
    )

    assert destination_folder_id(folder) == "dev/source/prices/granularity=day/date=2014-10-06/"


def test_run_migration_dry_run_lists_matching_partition_folders(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    import scripts.rename_prices_end_date_partitions as script

    fake_settings = SimpleNamespace(gcs_bucket="sbecipher-intelligence", gcs_prefix="", gcs_service_account_key_json="")
    storage_client = FakeStorageClient(FakeBucket(hns_enabled=True))
    control_client = FakeControlClient(
        folders=[
            "projects/_/buckets/sbecipher-intelligence/folders/source/prices/granularity=day/",
            "projects/_/buckets/sbecipher-intelligence/folders/source/prices/granularity=day/end_date=2014-10-06/",
            "projects/_/buckets/sbecipher-intelligence/folders/source/prices/granularity=day/end_date=2014-10-07/",
            "projects/_/buckets/sbecipher-intelligence/folders/source/prices/granularity=day/date=2014-10-08/",
        ]
    )

    monkeypatch.setattr(script, "_settings_clients", lambda: (fake_settings, storage_client, control_client))
    monkeypatch.setattr(script, "LOCAL_MANIFEST_DIR", tmp_path)

    manifest = run_migration(dry_run=True, start_date="2014-10-06", end_date="2014-10-06", limit=None)

    assert manifest["mappings"] == [
        {
            "date": "2014-10-06",
            "source_folder_path": "source/prices/granularity=day/end_date=2014-10-06/",
            "destination_folder_path": "source/prices/granularity=day/date=2014-10-06/",
        }
    ]
    assert manifest["selected_count"] == 1
    assert manifest["executed_count"] == 0
    assert manifest["skipped"] == [
        {
            "folder_path": "source/prices/granularity=day/date=2014-10-08/",
            "reason": "already_uses_date_partition",
        }
    ]
    assert manifest["failure_count"] == 0
    assert control_client.rename_requests == []


def test_run_migration_executes_folder_rename_and_skips_existing_destination(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    import scripts.rename_prices_end_date_partitions as script

    fake_settings = SimpleNamespace(gcs_bucket="sbecipher-intelligence", gcs_prefix="", gcs_service_account_key_json="")
    storage_client = FakeStorageClient(FakeBucket(hns_enabled=True))
    control_client = FakeControlClient(
        folders=[
            "projects/_/buckets/sbecipher-intelligence/folders/source/prices/granularity=day/",
            "projects/_/buckets/sbecipher-intelligence/folders/source/prices/granularity=day/end_date=2014-10-06/",
            "projects/_/buckets/sbecipher-intelligence/folders/source/prices/granularity=day/end_date=2014-10-07/",
        ],
        existing_destinations={
            "projects/_/buckets/sbecipher-intelligence/folders/source/prices/granularity=day/date=2014-10-07/"
        },
    )

    monkeypatch.setattr(script, "_settings_clients", lambda: (fake_settings, storage_client, control_client))
    monkeypatch.setattr(script, "LOCAL_MANIFEST_DIR", tmp_path)

    manifest = run_migration(dry_run=False, start_date=None, end_date=None, limit=None)

    assert len(control_client.rename_requests) == 1
    rename_request = control_client.rename_requests[0]
    assert rename_request.name == "projects/_/buckets/sbecipher-intelligence/folders/source/prices/granularity=day/end_date=2014-10-06/"
    assert rename_request.destination_folder_id == "source/prices/granularity=day/date=2014-10-06/"
    assert manifest["executed_count"] == 1
    assert manifest["mappings"][0]["destination_folder_path"] == "source/prices/granularity=day/date=2014-10-06/"
    assert manifest["skipped"] == [
        {
            "date": "2014-10-07",
            "source_folder_path": "source/prices/granularity=day/end_date=2014-10-07/",
            "destination_folder_path": "source/prices/granularity=day/date=2014-10-07/",
            "reason": "destination_exists",
        }
    ]


def test_run_migration_requires_hns_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    import scripts.rename_prices_end_date_partitions as script

    fake_settings = SimpleNamespace(gcs_bucket="sbecipher-intelligence", gcs_prefix="", gcs_service_account_key_json="")
    storage_client = FakeStorageClient(FakeBucket(hns_enabled=False))
    control_client = FakeControlClient(folders=[])

    monkeypatch.setattr(script, "_settings_clients", lambda: (fake_settings, storage_client, control_client))

    with pytest.raises(ValueError, match="hierarchical namespace"):
        run_migration(dry_run=True, start_date=None, end_date=None, limit=None)
