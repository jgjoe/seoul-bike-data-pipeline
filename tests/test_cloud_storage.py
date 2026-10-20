from types import SimpleNamespace

import pytest

pytest.importorskip("google.cloud.storage")

from google.api_core.exceptions import PreconditionFailed

from seoul_bike_pipeline.cloud_publish import CloudConfig, CloudPublishError, fingerprint
from seoul_bike_pipeline.cloud_storage import GoogleCloudStorage


class Blob:
    def __init__(self, bucket, name):
        self.bucket = bucket
        self.name = name
        self.metadata = None
        self.size = None
        self.generation = 1

    def upload_from_filename(self, path, *, if_generation_match, checksum):
        assert if_generation_match == 0
        assert checksum == "auto"
        self.size = len(open(path, "rb").read())
        self.bucket.objects[self.name] = self
        if self.bucket.race:
            raise PreconditionFailed("simulated concurrent create")


class Bucket:
    def __init__(self):
        self.location = "US-CENTRAL1"
        self.storage_class = "STANDARD"
        self.iam_configuration = SimpleNamespace(uniform_bucket_level_access_enabled=True,
                                                 public_access_prevention="enforced")
        self.objects = {}
        self.race = False

    def reload(self):
        pass

    def get_blob(self, name):
        return self.objects.get(name)

    def blob(self, name):
        return Blob(self, name)


def adapter(bucket, **limits):
    client = SimpleNamespace(bucket=lambda name: bucket,
                             list_blobs=lambda target: list(target.objects.values()))
    result = GoogleCloudStorage(CloudConfig("seoul-bike-dw", "offline-bucket", **limits), client=client)
    result.preflight()
    return result


def test_create_only_upload_reuse_and_conflict(tmp_path):
    bucket = Bucket()
    storage = adapter(bucket)
    path = tmp_path / "object"
    path.write_bytes(b"synthetic fixture")
    metadata = {key: str(value) for key, value in fingerprint(path).items()}
    assert storage.ensure_file("hash/data", path, metadata)["created"] is True
    assert storage.ensure_file("hash/data", path, metadata)["created"] is False
    assert len(bucket.objects) == 1
    bucket.objects["hash/data"].metadata["sha256"] = "wrong"
    with pytest.raises(CloudPublishError, match="metadata"):
        storage.ensure_file("hash/data", path, {**metadata, "sha256": fingerprint(path)["sha256"]})


def test_concurrent_create_rechecks_object_and_upload_cap(tmp_path):
    bucket = Bucket()
    bucket.race = True
    storage = adapter(bucket)
    path = tmp_path / "object"
    path.write_bytes(b"fixture")
    metadata = {key: str(value) for key, value in fingerprint(path).items()}
    assert storage.ensure_file("race", path, metadata)["created"] is False
    limited = adapter(Bucket(), maximum_upload_bytes=1)
    with pytest.raises(CloudPublishError, match="envelope"):
        limited.ensure_file("large", path, metadata)
    assert not limited.bucket.objects


@pytest.mark.parametrize("field,value", [
    ("location", "ASIA-NORTHEAST3"), ("storage_class", "NEARLINE"),
])
def test_wrong_bucket_contract_is_rejected(field, value):
    bucket = Bucket()
    setattr(bucket, field, value)
    with pytest.raises(CloudPublishError):
        adapter(bucket)


def test_private_bucket_contract_is_required():
    bucket = Bucket()
    bucket.iam_configuration.public_access_prevention = "inherited"
    with pytest.raises(CloudPublishError, match="public"):
        adapter(bucket)
