"""Create-only, private GCS publication adapter. No credentials at import time."""

from .cloud_publish import CloudPublishError


class GoogleCloudStorage:
    def __init__(self, config, *, client=None):
        from google.cloud import storage
        self.config = config
        self.client = client if client is not None else storage.Client(project=config.project)
        self.bucket = self.client.bucket(config.bucket)

    def preflight(self):
        self.bucket.reload()
        if self.bucket.location.lower() != self.config.location or self.bucket.storage_class != "STANDARD":
            raise CloudPublishError("bucket must be Standard storage in us-central1")
        if not self.bucket.iam_configuration.uniform_bucket_level_access_enabled:
            raise CloudPublishError("uniform bucket-level access is required")
        if self.bucket.iam_configuration.public_access_prevention != "enforced":
            raise CloudPublishError("public access prevention must be enforced")
        self.live_bytes = sum(blob.size or 0 for blob in self.client.list_blobs(self.bucket))
        if self.live_bytes > self.config.maximum_upload_bytes:
            raise CloudPublishError("existing bucket storage exceeds configured envelope")

    def ensure_file(self, name, path, metadata):
        from google.api_core.exceptions import PreconditionFailed
        blob = self.bucket.get_blob(name)
        created = False
        if blob is None:
            if self.live_bytes + int(metadata["size_bytes"]) > self.config.maximum_upload_bytes:
                raise CloudPublishError("upload would exceed bucket storage envelope")
            blob = self.bucket.blob(name)
            blob.metadata = metadata
            try:
                blob.upload_from_filename(str(path), if_generation_match=0, checksum="auto")
                created = True
                self.live_bytes += int(metadata["size_bytes"])
            except PreconditionFailed:
                # Another create of this exact content-addressed name won.
                blob = self.bucket.get_blob(name)
        if blob is None or any((blob.metadata or {}).get(key) != value for key, value in metadata.items()):
            raise CloudPublishError("existing GCS object metadata does not match captured artifact")
        if blob.size != int(metadata["size_bytes"]):
            raise CloudPublishError("existing GCS object size mismatch")
        return {
            "uri": f"gs://{self.config.bucket}/{name}", "generation": str(blob.generation),
            "size_bytes": blob.size, "sha256": metadata["sha256"], "created": created,
        }
