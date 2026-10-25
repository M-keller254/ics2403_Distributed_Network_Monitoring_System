"""S3-compatible object storage adapter backed by MinIO."""
import io
import os


class MinioObjectStore:
    def __init__(self, endpoint=None, access_key=None, secret_key=None,
                 bucket=None, secure=None):
        self.endpoint = endpoint or os.getenv("MINIO_ENDPOINT", "minio:9000")
        self.access_key = access_key or os.getenv("MINIO_ROOT_USER", "monitoradmin")
        self.secret_key = secret_key or os.getenv(
            "MINIO_ROOT_PASSWORD", "change-this-demo-password"
        )
        self.bucket = bucket or os.getenv("MINIO_BUCKET", "telemetry-archive")
        self.secure = (secure if secure is not None else
                       os.getenv("MINIO_SECURE", "false").lower() == "true")
        self._client_instance = None

    def _client(self):
        if self._client_instance is None:
            from minio import Minio
            self._client_instance = Minio(
                self.endpoint, access_key=self.access_key,
                secret_key=self.secret_key, secure=self.secure
            )
        return self._client_instance

    def ensure_bucket(self):
        client = self._client()
        if not client.bucket_exists(self.bucket):
            client.make_bucket(self.bucket)

    def put_bytes(self, object_name, content, content_type="application/json"):
        self.ensure_bucket()
        result = self._client().put_object(
            self.bucket, object_name, io.BytesIO(content), len(content),
            content_type=content_type
        )
        return result.etag

    def get_bytes(self, object_name):
        response = self._client().get_object(self.bucket, object_name)
        try:
            return response.read()
        finally:
            response.close()
            response.release_conn()

    def list_objects(self, prefix=""):
        self.ensure_bucket()
        return [
            {"object_name": item.object_name, "size": item.size,
             "etag": item.etag, "last_modified": item.last_modified.isoformat()
             if item.last_modified else None}
            for item in self._client().list_objects(
                self.bucket, prefix=prefix, recursive=True
            )
        ]
