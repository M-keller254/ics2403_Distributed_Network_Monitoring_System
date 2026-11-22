"""Object storage used by the archiver (Milestone 12).

  OBJECT_STORE=minio   MinIO (S3-compatible cloud object storage); the deployed path.
  OBJECT_STORE=local   a directory tree (development / tests); same interface.

Interface: put(key, bytes), get(key) -> bytes, list(prefix) -> [keys], exists(key), stat(key) -> size.
MinIO retries connection errors at start-up (compose starts MinIO and the archiver together).
"""
import io
import os
import time


class LocalStore:
    kind = "local"

    def __init__(self, root):
        self.root = root
        os.makedirs(root, exist_ok=True)

    def _p(self, key):
        p = os.path.join(self.root, key)
        os.makedirs(os.path.dirname(p), exist_ok=True)
        return p

    def put(self, key, data):
        tmp = self._p(key) + ".tmp"
        with open(tmp, "wb") as f:
            f.write(data)
        os.replace(tmp, self._p(key))

    def get(self, key):
        with open(self._p(key), "rb") as f:
            return f.read()

    def exists(self, key):
        return os.path.exists(self._p(key))

    def stat(self, key):
        return os.path.getsize(self._p(key))

    def list(self, prefix=""):
        out = []
        for d, _, fs in os.walk(self.root):
            for f in fs:
                if f.endswith(".tmp"):
                    continue
                k = os.path.relpath(os.path.join(d, f), self.root)
                if k.startswith(prefix):
                    out.append(k)
        return sorted(out)


class MinioStore:
    kind = "minio"

    def __init__(self, endpoint, access_key, secret_key, bucket, secure=False, wait_s=60):
        from minio import Minio
        self.bucket = bucket
        self.client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)
        deadline = time.time() + wait_s
        while True:                                  # MinIO may still be starting: retry ANY connection error
            try:
                if not self.client.bucket_exists(bucket):
                    self.client.make_bucket(bucket)
                break
            except Exception:
                if time.time() > deadline:
                    raise
                time.sleep(2)

    def put(self, key, data):
        self.client.put_object(self.bucket, key, io.BytesIO(data), length=len(data))

    def get(self, key):
        r = self.client.get_object(self.bucket, key)
        try:
            return r.read()
        finally:
            r.close()
            r.release_conn()

    def exists(self, key):
        try:
            self.client.stat_object(self.bucket, key)
            return True
        except Exception:
            return False

    def stat(self, key):
        return self.client.stat_object(self.bucket, key).size

    def list(self, prefix=""):
        return sorted(o.object_name for o in self.client.list_objects(self.bucket, prefix=prefix, recursive=True))


def get_store():
    if os.environ.get("OBJECT_STORE", "minio") == "local":
        return LocalStore(os.environ.get("OBJECT_STORE_DIR", "/tmp/object-store"))
    return MinioStore(os.environ.get("MINIO_ENDPOINT", "minio:9000"), os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
                      os.environ.get("MINIO_SECRET_KEY", "minioadmin"), os.environ.get("MINIO_BUCKET", "monitoring-data"))
