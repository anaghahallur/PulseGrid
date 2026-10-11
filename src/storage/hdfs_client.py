"""
WebHDFS REST API Client.
Provides cross-platform access to Hadoop HDFS without requiring native Java C-bindings.
Automatically resolves Docker internal hostname redirects (datanode:9864 -> localhost:9864)
for host environments.
"""

import json
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse, urlunparse
import requests

from src.config import get_settings


class HDFSClient:
    """
    Robust WebHDFS REST client with Docker Desktop redirect remapping.
    """

    def __init__(
        self,
        base_url: Optional[str] = None,
        user: Optional[str] = None,
        timeout: int = 15,
        host_remap: bool = True,
    ):
        settings = get_settings()
        self.base_url = (base_url or settings.hdfs_namenode_url).rstrip("/")
        self.user = user or settings.hdfs_user
        self.timeout = timeout
        self.host_remap = host_remap

    def _remap_redirect(self, url: str) -> str:
        """
        Remaps internal Docker container hostnames in DataNode redirects to localhost
        when connecting from host Python.
        """
        if not self.host_remap:
            return url
        parsed = urlparse(url)
        # If redirected to datanode:9864, remap to localhost:9864 for host execution
        if parsed.hostname in ("datanode", "namenode"):
            remapped_netloc = f"localhost:{parsed.port or 9864}"
            return urlunparse(parsed._replace(netloc=remapped_netloc))
        return url

    def is_healthy(self) -> bool:
        """Check if NameNode WebHDFS endpoint is reachable."""
        try:
            url = f"{self.base_url}/webhdfs/v1/?op=LISTSTATUS&user.name={self.user}"
            resp = requests.get(url, timeout=self.timeout)
            return resp.status_code == 200
        except Exception:
            return False

    def mkdir(self, hdfs_path: str) -> bool:
        """Create directory in HDFS."""
        path = hdfs_path.strip("/")
        url = f"{self.base_url}/webhdfs/v1/{path}?op=MKDIRS&user.name={self.user}"
        resp = requests.put(url, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json().get("boolean", False)

    def write_file(self, hdfs_path: str, data: bytes, overwrite: bool = True) -> bool:
        """
        Write bytes payload to HDFS using 2-step WebHDFS protocol.
        """
        path = hdfs_path.strip("/")
        # Step 1: Initiate CREATE request to NameNode (do not auto-follow redirect)
        create_url = (
            f"{self.base_url}/webhdfs/v1/{path}"
            f"?op=CREATE&overwrite={'true' if overwrite else 'false'}&user.name={self.user}"
        )
        resp = requests.put(create_url, allow_redirects=False, timeout=self.timeout)
        if resp.status_code not in (307, 201):
            resp.raise_for_status()

        datanode_location = resp.headers.get("Location")
        if not datanode_location:
            raise RuntimeError(f"NameNode did not provide DataNode Location header: {resp.text}")

        # Step 2: Upload data directly to DataNode
        remapped_url = self._remap_redirect(datanode_location)
        upload_resp = requests.put(
            remapped_url,
            data=data,
            headers={"Content-Type": "application/octet-stream"},
            timeout=self.timeout,
        )
        upload_resp.raise_for_status()
        return upload_resp.status_code == 201

    def read_file(self, hdfs_path: str) -> bytes:
        """
        Read file bytes from HDFS.
        """
        path = hdfs_path.strip("/")
        open_url = f"{self.base_url}/webhdfs/v1/{path}?op=OPEN&user.name={self.user}"
        resp = requests.get(open_url, allow_redirects=False, timeout=self.timeout)

        if resp.status_code == 307:
            remapped_url = self._remap_redirect(resp.headers["Location"])
            data_resp = requests.get(remapped_url, timeout=self.timeout)
            data_resp.raise_for_status()
            return data_resp.content

        resp.raise_for_status()
        return resp.content

    def list_dir(self, hdfs_path: str = "/") -> List[Dict[str, Any]]:
        """List contents of an HDFS directory."""
        path = hdfs_path.strip("/")
        url = f"{self.base_url}/webhdfs/v1/{path}?op=LISTSTATUS&user.name={self.user}"
        resp = requests.get(url, timeout=self.timeout)
        resp.raise_for_status()
        data = resp.json()
        return data.get("FileStatuses", {}).get("FileStatus", [])

    def delete(self, hdfs_path: str, recursive: bool = True) -> bool:
        """Delete an HDFS file or directory."""
        path = hdfs_path.strip("/")
        url = (
            f"{self.base_url}/webhdfs/v1/{path}"
            f"?op=DELETE&recursive={'true' if recursive else 'false'}&user.name={self.user}"
        )
        resp = requests.delete(url, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json().get("boolean", False)

    def exists(self, hdfs_path: str) -> bool:
        """Check if an HDFS path exists."""
        path = hdfs_path.strip("/")
        url = f"{self.base_url}/webhdfs/v1/{path}?op=GETFILESTATUS&user.name={self.user}"
        resp = requests.get(url, timeout=self.timeout)
        if resp.status_code == 200:
            return True
        if resp.status_code == 404:
            return False
        resp.raise_for_status()
        return False

    def rename(self, src_path: str, dst_path: str) -> bool:
        """
        Atomically rename a file or directory in HDFS.
        Destination must be an absolute path starting with '/'.
        """
        src = src_path.strip("/")
        dst = "/" + dst_path.strip("/")
        url = (
            f"{self.base_url}/webhdfs/v1/{src}"
            f"?op=RENAME&destination={dst}&user.name={self.user}"
        )
        resp = requests.put(url, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json().get("boolean", False)

    def write_jsonl(self, hdfs_path: str, records: List[Dict[str, Any]], overwrite: bool = True) -> bool:
        """Write list of dictionaries as newline-delimited JSON to HDFS."""
        lines = [json.dumps(r, default=str) for r in records]
        payload = ("\n".join(lines) + "\n").encode("utf-8")
        return self.write_file(hdfs_path, payload, overwrite=overwrite)

    def read_jsonl(self, hdfs_path: str) -> List[Dict[str, Any]]:
        """Read newline-delimited JSON file from HDFS into list of dictionaries."""
        content = self.read_file(hdfs_path).decode("utf-8")
        records = []
        for line in content.splitlines():
            line = line.strip()
            if line:
                records.append(json.loads(line))
        return records

    def write_parquet(self, hdfs_path: str, df: Any, overwrite: bool = True) -> bool:
        """Serialize a pandas DataFrame to Parquet and write to HDFS."""
        import io
        import pyarrow as pa
        import pyarrow.parquet as pq

        table = pa.Table.from_pandas(df)
        buf = io.BytesIO()
        pq.write_table(table, buf, compression="snappy")
        return self.write_file(hdfs_path, buf.getvalue(), overwrite=overwrite)

    def read_parquet(self, hdfs_path: str) -> Any:
        """Read Parquet file from HDFS into a pandas DataFrame."""
        import io
        import pyarrow.parquet as pq

        content = self.read_file(hdfs_path)
        buf = io.BytesIO(content)
        table = pq.read_table(buf)
        return table.to_pandas()
