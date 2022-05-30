import gzip
import json
import os
import re
import shutil
import tempfile
import urllib.request
from pathlib import Path
from typing import Any, Generator

from zyte_datadiff.dataset.dataset import Dataset


class ScrapyCloudJob(Dataset):
    download_path: Path = None
    url: str

    def __init__(self, url: str):
        self.url = url

    def __iter__(self) -> Generator[Any, None, None]:
        self.download_path = self.download_path or self.download()
        with gzip.open(self.download_path, mode="rt") as f:
            for line in f:
                yield json.loads(line)

    def download(self) -> Path:
        zyte = re.fullmatch(r"https://app.zyte.com/p/(?P<job_key>\d+/\d+/\d+)", self.url)
        opener = urllib.request.build_opener()
        opener.addheaders = [("accept-encoding", "gzip")]
        with tempfile.NamedTemporaryFile(mode="wb", delete=False) as fdst, opener.open(
            f"https://storage.scrapinghub.com/items/{zyte['job_key']}?apikey={os.getenv('SH_APIKEY')}&format=jl&meta=_key"
        ) as fsrc:
            shutil.copyfileobj(fsrc, fdst)
            return Path(fdst.name)
