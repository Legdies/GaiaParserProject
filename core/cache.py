import gzip
from pathlib import Path
from .utils import gzip_is_valid


class CacheManager:

    def __init__(self, cache_dir: Path):
        self.dir = cache_dir
        self.dir.mkdir(exist_ok=True)

    def path(self, fname):
        return self.dir / fname

    def exists(self, fname):
        p = self.path(fname)
        return p.exists() and gzip_is_valid(p)
