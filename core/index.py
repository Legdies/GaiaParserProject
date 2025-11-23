import re
import requests
from pathlib import Path
from .utils import save_json, load_json


class IndexManager:
    def __init__(self, file_index: Path, base_url: str):
        self.path = file_index
        self.base = base_url

    def load(self):
        if not self.path.exists():
            return None
        idx = load_json(self.path, {"files": []})
        return idx if idx["files"] else None

    def scan_remote(self):
        html = requests.get(self.base).text
        found = re.findall(r'href=[\'"]([^\'"]*?\.csv\.gz)[\'"]', html)
        files = [{"name": f, "url": self.base + f} for f in found]
        save_json(self.path, {"files": files})
        return {"files": files}
