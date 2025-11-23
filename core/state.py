from pathlib import Path
from .utils import load_json, save_json


class StateManager:
    def __init__(self, state_file: Path):
        self.path = state_file
        self.data = load_json(state_file, {"files": []})

    def cleanup_incomplete(self):
        for f in self.data["files"]:
            if f["status"] == "downloading":
                cache = f.get("cache")
                if cache and Path(cache).exists():
                    Path(cache).unlink()
                f["status"] = "failed"
        self.save()

    def save(self):
        save_json(self.path, self.data)

    def update(self, name, url, status, path=None, cache=None):
        entry = self.get(name)
        if entry:
            entry["status"] = status
            if path is not None:
                entry["path"] = str(path)
            if cache is not None:
                entry["cache"] = str(cache)
        else:
            self.data["files"].append({
                "name": name,
                "url": url,
                "status": status,
                "path": str(path) if path else None,
                "cache": str(cache) if cache else None
            })
        self.save()

    def get(self, name):
        for f in self.data["files"]:
            if f["name"] == name:
                return f
        return None
