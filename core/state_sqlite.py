import sqlite3
import threading
import time
from pathlib import Path
from contextlib import contextmanager


class StateDB:
    """
    Threading-safe state ETL with SQLite.

    schema:
      files(
        name TEXT PRIMARY KEY,
        url TEXT,
        status TEXT,
        path TEXT,
        cache TEXT,
        updated_at REAL
      )
    """
    def __init__(self, db_path: Path):
        self.path = db_path
        self.path.parent.mkdir(exist_ok=True)
        self._lock = threading.Lock()
        self._init_db()

    def _init_db(self):
        with self._connect() as con:
            con.execute("PRAGMA journal_mode=WAL;")
            con.execute("""
                CREATE TABLE IF NOT EXISTS files(
                    name TEXT PRIMARY KEY,
                    url TEXT,
                    status TEXT,
                    path TEXT,
                    cache TEXT,
                    updated_at REAL
                )
            """)
            con.execute("CREATE INDEX IF NOT EXISTS idx_status ON files(status)")
            con.commit()

    @contextmanager
    def _connect(self):
        con = sqlite3.connect(self.path, check_same_thread=False, timeout=60)
        try:
            yield con
        finally:
            con.close()

    def upsert(self, name, url=None, status=None, path=None, cache=None):
        ts = time.time()
        with self._lock, self._connect() as con:
            cur = con.execute("SELECT name,url,status,path,cache FROM files WHERE name=?", (name,))
            row = cur.fetchone()

            if row:
                old_name, old_url, old_status, old_path, old_cache = row
                url = url if url is not None else old_url
                status = status if status is not None else old_status
                path = str(path) if path is not None else old_path
                cache = str(cache) if cache is not None else old_cache

                con.execute("""
                    UPDATE files
                    SET url=?, status=?, path=?, cache=?, updated_at=?
                    WHERE name=?
                """, (url, status, path, cache, ts, name))
            else:
                con.execute("""
                    INSERT INTO files(name,url,status,path,cache,updated_at)
                    VALUES (?,?,?,?,?,?)
                """, (
                    name,
                    url,
                    status,
                    str(path) if path else None,
                    str(cache) if cache else None,
                    ts
                ))
            con.commit()

    def get(self, name):
        with self._connect() as con:
            cur = con.execute("""
                SELECT name,url,status,path,cache,updated_at
                FROM files WHERE name=?
            """, (name,))
            row = cur.fetchone()
            if not row:
                return None
            keys = ["name","url","status","path","cache","updated_at"]
            return dict(zip(keys, row))

    def iter_all(self):
        with self._connect() as con:
            cur = con.execute("""
                SELECT name,url,status,path,cache,updated_at FROM files
            """)
            for row in cur.fetchall():
                keys = ["name","url","status","path","cache","updated_at"]
                yield dict(zip(keys, row))

    def cleanup_incomplete(self):
        """
        All 'downloading' after crash or sudden stop threating as 'failed' and sending to re-download
        """
        with self._lock, self._connect() as con:
            cur = con.execute("""
                SELECT name, cache FROM files WHERE status='downloading'
            """)
            bad = cur.fetchall()
            for name, cache in bad:
                if cache:
                    try:
                        p = Path(cache)
                        if p.exists():
                            p.unlink()
                    except Exception:
                        pass
                con.execute("""
                    UPDATE files SET status='failed', updated_at=? WHERE name=?
                """, (time.time(), name))
            con.commit()
