import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable, Optional, Tuple


@dataclass(frozen=True)
class UserHandles:
    qq_id: int
    cf_handle: Optional[str]
    cf_rating: Optional[int]
    atc_handle: Optional[str]
    niuke_handle: Optional[str]
    luogu_handle: Optional[str]


class DataStore:
    """SQLite-backed data store for account bindings and solved problems."""

    def __init__(self, db_path: str):
        self._db_path = db_path
        self._init_db()

    @contextmanager
    def _connect(self) -> Iterable[sqlite3.Connection]:
        conn = sqlite3.connect(self._db_path)
        try:
            conn.execute("PRAGMA foreign_keys = ON;")
            yield conn
            conn.commit()
        finally:
            conn.close()

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    qq_id INTEGER PRIMARY KEY,
                    cf_handle TEXT,
                    cf_rating INTEGER DEFAULT 0,
                    atc_handle TEXT,
                    niuke_handle TEXT,
                    luogu_handle TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS solved_problems (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    qq_id INTEGER NOT NULL,
                    platform TEXT NOT NULL,
                    problem_id TEXT NOT NULL,
                    problem_name TEXT,
                    problem_rating TEXT,
                    problem_url TEXT,
                    submit_time INTEGER NOT NULL,
                    UNIQUE(qq_id, platform, problem_id),
                    FOREIGN KEY (qq_id) REFERENCES users(qq_id) ON DELETE CASCADE
                );
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS admins (
                    qq_id INTEGER PRIMARY KEY,
                    created_at TEXT NOT NULL
                );
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_solved_user_platform
                ON solved_problems (qq_id, platform);
                """
            )
            # Try to add column if it doesn't exist to update existing DB schemas
            try:
                conn.execute("ALTER TABLE users ADD COLUMN cf_rating INTEGER DEFAULT 0;")
            except sqlite3.OperationalError:
                pass

    @staticmethod
    def _now_iso() -> str:
        return datetime.now(timezone.utc).isoformat()

    def upsert_user(self, qq_id: int) -> None:
        now = self._now_iso()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO users (qq_id, created_at, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(qq_id) DO UPDATE SET updated_at = excluded.updated_at;
                """,
                (qq_id, now, now),
            )

    def add_admin(self, qq_id: int) -> bool:
        """Add an admin. Returns True if added, False if already exists."""
        now = self._now_iso()
        with self._connect() as conn:
            cur = conn.execute(
                """
                INSERT OR IGNORE INTO admins (qq_id, created_at)
                VALUES (?, ?);
                """,
                (qq_id, now),
            )
            return cur.rowcount > 0

    def remove_admin(self, qq_id: int) -> bool:
        """Remove an admin. Returns True if removed, False if not found."""
        with self._connect() as conn:
            cur = conn.execute("DELETE FROM admins WHERE qq_id = ?;", (qq_id,))
            return cur.rowcount > 0

    def is_admin(self, qq_id: int) -> bool:
        with self._connect() as conn:
            row = conn.execute("SELECT 1 FROM admins WHERE qq_id = ?;", (qq_id,)).fetchone()
            return bool(row)

    def get_admins(self) -> Iterable[int]:
        with self._connect() as conn:
            rows = conn.execute("SELECT qq_id FROM admins;").fetchall()
            return [row[0] for row in rows]

    def get_user(self, qq_id: int) -> Optional[UserHandles]:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT qq_id, cf_handle, cf_rating, atc_handle, niuke_handle, luogu_handle
                FROM users WHERE qq_id = ?;
                """,
                (qq_id,),
            ).fetchone()
        if not row:
            return None
        return UserHandles(*row)

    def bind_handle(self, qq_id: int, platform: str, handle: Optional[str]) -> None:
        if platform not in {"cf", "atc", "niuke", "luogu"}:
            raise ValueError(f"Unsupported platform: {platform}")
        self.upsert_user(qq_id)
        now = self._now_iso()
        field = f"{platform}_handle"
        with self._connect() as conn:
            conn.execute(
                f"""
                UPDATE users
                SET {field} = ?, updated_at = ?
                WHERE qq_id = ?;
                """,
                (handle, now, qq_id),
            )

    def update_cf_rating(self, qq_id: int, rating: int) -> None:
        self.upsert_user(qq_id)
        now = self._now_iso()
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE users
                SET cf_rating = ?, updated_at = ?
                WHERE qq_id = ?;
                """,
                (rating, now, qq_id),
            )

    def add_solved_records(self, qq_id: int, platform: str, records: Iterable[Tuple[str, str, str, str, int]]) -> int:
        """
        批量录入刷题记录。
        records: List of tuples (problem_id, problem_name, problem_rating, problem_url, submit_time)
        返回新增成功的数量。
        """
        if platform not in {"cf", "atc", "niuke", "luogu"}:
            raise ValueError(f"Unsupported platform: {platform}")
        self.upsert_user(qq_id)

        insert_data = []
        for r in records:
            # r = (problem_id, problem_name, problem_rating, problem_url, submit_time)
            insert_data.append((qq_id, platform, r[0], r[1], r[2], r[3], r[4]))

        with self._connect() as conn:
            # INSERT OR IGNORE automatically skips duplicates (qq_id, platform, problem_id)
            cur = conn.executemany(
                """
                INSERT OR IGNORE INTO solved_problems 
                (qq_id, platform, problem_id, problem_name, problem_rating, problem_url, submit_time)
                VALUES (?, ?, ?, ?, ?, ?, ?);
                """,
                insert_data,
            )
            return cur.rowcount

    def get_all_users(self) -> Iterable[UserHandles]:
        """获取所有已记录的用户（用于定时任务轮询他们的平台账号查题记录）"""
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT qq_id, cf_handle, cf_rating, atc_handle, niuke_handle, luogu_handle
                FROM users;
                """
            ).fetchall()
        return [UserHandles(*row) for row in rows]

    def count_solved(self, qq_id: int, platform: Optional[str] = None) -> int:
        with self._connect() as conn:
            if platform:
                row = conn.execute(
                    """
                    SELECT COUNT(*) FROM solved_problems
                    WHERE qq_id = ? AND platform = ?;
                    """,
                    (qq_id, platform),
                ).fetchone()
            else:
                row = conn.execute(
                    """
                    SELECT COUNT(*) FROM solved_problems
                    WHERE qq_id = ?;
                    """,
                    (qq_id,),
                ).fetchone()
        return int(row[0]) if row else 0

    def list_solved(
        self, qq_id: int, platform: Optional[str] = None, limit: int = 10
    ) -> Iterable[Tuple[str, str, str, str, str, int]]:
        """获取最近AC的题目."""
        with self._connect() as conn:
            if platform:
                rows = conn.execute(
                    """
                    SELECT platform, problem_id, problem_name, problem_rating, problem_url, submit_time
                    FROM solved_problems
                    WHERE qq_id = ? AND platform = ?
                    ORDER BY submit_time DESC
                    LIMIT ?;
                    """,
                    (qq_id, platform, limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT platform, problem_id, problem_name, problem_rating, problem_url, submit_time
                    FROM solved_problems
                    WHERE qq_id = ?
                    ORDER BY submit_time DESC
                    LIMIT ?;
                    """,
                    (qq_id, limit),
                ).fetchall()
        return rows
