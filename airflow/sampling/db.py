from __future__ import annotations

import logging
import os
import re
import secrets
from contextlib import contextmanager
from datetime import datetime
from decimal import Decimal
from typing import Iterator, Optional, Sequence


log = logging.getLogger(__name__)

try:
    import teradatasql
    _TERADATA_AVAILABLE = True
except ImportError:
    teradatasql = None
    _TERADATA_AVAILABLE = False


def _require(key: str) -> str:
    value = os.environ.get(key)
    if not value:
        raise EnvironmentError(f"Required environment variable '{key}' is not set")
    return value


def _one_line(sql: str, max_len: int = 500) -> str:
    text = " ".join(sql.split())
    return text if len(text) <= max_len else text[:max_len] + " ..."


def _clean(value):
    if isinstance(value, Decimal) and value == value.to_integral_value():
        return int(value)
    return value


def connect_teradata(database: Optional[str] = None, access_lock: bool = True) -> "Connection":
    if not _TERADATA_AVAILABLE:
        raise ImportError("teradatasql is required. Install with: pip install teradatasql")
    host = _require("TERADATA_HOST")
    user = _require("TERADATA_USER")
    logmech = os.getenv("TERADATA_LOGMECH", "LDAP")
    log.info("Connecting to Teradata: host=%s user=%s logmech=%s database=%s", host, user, logmech, database)
    kwargs = {"host": host, "user": user, "password": _require("TERADATA_PASSWORD"), "logmech": logmech}
    if database:
        kwargs["database"] = database
    return Connection(teradatasql.connect(**kwargs), host=host, access_lock=access_lock)


class Connection:
    def __init__(self, raw, host: Optional[str] = None, access_lock: bool = True):
        self._raw = raw
        self.host = host
        self.access_lock = access_lock
        self._depth = 0
        self.closed = False

    def _text(self, sql: str) -> str:
        sql = sql.strip()
        if self.access_lock and sql[:6].upper() == "SELECT":
            return "LOCKING ROW FOR ACCESS " + sql
        return sql

    def query(self, sql: str, params: Sequence = ()) -> list:
        text = self._text(sql)
        log.debug("query: %s | params=%d", _one_line(text), len(params or ()))
        cur = self._raw.cursor()
        try:
            cur.execute(text, list(params)) if params else cur.execute(text)
            if not cur.description:
                return []
            names = [c[0].lower() for c in cur.description]
            rows = [dict(zip(names, map(_clean, r))) for r in cur.fetchall()]
            log.debug("query: %d row(s)", len(rows))
            return rows
        finally:
            cur.close()

    def columns(self, sql: str) -> list:
        text = self._text(sql)
        log.debug("columns: %s", _one_line(text))
        cur = self._raw.cursor()
        try:
            cur.execute(text)
            names = [c[0] for c in cur.description or ()]
            cur.fetchall()
            return names
        finally:
            cur.close()

    def stream(self, sql: str, chunk_rows: int) -> list:
        text = self._text(sql)
        log.debug("stream: %s | chunk=%d", _one_line(text), chunk_rows)
        cur = self._raw.cursor()
        try:
            cur.execute(text)
            if not cur.description:
                return []
            names = [c[0].lower() for c in cur.description]
            rows = []
            while True:
                batch = cur.fetchmany(chunk_rows)
                if not batch:
                    break
                rows.extend(dict(zip(names, map(_clean, r))) for r in batch)
            log.debug("stream: %d row(s)", len(rows))
            return rows
        finally:
            cur.close()

    def execute(self, sql: str, params: Sequence = ()) -> int:
        log.debug("execute: %s | params=%d", _one_line(sql), len(params or ()))
        cur = self._raw.cursor()
        try:
            cur.execute(sql, list(params)) if params else cur.execute(sql)
            return getattr(cur, "rowcount", -1)
        finally:
            cur.close()

    def executemany(self, sql: str, rows: list, chunk_rows: int) -> int:
        if not rows:
            return 0
        log.debug("executemany: %s | %d row(s), chunk=%d", _one_line(sql), len(rows), chunk_rows)
        cur = self._raw.cursor()
        try:
            for i in range(0, len(rows), chunk_rows):
                cur.executemany(sql, rows[i:i + chunk_rows])
        finally:
            cur.close()
        return len(rows)

    @contextmanager
    def transaction(self) -> Iterator[None]:
        if self._depth:
            self._depth += 1
            try:
                yield
            finally:
                self._depth -= 1
            return
        self._autocommit(False)
        self._depth = 1
        try:
            yield
        except BaseException:
            self._depth = 0
            try:
                self._raw.rollback()
            except Exception:  # noqa: BLE001
                log.debug("rollback failed", exc_info=True)
            finally:
                self._autocommit(True)
            raise
        else:
            self._depth = 0
            try:
                self._raw.commit()
            finally:
                self._autocommit(True)

    def _autocommit(self, on: bool) -> None:
        if hasattr(type(self._raw), "autocommit"):
            self._raw.autocommit = on
            return
        cur = self._raw.cursor()
        try:
            cur.execute("{fn teradata_nativesql}{fn teradata_autocommit_" + ("on" if on else "off") + "}")
        finally:
            cur.close()

    def close(self) -> None:
        if not self.closed:
            self.closed = True
            self._raw.close()

    def __enter__(self) -> "Connection":
        return self

    def __exit__(self, *exc) -> None:
        self.close()


_TOKEN_RE = re.compile(r"\{([a-zA-Z_][a-zA-Z0-9_]*)\}|\$([a-zA-Z_][a-zA-Z0-9_]*)\b")


def _escape_literal(value) -> str:
    return str(value if value is not None else "").replace("'", "''")


def substitute_params(sql: Optional[str], params: dict, escape: bool = True) -> Optional[str]:
    if not sql:
        return sql
    params = params or {}
    lowered = {str(k).lower(): v for k, v in params.items()}

    def _replace(m):
        key = (m.group(1) or m.group(2)).lower()
        if key not in lowered:
            return m.group(0)
        return _escape_literal(lowered[key]) if escape else str(lowered[key] if lowered[key] is not None else "")

    resolved = _TOKEN_RE.sub(_replace, sql)
    missing = sorted({a or b for a, b in _TOKEN_RE.findall(resolved)})
    if missing:
        raise ValueError(f"no value for parameter(s) {missing} in the SQL; run_params given: {sorted(lowered)}")
    return resolved


def generate_run_id(*label_parts, timestamp: Optional[datetime] = None) -> str:
    ts = (timestamp or datetime.now()).strftime("%Y%m%dT%H%M%S.%f")
    labels = [str(p) for p in label_parts if p not in (None, "")]
    run_id = "::".join([*labels, ts, secrets.token_hex(3)])
    if len(run_id) > 250:
        log.warning("run id is %d characters, over the VARCHAR(250) columns that hold it: %s", len(run_id), labels)
    return run_id


_DATABASE_ERROR = re.compile(r"\[Teradata Database\] \[Error (\d+)\].*")
ERROR_TEXT_MAX = 1000


def error_line(exc: BaseException) -> str:
    m = _DATABASE_ERROR.search(str(exc))
    return m.group(0) if m else (str(exc).strip().splitlines() or [""])[0]


def error_text(exc: BaseException) -> str:
    return f"{type(exc).__name__}: {error_line(exc)}"[:ERROR_TEXT_MAX]


def log_sql(what: str, template: Optional[str], sql: str, params: Optional[dict] = None) -> None:
    params = {str(k).lower(): v for k, v in (params or {}).items()}
    names = dict.fromkeys(n for n in ((a or b).lower() for a, b in _TOKEN_RE.findall(template or "")) if n != "env")
    shown = ", ".join(f"{n} = {params.get(n)!r}" for n in names) or "none"
    log.info("%s\n    SQL:         %s\n    parameters:  %s\n    with values: %s", what,
             " ".join(str(template or sql).split()), shown, " ".join(sql.split()))
