"""Teradata layer: the connection wrapper, transactions, {key} / $key parameters, run ids.

One Teradata connection serves everything a run touches: the GOVT_RGLTRY_Sampl* metadata tables (in
Settings.meta_db), the source table a sample reads (Src_Schema_Nm.Src_Table_Nm) and its sample table.

The connection comes from the TERADATA_HOST / TERADATA_USER / TERADATA_PASSWORD / TERADATA_LOGMECH environment
variables - set by the Airflow bridge from the Teradata Connection, or locally from dev.env. Nothing else here
knows where they came from.

Logging: SQL text and row counts only, never bind values or fetched row data (the source can hold member and
case identifiers).
"""
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
except ImportError:  # the tests run on a stand-in
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
    """Teradata returns whole numbers (COUNT(*), ROW_NUMBER()) as Decimal: give them back as int."""
    if isinstance(value, Decimal) and value == value.to_integral_value():
        return int(value)
    return value


def connect_teradata(database: Optional[str] = None, access_lock: bool = True) -> "Connection":
    """A new Teradata connection from the TERADATA_* environment; `database` becomes the session default."""
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
    """Wraps a teradatasql connection (autocommit on; transaction() turns it off for one atomic unit)."""

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
        """Run a SELECT; rows as dicts with lower-case column names."""
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
        """The column names (as the database spells them) of a SELECT's result; run it with WHERE 1=0."""
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
        """Run one large SELECT, fetched `chunk_rows` at a time (bounds the driver's buffer, not the result)."""
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
        """One DML statement (committed by itself, or with the enclosing transaction()); returns the row count."""
        log.debug("execute: %s | params=%d", _one_line(sql), len(params or ()))
        cur = self._raw.cursor()
        try:
            cur.execute(sql, list(params)) if params else cur.execute(sql)
            return getattr(cur, "rowcount", -1)
        finally:
            cur.close()

    def executemany(self, sql: str, rows: list, chunk_rows: int) -> int:
        """Batched parameterised DML; returns the number of rows sent."""
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
        """One atomic unit; nested blocks join the outer one (Teradata has no savepoints)."""
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

    def ping(self) -> bool:
        try:
            self.query("SELECT 1 AS ok")
            return True
        except Exception:  # noqa: BLE001
            return False

    def close(self) -> None:
        if not self.closed:
            self.closed = True
            self._raw.close()

    def __enter__(self) -> "Connection":
        return self

    def __exit__(self, *exc) -> None:
        self.close()


# ---------------------------------------------------------------------------
# {key} / $key run parameters in Scope_Sql_Txt and the rules' SQL
# ---------------------------------------------------------------------------
# Both spellings, freely mixed. "$key" ends at a word boundary, so "$year" in "$year_end" is not consumed.
_TOKEN_RE = re.compile(r"\{([a-zA-Z_][a-zA-Z0-9_]*)\}|\$([a-zA-Z_][a-zA-Z0-9_]*)\b")


def _escape_literal(value) -> str:
    return str(value if value is not None else "").replace("'", "''")


def substitute_params(sql: Optional[str], params: dict, escape: bool = True) -> Optional[str]:
    """Replace every {key} / $key with the value of params[key], quotes doubled (the author writes the quotes:
    pull_date = '{pull_date}'; escape=False for text that is not SQL, e.g. a run_key template). Raises
    ValueError naming any token left without a value. Keys are not case sensitive."""
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



# ---------------------------------------------------------------------------
# run_key / run ids
# ---------------------------------------------------------------------------

def build_run_key(*parts, delimiter: str = "_") -> str:
    """Join values into one run_key: build_run_key(2026, 8) -> '2026_8'."""
    if not parts:
        raise ValueError("build_run_key() needs at least one part")
    return delimiter.join(str(p) for p in parts)


def generate_run_id(*label_parts, timestamp: Optional[datetime] = None) -> str:
    """'<labels joined by ::>::<YYYYMMDDTHHMMSS.ffffff>::<6 hex>' - sortable, readable, collision-free.

    e.g. UMCM.ODAG1.WKLY::2026-10-30::attempt-2::airflow-scheduled::20261030T080000.183045::f9e8d7
    """
    ts = (timestamp or datetime.now()).strftime("%Y%m%dT%H%M%S.%f")
    labels = [str(p) for p in label_parts if p not in (None, "")]
    run_id = "::".join([*labels, ts, secrets.token_hex(3)])
    if len(run_id) > 250:
        log.warning("run id is %d characters, over the VARCHAR(250) columns that hold it: %s", len(run_id), labels)
    return run_id


def error_text(exc: BaseException) -> str:
    """One line for logs and stored error text: the exception type and its first line."""
    first = (str(exc).strip().splitlines() or [""])[0]
    return f"{type(exc).__name__}: {first}"[:2000]
