from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta, tzinfo
from functools import lru_cache
from typing import Any, Mapping, Optional
from zoneinfo import ZoneInfo

ENV_TOKEN_VALUES = {"DEV": "dev", "QA": "qa", "INT": "int", "UAT": "", "PROD": ""}
DEFAULT_TIMEZONE = "America/Chicago"
DEFAULT_CHUNK_ROWS = 1000

_ENV_TOKEN = re.compile(r"\$env", re.IGNORECASE)
_IDENT = re.compile(r"^[A-Za-z_$#][A-Za-z0-9_$#]{0,127}$")


log = logging.getLogger(__name__)


class ConfigError(ValueError):
    pass


def _nth_sunday(year: int, month: int, n: int) -> datetime:
    first = datetime(year, month, 1)
    return first + timedelta(days=(6 - first.weekday()) % 7 + 7 * (n - 1))


class CentralTime(tzinfo):
    _STD = timedelta(hours=-6)
    _HOUR = timedelta(hours=1)

    def _dst_span(self, year: int) -> tuple:
        return _nth_sunday(year, 3, 2) + timedelta(hours=2), _nth_sunday(year, 11, 1) + timedelta(hours=1)

    def dst(self, dt):
        if dt is None:
            return timedelta(0)
        start, end = self._dst_span(dt.year)
        local = dt.replace(tzinfo=None)
        if start <= local < end or (end <= local < end + self._HOUR and not dt.fold):
            return self._HOUR
        return timedelta(0)

    def utcoffset(self, dt):
        return self._STD + self.dst(dt)

    def tzname(self, dt):
        return "CDT" if self.dst(dt) else "CST"

    def fromutc(self, dt):
        standard = dt.replace(tzinfo=None) + self._STD
        start, end = self._dst_span(standard.year)
        if start <= standard < end:
            return (standard + self._HOUR).replace(tzinfo=self)
        if end <= standard < end + self._HOUR:
            return standard.replace(tzinfo=self, fold=1)
        return standard.replace(tzinfo=self)

    def __repr__(self):
        return "CentralTime()"


@lru_cache(maxsize=None)
def zone(name: str) -> tzinfo:
    try:
        return ZoneInfo(name)
    except Exception as e:
        try:
            ZoneInfo(DEFAULT_TIMEZONE)
        except Exception:
            log.warning("this server has no time zone database: %s is used for %r (US daylight saving rules)",
                        DEFAULT_TIMEZONE, name)
            return CentralTime()
        raise ConfigError(f"timezone {name!r} is not a known time zone") from e


def _flag(value: Any, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value or "").strip().lower()
    return True if text in ("true", "yes", "y", "1") else False if text in ("false", "no", "n", "0") else default


_CRON_ALIASES = {"@yearly": "0 0 1 1 *", "@annually": "0 0 1 1 *", "@monthly": "0 0 1 * *",
                 "@weekly": "0 0 * * 0", "@daily": "0 0 * * *", "@midnight": "0 0 * * *", "@hourly": "0 * * * *"}
_CRON_NAMES = {**{m: i + 1 for i, m in enumerate("JAN FEB MAR APR MAY JUN JUL AUG SEP OCT NOV DEC".split())},
               **{d: i for i, d in enumerate("SUN MON TUE WED THU FRI SAT".split())}}
_CRON_RANGES = ((0, 59), (0, 23), (1, 31), (1, 12), (0, 7))


def _cron_field(text: str, low: int, high: int) -> set:
    def number(part: str) -> int:
        value = _CRON_NAMES.get(part.upper())
        value = int(part) if value is None else value
        if not low <= value <= high:
            raise ValueError(f"{part} is outside {low}-{high}")
        return value

    out = set()
    for item in text.split(","):
        span, _, step = item.partition("/")
        step = int(step) if step else 1
        if step < 1:
            raise ValueError(f"step {step} must be 1 or more")
        if span == "*":
            start, end = low, high
        elif "-" in span:
            start, end = (number(p) for p in span.split("-", 1))
            if start > end:
                raise ValueError(f"range {span} runs backwards")
        else:
            start = number(span)
            end = high if "/" in item else start
        out.update(range(start, end + 1, step))
    return out


def cron_match(expression: str, when: datetime) -> bool:
    text = _CRON_ALIASES.get(str(expression).strip().lower(), str(expression).strip())
    fields = text.split()
    if len(fields) != 5:
        raise ConfigError(f"schedule {expression!r} must have 5 fields: minute hour day month weekday")
    try:
        minute, hour, day, month, weekday = (_cron_field(f, *r) for f, r in zip(fields, _CRON_RANGES))
    except ValueError as e:
        raise ConfigError(f"schedule {expression!r} is not a valid cron: {e}") from None
    weekday = {0 if d == 7 else d for d in weekday}
    if when.minute not in minute or when.hour not in hour or when.month not in month:
        return False
    day_ok, weekday_ok = when.day in day, (when.weekday() + 1) % 7 in weekday
    if fields[2] != "*" and fields[4] != "*":
        return day_ok or weekday_ok
    return day_ok and weekday_ok


@dataclass(frozen=True)
class Settings:
    meta_db: str
    environment: str = "DEV"
    env_value: Optional[str] = None
    timezone: str = DEFAULT_TIMEZONE
    chunk_rows: int = DEFAULT_CHUNK_ROWS
    access_lock: bool = True

    def __post_init__(self):
        object.__setattr__(self, "environment", str(self.environment or "DEV").strip().upper())
        object.__setattr__(self, "meta_db", self.resolve_env(str(self.meta_db or "").strip()))
        if not self.meta_db or not _IDENT.match(self.meta_db):
            raise ConfigError(f"meta_db {self.meta_db!r} must be a plain Teradata database name")
        zone(self.timezone)
        if int(self.chunk_rows) < 1:
            raise ConfigError("chunk_rows must be >= 1")

    @classmethod
    def from_mapping(cls, values: Mapping[str, Any]) -> "Settings":
        v = {str(k).lower(): val for k, val in (values or {}).items() if val not in (None, "")}
        if not v.get("meta_db"):
            raise ConfigError("'meta_db' is required")
        try:
            chunk = int(v.get("chunk_rows") or DEFAULT_CHUNK_ROWS)
        except (TypeError, ValueError) as e:
            raise ConfigError(f"chunk_rows must be a whole number, got {v.get('chunk_rows')!r}") from e
        return cls(meta_db=str(v["meta_db"]).strip(),
                   environment=str(v.get("environment") or v.get("load_env") or "DEV"),
                   env_value=v.get("env_value"),
                   timezone=str(v.get("timezone") or DEFAULT_TIMEZONE),
                   chunk_rows=chunk,
                   access_lock=_flag(v.get("access_lock"), True))

    def now(self) -> datetime:
        return datetime.now(zone(self.timezone)).replace(tzinfo=None)

    def today(self) -> date:
        return self.now().date()

    def resolve_env(self, text: Optional[str], identifier: bool = True) -> Optional[str]:
        if not text or not _ENV_TOKEN.search(text):
            return text
        value = self.env_value if self.env_value is not None else ENV_TOKEN_VALUES.get(self.environment,
                                                                                       self.environment.lower())
        low = value.lower()
        if not identifier and not low:
            return re.sub(r"_\$env|\$env_?", "", text, flags=re.IGNORECASE)

        def sub(m):
            token = m.group(0)[1:]
            return low.upper() if token.isupper() else low if token.islower() else low.capitalize()

        out = _ENV_TOKEN.sub(sub, text)
        if identifier and not low:
            return re.sub(r"_+(?=\.|$)|(?<=\.)_+|^_+", "", re.sub(r"_{2,}", "_", out))
        return re.sub(r"_{2,}", "_", out) if identifier else out

    def describe(self) -> dict:
        return {"meta_db": self.meta_db, "environment": self.environment, "env_value": self.env_value,
                "timezone": self.timezone, "chunk_rows": self.chunk_rows, "access_lock": self.access_lock}
