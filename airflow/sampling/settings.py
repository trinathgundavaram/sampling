from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Mapping, Optional
from zoneinfo import ZoneInfo

ENV_TOKEN_VALUES = {"DEV": "dev", "QA": "qa", "INT": "int", "UAT": "", "PROD": ""}
DEFAULT_TIMEZONE = "America/Chicago"
DEFAULT_CHUNK_ROWS = 1000

_ENV_TOKEN = re.compile(r"\$env", re.IGNORECASE)
_IDENT = re.compile(r"^[A-Za-z_$#][A-Za-z0-9_$#]{0,127}$")


class ConfigError(ValueError):
    pass


def _flag(value: Any, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value or "").strip().lower()
    return True if text in ("true", "yes", "y", "1") else False if text in ("false", "no", "n", "0") else default


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
        try:
            ZoneInfo(self.timezone)
        except Exception as e:  # noqa: BLE001
            raise ConfigError(f"timezone {self.timezone!r} is not a known time zone") from e
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
        return datetime.now(ZoneInfo(self.timezone)).replace(tzinfo=None)

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
