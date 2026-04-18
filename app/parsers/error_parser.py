from __future__ import annotations

from datetime import datetime, timezone
import re

from app.models.log import ErrorParsed

ERROR_PREFIX_PATTERN = re.compile(r"^\[(?P<timestamp>[^\]]+)\]\s*(?P<rest>.*)$")
BRACKETED_SECTION_PATTERN = re.compile(r"^\[(?P<section>[^\]]+)\]\s*")


class ErrorLogParser:
    TIMESTAMP_FORMATS = (
        "%a %b %d %H:%M:%S.%f",
        "%a %b %d %H:%M:%S",
    )

    @classmethod
    def parse(cls, line: str) -> ErrorParsed:
        match = ERROR_PREFIX_PATTERN.match(line.strip())
        if not match:
            raise ValueError("Unable to parse Apache error log line")

        timestamp_raw = match.group("timestamp")
        rest = match.group("rest")
        timestamp = cls._parse_timestamp(timestamp_raw)
        sections: list[str] = []

        while rest.startswith("["):
            section_match = BRACKETED_SECTION_PATTERN.match(rest)
            if not section_match:
                break
            sections.append(section_match.group("section"))
            rest = rest[section_match.end() :]

        message = rest.strip()
        level = None
        client = None
        pid = None
        tid = None

        for section in sections:
            if ":" in section and section.split(":")[-1] in {"debug", "info", "notice", "warn", "error", "crit", "alert", "emerg"}:
                level = section.split(":")[-1]
                continue
            if section in {"debug", "info", "notice", "warn", "error", "crit", "alert", "emerg"}:
                level = section
                continue
            if section.startswith("client "):
                client = section[len("client ") :]
                continue
            if section.startswith("pid "):
                try:
                    pid = int(section[len("pid ") :].strip())
                except ValueError:
                    pid = None
                continue
            if section.startswith("tid "):
                try:
                    tid = int(section[len("tid ") :].strip())
                except ValueError:
                    tid = None
                continue

        return ErrorParsed(
            timestamp=timestamp,
            level=level,
            message=message,
            client=client,
            pid=pid,
            tid=tid,
        )

    @classmethod
    def _parse_timestamp(cls, value: str) -> datetime:
        raw = value.strip()

        ts_main = raw
        if " " in raw:
            maybe_main, maybe_suffix = raw.rsplit(" ", 1)
            if maybe_suffix.isdigit():
                ts_main = maybe_main

        for fmt in cls.TIMESTAMP_FORMATS:
            try:
                dt = datetime.strptime(ts_main, fmt)
                return dt.replace(year=datetime.now(timezone.utc).year, tzinfo=timezone.utc)
            except ValueError:
                continue

        raise ValueError(f"Unable to parse Apache error log timestamp: {value}")