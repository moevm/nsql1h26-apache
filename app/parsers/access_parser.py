from __future__ import annotations

from datetime import datetime
import re

from app.models.log import AccessParsed

ACCESS_LOG_PATTERN = re.compile(
    r'^(?P<remote_addr>\S+)\s+'
    r'\S+\s+'
    r'(?P<remote_user>\S+)\s+'
    r'\[(?P<time_local>[^\]]+)\]\s+'
    r'"(?P<request>[^"]*)"\s+'
    r'(?P<status>\d{3})\s+'
    r'(?P<body_bytes_sent>\S+)'
    r'(?:\s+"(?P<http_referer>[^"]*)"\s+"(?P<http_user_agent>[^"]*)")?\s*$'
)


class AccessLogParser:
    @staticmethod
    def parse(line: str) -> AccessParsed:
        match = ACCESS_LOG_PATTERN.match(line.strip())
        if not match:
            raise ValueError("Unable to parse Apache access log line")

        data = match.groupdict()
        request_method = None
        request_path = None
        request_protocol = None

        request = (data.get("request") or "").strip()
        if request and request != "-":
            request_parts = request.split()
            if len(request_parts) >= 1:
                request_method = request_parts[0]
            if len(request_parts) >= 2:
                request_path = request_parts[1]
            if len(request_parts) >= 3:
                request_protocol = request_parts[2]

        timestamp = datetime.strptime(data["time_local"], "%d/%b/%Y:%H:%M:%S %z")
        body_bytes_raw = data.get("body_bytes_sent")
        body_bytes_sent = 0 if body_bytes_raw in (None, "-") else int(body_bytes_raw)
        remote_user = data.get("remote_user")
        referer = data.get("http_referer")
        user_agent = data.get("http_user_agent")

        return AccessParsed(
            remote_addr=data.get("remote_addr"),
            remote_user=None if remote_user == "-" else remote_user,
            time_local=timestamp,
            request_method=request_method,
            request_path=request_path,
            request_protocol=request_protocol,
            status=int(data["status"]),
            body_bytes_sent=body_bytes_sent,
            http_referer=None if referer in (None, "-") else referer,
            http_user_agent=None if user_agent in (None, "-") else user_agent,
            ip=data.get("remote_addr"),
            method=request_method,
            uri=request_path,
            user_agent=None if user_agent in (None, "-") else user_agent,
            referer=None if referer in (None, "-") else referer,
        )
