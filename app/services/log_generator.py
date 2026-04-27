from __future__ import annotations

from datetime import datetime, timedelta, timezone
import random

from faker import Faker


STATIC_ASSETS = ["app.js", "styles.css", "logo.svg", "favicon.ico", "vendor.js"]
PRIVATE_RESOURCES = ["reports", "admin", "exports", "billing", "support"]


ACCESS_PATTERNS = [
    ("GET", "/", (200, 200, 200, 304), 20, (600, 4096)),
    ("GET", "/health", (200, 200, 200, 503), 8, (64, 256)),
    ("GET", "/api/users/{user_id}", (200, 200, 404, 500), 20, (256, 4096)),
    ("POST", "/api/users", (201, 201, 400, 500), 10, (128, 1024)),
    ("GET", "/api/orders/{order_id}", (200, 200, 404, 500), 16, (256, 4096)),
    ("POST", "/api/orders", (201, 201, 400, 401), 12, (128, 1024)),
    ("GET", "/static/{asset}", (200, 200, 304, 404), 18, (512, 8192)),
    ("GET", "/admin", (200, 401, 403), 6, (128, 1024)),
]


ERROR_PATTERNS = [
    ("php", "error", "PHP Fatal error: Uncaught Exception: User {user_id} not found", 14),
    ("core", "error", "File does not exist: /var/www/html/{asset}", 10),
    ("authz_core", "error", "client denied by server configuration: /var/www/private/{resource}", 8),
    ("proxy_fcgi", "error", "AH01071: Got error 'Primary script unknown' for request /api/orders/{order_id}", 12),
    ("php", "warn", "PHP Warning: Undefined array key {key_id} in /var/www/app/controllers/user.php on line {line}", 10),
    ("rewrite", "notice", "redirected request to /index.php?route={route_id}", 6),
]


def generate_lines(
    access_count: int,
    error_count: int,
    seed: int | None,
    days: int = 7,
    start_date: datetime | None = None,
) -> tuple[list[str], list[str]]:
    rng = random.Random(seed)
    Faker.seed(seed)
    fake = Faker()
    if seed is not None:
        fake.seed_instance(seed)

    hosts = _build_host_pool(fake)
    user_agents = _build_user_agent_pool(fake)
    referers = _build_referer_pool(fake)
    start = start_date or (datetime.now(timezone.utc) - timedelta(days=days))

    access_lines = [
        _build_access_line(ts, rng, hosts, user_agents, referers)
        for ts in _build_timestamps(access_count, rng, start, days)
    ]
    error_lines = [
        _build_error_line(ts, rng, hosts)
        for ts in _build_timestamps(error_count, rng, start, days)
    ]
    return access_lines, error_lines


def _weighted_choice(items, rng: random.Random):
    weights = [item[3] for item in items]
    return rng.choices(items, weights=weights, k=1)[0]


def _build_timestamps(count: int, rng: random.Random, start: datetime, days: int) -> list[datetime]:
    seconds_range = max(days, 1) * 24 * 60 * 60
    return sorted(start + timedelta(seconds=rng.randint(0, seconds_range - 1)) for _ in range(max(count, 0)))


def _build_host_pool(fake: Faker) -> list[str]:
    hosts = ["127.0.0.1"]
    hosts.extend(fake.ipv4_private() for _ in range(24))
    hosts.extend(fake.ipv4_public() for _ in range(12))
    return hosts


def _build_user_agent_pool(fake: Faker) -> list[str]:
    agents = [fake.user_agent() for _ in range(18)]
    agents.extend(["curl/8.0.1", "python-requests/2.32.0", "PostmanRuntime/7.37.3", "Googlebot/2.1", "YandexBot/3.0"])
    return agents


def _build_referer_pool(fake: Faker) -> list[str]:
    referers = ["-"]
    referers.extend(fake.url() for _ in range(16))
    referers.extend(["https://example.com/dashboard", "https://search.example.org/results?q=apache", "https://portal.example.net/logs"])
    return referers


def _format_access_path(template: str, rng: random.Random) -> str:
    return template.format(
        user_id=rng.randint(100, 130),
        order_id=rng.randint(1000, 1060),
        asset=rng.choice(STATIC_ASSETS),
    )


def _format_error_message(template: str, rng: random.Random) -> str:
    return template.format(
        user_id=rng.randint(100, 130),
        order_id=rng.randint(1000, 1060),
        asset=rng.choice(["favicon.ico", "robots.txt", "apple-touch-icon.png", "manifest.json"]),
        resource=rng.choice(PRIVATE_RESOURCES),
        key_id=rng.randint(1, 12),
        line=rng.randint(20, 220),
        route_id=rng.randint(1, 8),
    )


def _build_access_line(ts: datetime, rng: random.Random, hosts: list[str], user_agents: list[str], referers: list[str]) -> str:
    method, path_template, statuses, _, size_range = _weighted_choice(ACCESS_PATTERNS, rng)
    path = _format_access_path(path_template, rng)
    timestamp = ts.strftime("%d/%b/%Y:%H:%M:%S +0000")
    return (
        f'{rng.choice(hosts)} - - [{timestamp}] "{method} {path} HTTP/1.1" '
        f'{rng.choice(statuses)} {rng.randint(*size_range)} "{rng.choice(referers)}" "{rng.choice(user_agents)}"'
    )


def _build_error_line(ts: datetime, rng: random.Random, hosts: list[str]) -> str:
    module, level, message_template, _ = _weighted_choice(ERROR_PATTERNS, rng)
    timestamp = ts.strftime("%a %b %d %H:%M:%S.%f %Y")
    return (
        f"[{timestamp}] [{module}:{level}] [pid {rng.randint(1000, 9999)}] "
        f"[client {rng.choice(hosts)}] {_format_error_message(message_template, rng)}"
    )
