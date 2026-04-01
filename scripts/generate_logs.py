from __future__ import annotations

import argparse
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from faker import Faker


STATIC_ASSETS = ["app.js", "styles.css", "logo.svg", "favicon.ico", "vendor.js"]
PRIVATE_RESOURCES = ["reports", "admin", "exports", "billing", "support"]


@dataclass(frozen=True)
class AccessPattern:
    method: str
    path_template: str
    statuses: tuple[int, ...]
    weight: int
    size_range: tuple[int, int]


@dataclass(frozen=True)
class ErrorPattern:
    module: str
    level: str
    message_template: str
    weight: int


ACCESS_PATTERNS = [
    AccessPattern("GET", "/", (200, 200, 200, 304), 20, (600, 4096)),
    AccessPattern("GET", "/health", (200, 200, 200, 503), 8, (64, 256)),
    AccessPattern("GET", "/api/users/{user_id}", (200, 200, 404, 500), 20, (256, 4096)),
    AccessPattern("POST", "/api/users", (201, 201, 400, 500), 10, (128, 1024)),
    AccessPattern("GET", "/api/orders/{order_id}", (200, 200, 404, 500), 16, (256, 4096)),
    AccessPattern("POST", "/api/orders", (201, 201, 400, 401), 12, (128, 1024)),
    AccessPattern("GET", "/static/{asset}", (200, 200, 304, 404), 18, (512, 8192)),
    AccessPattern("GET", "/admin", (200, 401, 403), 6, (128, 1024)),
]

ERROR_PATTERNS = [
    ErrorPattern("php", "error", "PHP Fatal error: Uncaught Exception: User {user_id} not found", 14),
    ErrorPattern("core", "error", "File does not exist: /var/www/html/{asset}", 10),
    ErrorPattern("authz_core", "error", "client denied by server configuration: /var/www/private/{resource}", 8),
    ErrorPattern("proxy_fcgi", "error", "AH01071: Got error 'Primary script unknown' for request /api/orders/{order_id}", 12),
    ErrorPattern("php", "warn", "PHP Warning: Undefined array key {key_id} in /var/www/app/controllers/user.php on line {line}", 10),
    ErrorPattern("rewrite", "notice", "redirected request to /index.php?route={route_id}", 6),
]


def weighted_choice(items, rng: random.Random):
    weights = [item.weight for item in items]
    return rng.choices(items, weights=weights, k=1)[0]


def build_host_pool(fake: Faker) -> list[str]:
    hosts = ["127.0.0.1"]
    hosts.extend(fake.ipv4_private() for _ in range(24))
    hosts.extend(fake.ipv4_public() for _ in range(12))
    return hosts


def build_user_agent_pool(fake: Faker) -> list[str]:
    agents = [fake.user_agent() for _ in range(18)]
    agents.extend(
        [
            "curl/8.0.1",
            "python-requests/2.32.0",
            "PostmanRuntime/7.37.3",
            "Googlebot/2.1",
            "YandexBot/3.0",
        ]
    )
    return agents


def build_referer_pool(fake: Faker) -> list[str]:
    referers = ["-"]
    referers.extend(fake.url() for _ in range(16))
    referers.extend(
        [
            "https://example.com/dashboard",
            "https://search.example.org/results?q=apache",
            "https://portal.example.net/logs",
        ]
    )
    return referers


def format_access_path(pattern: AccessPattern, rng: random.Random) -> str:
    return pattern.path_template.format(
        user_id=rng.randint(100, 130),
        order_id=rng.randint(1000, 1060),
        asset=rng.choice(STATIC_ASSETS),
    )


def format_error_message(pattern: ErrorPattern, rng: random.Random) -> str:
    return pattern.message_template.format(
        user_id=rng.randint(100, 130),
        order_id=rng.randint(1000, 1060),
        asset=rng.choice(["favicon.ico", "robots.txt", "apple-touch-icon.png", "manifest.json"]),
        resource=rng.choice(PRIVATE_RESOURCES),
        key_id=rng.randint(1, 12),
        line=rng.randint(20, 220),
        route_id=rng.randint(1, 8),
    )


def build_access_line(
    ts: datetime,
    rng: random.Random,
    hosts: list[str],
    user_agents: list[str],
    referers: list[str],
) -> str:
    pattern = weighted_choice(ACCESS_PATTERNS, rng)
    ip = rng.choice(hosts)
    path = format_access_path(pattern, rng)
    status = rng.choice(pattern.statuses)
    size = rng.randint(*pattern.size_range)
    user_agent = rng.choice(user_agents)
    referer = rng.choice(referers)
    timestamp = ts.strftime("%d/%b/%Y:%H:%M:%S +0000")
    return (
        f'{ip} - - [{timestamp}] "{pattern.method} {path} HTTP/1.1" '
        f'{status} {size} "{referer}" "{user_agent}"'
    )


def build_error_line(ts: datetime, rng: random.Random, hosts: list[str]) -> str:
    pattern = weighted_choice(ERROR_PATTERNS, rng)
    client_ip = rng.choice(hosts)
    pid = rng.randint(1000, 9999)
    message = format_error_message(pattern, rng)
    timestamp = ts.strftime("%a %b %d %H:%M:%S.%f %Y")[:-3]
    return (
        f"[{timestamp}] [{pattern.module}:{pattern.level}] "
        f"[pid {pid}] [client {client_ip}] {message}"
    )


def write_lines(path: Path, lines: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def generate(output_dir: Path, access_count: int, error_count: int, seed: int | None) -> None:
    rng = random.Random(seed)
    Faker.seed(seed)
    fake = Faker()
    if seed is not None:
        fake.seed_instance(seed)

    hosts = build_host_pool(fake)
    user_agents = build_user_agent_pool(fake)
    referers = build_referer_pool(fake)

    now = datetime.now(timezone.utc) - timedelta(minutes=15)
    access_lines = []
    error_lines = []

    for i in range(access_count):
        ts = now + timedelta(seconds=i * rng.randint(1, 3))
        access_lines.append(build_access_line(ts, rng, hosts, user_agents, referers))

    for i in range(error_count):
        ts = now + timedelta(seconds=i * rng.randint(5, 12))
        error_lines.append(build_error_line(ts, rng, hosts))

    write_lines(output_dir / "access.log", access_lines)
    write_lines(output_dir / "error.log", error_lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate demo Apache logs.")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("generated_logs"),
        help="Directory for generated log files.",
    )
    parser.add_argument(
        "--access-count",
        type=int,
        default=200,
        help="Number of access log lines.",
    )
    parser.add_argument(
        "--error-count",
        type=int,
        default=60,
        help="Number of error log lines.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Optional random seed for reproducible output.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    generate(args.output_dir, args.access_count, args.error_count, args.seed)
    print(f"Generated logs in {args.output_dir.resolve()}")


if __name__ == "__main__":
    main()
