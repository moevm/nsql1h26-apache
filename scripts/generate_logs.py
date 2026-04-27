from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path

from app.services.log_generator import generate_lines


def write_lines(path: Path, lines: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def generate(
    output_dir: Path,
    access_count: int,
    error_count: int,
    seed: int | None,
    days: int = 7,
    start_date: datetime | None = None,
) -> None:
    access_lines, error_lines = generate_lines(access_count, error_count, seed, days, start_date)
    mixed_lines = sorted(access_lines + error_lines)

    write_lines(output_dir / "access.log", access_lines)
    write_lines(output_dir / "error.log", error_lines)
    write_lines(output_dir / "mixed.log", mixed_lines)


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
    parser.add_argument(
        "--days",
        type=int,
        default=7,
        help="Number of days to distribute generated timestamps across.",
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default=None,
        help="UTC start date in YYYY-MM-DD format. Defaults to now minus --days.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    start_date = None
    if args.start_date:
        start_date = datetime.fromisoformat(args.start_date).replace(tzinfo=timezone.utc)
    generate(args.output_dir, args.access_count, args.error_count, args.seed, args.days, start_date)
    print(f"Generated logs in {args.output_dir.resolve()}")


if __name__ == "__main__":
    main()
