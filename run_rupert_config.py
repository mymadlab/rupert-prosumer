#!/usr/bin/env python3
"""Simple CLI runner for RupertConfig."""

import argparse
import json
import os
import sys

try:
    from rupert_config import RupertConfig
except ImportError:
    from rupert_prosumer.rupert_config import RupertConfig


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Load and print Rupert JSON configuration.",
    )
    parser.add_argument(
        "--config-file",
        default=os.environ.get("RUPERT_CONFIG_PATH"),
        help="Path to the Rupert config JSON file. Defaults to RUPERT_CONFIG_PATH.",
    )
    parser.add_argument(
        "--compact",
        action="store_true",
        help="Print compact JSON instead of pretty formatted JSON.",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if not args.config_file:
        parser.error("Missing --config-file and RUPERT_CONFIG_PATH is not set.")

    config = RupertConfig(args.config_file).config

    if args.compact:
        print(json.dumps(config, separators=(",", ":")))
    else:
        print(json.dumps(config, indent=2, sort_keys=True))

    return 0


if __name__ == "__main__":
    sys.exit(main())
