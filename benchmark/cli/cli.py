#!/usr/bin/env python3
"""
Shared helpers for command-line interfaces used across benchmark scripts.
"""
import argparse
from typing import Optional


def build_env_parser(description: Optional[str] = None) -> argparse.ArgumentParser:
    """
    Create an ArgumentParser with the common --env option.

    Args:
        description: Optional parser description shown in CLI help.

    Returns:
        argparse.ArgumentParser: parser preconfigured with the --env argument.
    """
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        "--env",
        type=str,
        default=None,
        help=(
            "Environment name for configuration override (e.g., 'dev', 'prod'). "
            "Loads config_<env>.yaml in addition to the base config.yaml."
        ),
    )
    return parser


def parse_env_args(description: Optional[str] = None) -> argparse.Namespace:
    """
    Parse CLI arguments using the common --env option.

    Args:
        description: Optional parser description shown in CLI help.

    Returns:
        argparse.Namespace: parsed arguments containing `env`.
    """
    parser = build_env_parser(description=description)
    return parser.parse_args()
