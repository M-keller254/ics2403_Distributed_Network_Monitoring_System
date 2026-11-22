"""Tiny helpers for reading configuration from environment variables."""
import os


def env_str(name, default=""):
    return os.environ.get(name, default)


def env_int(name, default=0):
    return int(os.environ.get(name, default))


def env_float(name, default=0.0):
    return float(os.environ.get(name, default))


def env_bool(name, default=False):
    v = os.environ.get(name)
    return default if v is None else v.strip().lower() in ("1", "true", "yes", "on")


def env_list(name, default=""):
    return [x.strip() for x in os.environ.get(name, default).split(",") if x.strip()]


def parse_peers(text, key_type=int):
    """'1=http://a:5001,2=http://b:5001' -> {1: 'http://a:5001', 2: ...}  (key_type=str keeps names like 'p1')"""
    peers = {}
    for item in text.split(","):
        if item.strip():
            pid, url = item.split("=", 1)
            peers[key_type(pid)] = url.strip().rstrip("/")
    return peers
