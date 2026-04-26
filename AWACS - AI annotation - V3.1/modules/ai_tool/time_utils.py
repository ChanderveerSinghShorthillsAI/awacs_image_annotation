from datetime import datetime
from zoneinfo import ZoneInfo

_DEFAULT_TZ = "Asia/Kolkata"


def _tz() -> ZoneInfo:
    try:
        from .config_loader import config
        name = getattr(config, "timezone", None) or _DEFAULT_TZ
        return ZoneInfo(name)
    except Exception:
        return ZoneInfo(_DEFAULT_TZ)


def now_ist() -> datetime:
    """Tz-aware 'now' in the configured timezone (default Asia/Kolkata)."""
    return datetime.now(_tz())
