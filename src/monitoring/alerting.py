"""Deprecated: use src.monitoring.alert_dispatcher instead.

This module re-exports from alert_dispatcher for backward compatibility.
All new code should import from src.monitoring.alert_dispatcher directly.
"""

from src.monitoring.alert_dispatcher import (  # noqa: F401
    THESIS_ALLOWED_EVENT_TYPES as _THESIS_ALLOWED_EVENT_TYPES,
)
from src.monitoring.alert_dispatcher import (
    fmt_price as fmt_price,
)
from src.monitoring.alert_dispatcher import (
    fmt_size as fmt_size,
)
from src.monitoring.alert_dispatcher import (
    send_alert as send_alert,
)
from src.monitoring.alert_dispatcher import (
    send_alert_sync as send_alert_sync,
)
