"""Deprecated: use src.monitoring.alert_dispatcher instead.

This module is retained for backward compatibility only.
The AlertSystem class and get_alert_system() are no longer used by live code.
All alerting is handled by src.monitoring.alert_dispatcher.
"""

from src.monitoring.alert_dispatcher import AlertLevel as AlertLevel  # noqa: F401
from src.monitoring.alert_dispatcher import send_alert as send_alert  # noqa: F401
from src.monitoring.alert_dispatcher import send_alert_sync as send_alert_sync  # noqa: F401
