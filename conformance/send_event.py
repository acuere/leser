"""Conformance sender: the REAL sentry-sdk (Python), unmodified, pointed at
leser by DSN alone (order.md §3). Sends one handled exception and flushes."""
import sys

import sentry_sdk

dsn = sys.argv[1]

sentry_sdk.init(
    dsn=dsn,
    release="conformance-py-1.0",
    environment="conformance",
    default_integrations=False,  # deterministic: no auto-instrumentation noise
)

def checkout_cart():
    prices = {"apple": 3}
    return prices["banana"]  # KeyError

try:
    checkout_cart()
except KeyError:
    sentry_sdk.capture_exception()

ok = sentry_sdk.flush(timeout=10)
print("python-sdk: flushed")
