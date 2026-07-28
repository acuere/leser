// Conformance sender: the REAL @sentry/node SDK, unmodified, DSN-only.
import * as Sentry from "@sentry/node";

const dsn = process.argv[2];

Sentry.init({
  dsn,
  release: "conformance-node-1.0",
  environment: "conformance",
  defaultIntegrations: false,
});

function loadProfile() {
  const user = undefined;
  return user.name; // TypeError
}

try {
  loadProfile();
} catch (err) {
  Sentry.captureException(err);
}

await Sentry.flush(10000);
console.log("node-sdk: flushed");
