#!/bin/sh
set -eu

KESTRA_PID=""

cleanup() {
  if [ -n "$KESTRA_PID" ] && kill -0 "$KESTRA_PID" 2>/dev/null; then
    kill "$KESTRA_PID"
    wait "$KESTRA_PID" || true
  fi
}

trap cleanup INT TERM

/app/kestra server standalone &
KESTRA_PID="$!"

for _ in $(seq 1 90); do
  if curl -sf http://localhost:8081/health >/dev/null; then
    break
  fi

  if ! kill -0 "$KESTRA_PID" 2>/dev/null; then
    echo "Kestra server exited before becoming healthy."
    exit 1
  fi

  sleep 2
done

authenticated="false"
for _ in $(seq 1 60); do
  status_code="$(
    curl -s -o /dev/null -w "%{http_code}" \
      -u "${KESTRA_ADMIN_USERNAME}:${KESTRA_ADMIN_PASSWORD}" \
      http://localhost:8080/api/v1/main/flows/stock_pipeline/__bootstrap_probe__
  )"

  if [ "$status_code" != "000" ] && [ "$status_code" != "401" ] && [ "$status_code" != "403" ]; then
    authenticated="true"
    break
  fi

  if ! kill -0 "$KESTRA_PID" 2>/dev/null; then
    echo "Kestra server exited before the API became ready."
    exit 1
  fi

  sleep 2
done

if [ "$authenticated" != "true" ]; then
  echo "Kestra API authentication never became ready."
  cleanup
  exit 1
fi

if ! sh /app/kestra flow updates /app/flows \
  --delete \
  --server http://localhost:8080 \
  --user "${KESTRA_ADMIN_USERNAME}:${KESTRA_ADMIN_PASSWORD}"; then
  echo "Kestra flow bootstrap failed."
  cleanup
  exit 1
fi

wait "$KESTRA_PID"
