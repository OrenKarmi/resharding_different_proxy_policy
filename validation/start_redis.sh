#!/bin/sh
# Start a clean Redis for local harness validation. Loopback bind is fine: the
# Windows-side connection reaches it through the WSL2 localhost relay.
sudo pkill -f redis-server >/dev/null 2>&1
sleep 1
rm -f /tmp/redis.log
redis-server \
  --bind 127.0.0.1 \
  --protected-mode no \
  --port 6379 \
  --save '' \
  --appendonly no \
  --timeout 0 \
  --tcp-keepalive 0 \
  --logfile /tmp/redis.log \
  --daemonize yes
echo "start_exit=$?"
sleep 2
echo "--- log ---"
tail -n 15 /tmp/redis.log
echo "--- listeners ---"
sudo ss -ltnp 2>/dev/null | grep 6379 || echo NOT_LISTENING
echo "--- ping ---"
redis-cli ping
