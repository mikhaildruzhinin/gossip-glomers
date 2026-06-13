bin/maelstrom/maelstrom test -w broadcast \
  --bin "scripts/run.sh" \
  --node-count 25 \
  --time-limit 20 \
  --rate 100 \
  --latency 100 \
  --log-stderr
