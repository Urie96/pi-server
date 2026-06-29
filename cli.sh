#!/usr/bin/env bash

curl -N -X POST http://127.0.0.1:8081/chat \
  -H "X-Agent-Id: zhuzhu" \
  -H "Content-Type: application/json" \
  -d '{"prompt":"你好"}'
