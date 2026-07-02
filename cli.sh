#!/usr/bin/env bash

host="http://127.0.0.1:8081"
# host="http://127.0.0.1:3115"

curl -N -X POST $host/chat \
  -H "X-Agent-Id: yangrui" \
  -H "Content-Type: application/json" \
  -d '{"prompt":"看一下当前文件夹有什么文件"}'
