#!/usr/bin/env bash
source .env
echo "http://localhost:${PORT}"
docker-compose run -p ${PORT}:8888 jupyter