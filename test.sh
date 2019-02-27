#!/usr/bin/env bash
docker-compose build --no-cache test
docker-compose run test
