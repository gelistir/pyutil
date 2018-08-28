#!/usr/bin/env bash
docker-compose -f docker-compose.test.yml build
./graph.sh
docker-compose -f docker-compose.test.yml run test-pyutil
./sphinx.sh

# remove all containers that are exited...
docker rm $(docker ps -q -f status=exited)
# remove the volumes hanging around...
docker volume prune --force
