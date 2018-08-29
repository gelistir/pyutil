#!/usr/bin/env bash
docker-compose -f docker-compose.test.yml build test
./graph.sh
docker-compose -f docker-compose.test.yml run test
./sphinx.sh

# removes also all the containers linked to this particular service "test" defined in "docker-compose.test.yml"
docker-compose -f docker-compose.test.yml rm -v -f test