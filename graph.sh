#!/usr/bin/env bash
docker-compose -f docker-compose.test.yml run test-pyutil python pyan.py pyutil/**/*.py -v --uses --defines --colored --dot --nested-groups > graph/architecture.dot
docker-compose -f docker-compose.test.yml run test-pyutil dot -Tsvg graph/architecture.dot > graph/architecture.svg

