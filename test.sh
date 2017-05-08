#!/usr/bin/env bash
docker build --file Dockerfile-Test --tag pyutil:test .

# run a mongo container
docker run -p 27017:27017 --name testmongo -d mongo:latest

# run all tests, seems to be slow on teamcity
docker run --rm --link testmongo -v $(pwd)/html-coverage/:/html-coverage pyutil:test

ret=$?

# delete the mongo container
docker rm -f testmongo

docker rmi -f pyutil:test
exit $ret