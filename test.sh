#!/usr/bin/env bash
docker build --file Dockerfile-Test --tag pyutil:test .

# run a mongo container
docker run -p 27017:27017 --name testmongo -d mongo:latest

# run all tests, seems to be slow on teamcity
docker run --rm --link testmongo -v $(pwd)/html-coverage/:/html-coverage pyutil:test

ret=$?

# build documentation
docker run --rm -v $(pwd)/source:/pyutil/source:ro -v $(pwd)/build:/pyutil/build pyutil:test sphinx-build source build

# delete the mongo container
docker rm -f testmongo

# delete the images used...
docker rmi -f mongo:latest
docker rmi -f pyutil:test

exit $ret