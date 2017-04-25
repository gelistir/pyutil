#!/usr/bin/env bash
docker build --file Dockerfile-Test --tag pyutil:latest .

# run a mongo container
docker run -p 27017:27017 --name mongo -d mongo:latest

# run all tests, seems to be slow on teamcity
docker run --rm --link mongo \
 	-v $(pwd)/test:/pyutil/test \
 	-v $(pwd)/pyutil:/pyutil/pyutil \
 	pyutil:latest nosetests -w /pyutil/test --with-timer

# delete the mongo container
docker rm -f mongo
