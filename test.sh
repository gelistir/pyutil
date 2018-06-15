#!/usr/bin/env bash
docker build --file Dockerfile --no-cache --target test --tag pyutil:test .

# delete all files in html-coverage
rm -rf $(pwd)/html-coverage/*

# step 1: create a postgresql server running on server "test-postgresql" with user "postgres"
#         and database "postgres", password is "test"
docker rm -f test-postgresql

docker run --name test-postgresql -e POSTGRES_PASSWORD=test -d postgres:9.6

# step 2: wait for postgres to be up and running...
# docker run --link test-postgresql pyutil:test /pyutil/wait-for-postgres.sh test-postgresql



# run all tests, seems to be slow on teamcity
docker run --link test-postgresql --rm -v $(pwd)/html-coverage:/html-coverage  -v $(pwd)/html-report:/html-report pyutil:test

ret=$?

# build documentation
docker run --rm -v $(pwd)/source:/pyutil/source:ro -v $(pwd)/build:/pyutil/build pyutil:test sphinx-build source build

# delete the images used...
docker rmi -f pyutil:test
docker rm -f test-postgresql

exit $ret