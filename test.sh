#!/usr/bin/env bash
docker build --file Dockerfile --target test --tag pyutil:test .

# delete all files in html-coverage
rm -rf $(pwd)/html-coverage/*

# run all tests, seems to be slow on teamcity
docker run --rm -v $(pwd)/html-coverage:/html-coverage  -v $(pwd)/html-report:/html-report pyutil:test

ret=$?

# build documentation
docker run --rm -v $(pwd)/source:/pyutil/source:ro -v $(pwd)/build:/pyutil/build pyutil:test sphinx-build source build

# delete the images used...
docker rmi -f pyutil:test

exit $ret