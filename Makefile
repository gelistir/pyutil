#!make

# needed to get the ${PORT} environment variable
include .env
export

include pyutil/__init__.py
PROJECT_VERSION := ${__version__}
PACKAGE := ${__title__}

.PHONY: help build test teamcity graph doc tag clean


.DEFAULT: help

help:
	#@echo "make build"
	#@echo "       Build the docker image."
	@echo "make test"
	@echo "       Build the docker image for testing and run them."
	@echo "make teamcity"
	@echo "       Run tests, build a dependency graph and construct the documentation."
	@echo "make jupyter"
	@echo "       Start the Jupyter server."
	@echo "make graph"
	@echo "       Build a dependency graph."
	@echo "make doc"
	@echo "       Construct the documentation."
	@echo "make tag"
	@echo "       Make a tag on Github."



#build:
	#docker-compose build jupyter
	#docker-compose build --no-cache pyutil

test: #clean
	mkdir -p artifacts
	#docker-compose -f docker-compose.test.yml down -v --rmi all --remove-orphans
	docker-compose -f docker-compose.test.yml run sut

teamcity: test doc

jupyter:
	docker-compose build jupyter
	echo "http://localhost:${PORT}"
	docker-compose up jupyter

graph: test
	mkdir -p ${PWD}/artifacts/graph

	docker run --rm --mount type=bind,source=${PWD}/${PACKAGE},target=/pyan/${PACKAGE},readonly \
		   tschm/pyan:latest python pyan.py ${PACKAGE}/**/*.py -V --uses --defines --colored --dot --nested-groups > graph.dot

	# remove all the private nodes...
	grep -vE "____" graph.dot > graph2.dot

	docker run --rm -v ${PWD}/graph2.dot:/pyan/graph.dot:ro \
		   tschm/pyan:latest dot -Tsvg /pyan/graph.dot > artifacts/graph/graph.svg

	rm graph.dot graph2.dot

doc: test
	docker-compose -f docker-compose.test.yml run sut sphinx-build /source artifacts/build

tag: test
	git tag -a ${PROJECT_VERSION} -m "new tag"
	git push --tags


#clean-notebooks:
#	docker-compose exec jupyter jupyter nbconvert --ClearOutputPreprocessor.enabled=True --inplace **/*.ipynb
pypi: tag
	python setup.py sdist
	twine check dist/*
	twine upload dist/*