NAME=`python setup.py --name`

all: check test source deb

init:
	pip install .
