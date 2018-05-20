EXECS=hpc_instagram_geoprocessing.py
GRIDFILE?=../data/melbGrid.json
INSTAFILE=../data/tinyInstagram.json

all: ${EXECS}

run:
	mpirun -n 2 python ${EXECS} ${GRIDFILE} ${INSTAFILE}

run11:
	mpirun -n 1 python ${EXECS} ${GRIDFILE} ${INSTAFILE}

run18:
	mpirun -n 8 python ${EXECS} ${GRIDFILE} ${INSTAFILE}
