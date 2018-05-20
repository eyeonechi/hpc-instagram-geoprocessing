EXECS=HPCInstagramGeoprocessing
MPICC?=javac
GRIDFILE?=../data/melbGrid.json
INSTAFILE=../data/tinyInstagram.json

all: ${EXECS}

HPCInstagramGeoprocessing.java: ${EXECS}.java
	${MPICC} -cp .:$$MPJ_HOME/lib/mpj.jar *.java

run:
	${MPICC} -cp .:$$MPJ_HOME/lib/mpj.jar *.java
	mpjrun.sh -np 2 ${EXECS} ${GRIDFILE} ${INSTAFILE}

run11:
	${MPICC} -cp .:$$MPJ_HOME/lib/mpj.jar *.java
	mpjrun.sh -np 1 ${EXECS} ${GRIDFILE} ${INSTAFILE}

run18:
	${MPICC} -cp .:$$MPJ_HOME/lib/mpj.jar *.java
	mpjrun.sh -np 8 ${EXECS} ${GRIDFILE} ${INSTAFILE}

clean:
	rm ${EXECS}
