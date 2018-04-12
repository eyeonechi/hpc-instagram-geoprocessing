EXECS=instaprocess_cpp
MPICC?=mpic++
GRIDFILE?=../data/melbGrid.json
#INSTAFILE=../data/tinyInstagram.json
INSTAFILE=../data/mediumInstagram.json

all: ${EXECS}

instaprocess_cpp: ${EXECS}.cpp
	${MPICC} -o ${EXECS} ${EXECS}.cpp

run:
	${MPICC} -o ${EXECS} ${EXECS}.cpp
	mpirun -n 2 ./${EXECS} ${GRIDFILE} ${INSTAFILE}

run1:
	${MPICC} -o ${EXECS} ${EXECS}.cpp
	mpirun -n 1 ./${EXECS} ${GRIDFILE} ${INSTAFILE}

run3:
	${MPICC} -o ${EXECS} ${EXECS}.cpp
	mpirun -n 3 ./${EXECS} ${GRIDFILE} ${INSTAFILE}


run4:
	${MPICC} -o ${EXECS} ${EXECS}.cpp
	mpirun -n 4 ./${EXECS} ${GRIDFILE} ${INSTAFILE}

clean:
	rm ${EXECS}
