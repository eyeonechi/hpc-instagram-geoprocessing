EXECS=instaprocess_cpp
MPICC?=mpic++
GRIDFILE?=../data/melbGrid.json
INSTAFILE=../data/mediumInstagram.json

all: ${EXECS}

instaprocess_cpp: ${EXECS}.cpp
	${MPICC} -o ${EXECS} ${EXECS}.cpp

run:
	${MPICC} -o ${EXECS} ${EXECS}.cpp
	mpirun -n 2 ./${EXECS} ${GRIDFILE} ${INSTAFILE}

run11:
	${MPICC} -o ${EXECS} ${EXECS}.cpp
	mpirun -n 1 ./${EXECS} ${GRIDFILE} ${INSTAFILE}

run18:
	${MPICC} -o ${EXECS} ${EXECS}.cpp
	mpirun -n 8 ./${EXECS} ${GRIDFILE} ${INSTAFILE}

clean:
	rm ${EXECS}
