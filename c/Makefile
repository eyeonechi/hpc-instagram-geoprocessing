EXECS=instaprocess_c
MPICC?=mpicc
GRIDFILE?=../data/melbGrid.json
INSTAFILE=../data/mediumInstagram.json

all: ${EXECS}

instaprocess_c: instaprocess_c.c
	${MPICC} -o instaprocess_c instaprocess_c.c

run:
	${MPICC} -o instaprocess_c instaprocess_c.c
	mpirun -n 2 ./instaprocess_c ${GRIDFILE} ${INSTAFILE}

run1:
	${MPICC} -o instaprocess_c instaprocess_c.c
	mpirun -n 1 ./instaprocess_c ${GRIDFILE} ${INSTAFILE}

run4:
	${MPICC} -o instaprocess_c instaprocess_c.c
	mpirun -n 4 ./instaprocess_c ${GRIDFILE} ${INSTAFILE}

clean:
	rm ${EXECS}
