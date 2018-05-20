EXECS=instaprocess_c
MPICC?=mpicc
GRIDFILE?=../data/melbGrid.json
INSTAFILE=../data/tinyInstagram.json

all: ${EXECS}

instaprocess_c: instaprocess_c.c
	${MPICC} -o instaprocess_c instaprocess_c.c

run:
	${MPICC} -o instaprocess_c instaprocess_c.c
	mpirun -n 2 ./instaprocess_c ${GRIDFILE} ${INSTAFILE}

run11:
	${MPICC} -o instaprocess_c instaprocess_c.c
	mpirun -n 1 ./instaprocess_c ${GRIDFILE} ${INSTAFILE}

run18:
	${MPICC} -o instaprocess_c instaprocess_c.c
	mpirun -n 8 ./instaprocess_c ${GRIDFILE} ${INSTAFILE}

clean:
	rm ${EXECS}
