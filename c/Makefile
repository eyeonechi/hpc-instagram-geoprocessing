EXECS=hpc_instagram_geoprocessing
MPICC?=mpicc

all: ${EXECS}

hpc_instagram_geoprocessing: hpc_instagram_geoprocessing.c
	${MPICC} -o hpc_instagram_geoprocessing hpc_instagram_geoprocessing.c

run1:
	mpirun -n 1 ./hpc_instagram_geoprocessing

run2:
	mpirun -n 2 ./hpc_instagram_geoprocessing

run3:
	mpirun -n 3 ./hpc_instagram_geoprocessing

run4:
	mpirun -n 4 ./hpc_instagram_geoprocessing

clean:
	rm ${EXECS}
