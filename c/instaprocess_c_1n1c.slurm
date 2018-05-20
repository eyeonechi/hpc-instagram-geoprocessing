#!/bin/bash
#SBATCH --job-name="c11"
#SBATCH --time=00:05:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --partition=physical
#SBATCH --output=c-1n1c.out

# Load required modules
module load OpenMPI/1.10.0-GCC-4.9.2
mpicc instaprocess_c.c -o instaprocess_c

# Launch C code
echo "Processing bigInstagram.json"
time mpiexec ./instaprocess_c melbGrid.json bigInstagram.json
