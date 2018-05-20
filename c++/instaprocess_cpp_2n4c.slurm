#!/bin/bash
#SBATCH --job-name="cpp24"
#SBATCH --time=00:05:00
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=4
#SBATCH --partition=physical
#SBATCH --output=cpp-2n4c.out

# Load required modules
module load OpenMPI/1.10.0-GCC-4.9.2
mpic++ instaprocess_cpp.cpp -o instaprocess_cpp

# Launch C++ code
echo "Processing bigInstagram.json"
time mpiexec ./instaprocess_cpp melbGrid.json bigInstagram.json
