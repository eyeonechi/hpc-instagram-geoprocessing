#!/bin/bash
#SBATCH --job-name="cpp18"
#SBATCH --time=00:05:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=8
#SBATCH --partition=physical
#SBATCH --output=cpp-1n8c.out

# Load required modules
module load OpenMPI/1.10.0-GCC-4.9.2
mpic++ instaprocess_cpp.cpp -o instaprocess_cpp

# Launch C++ code
echo "Processing bigInstagram.json"
time mpiexec ./instaprocess_cpp melbGrid.json bigInstagram.json
