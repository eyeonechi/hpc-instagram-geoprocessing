#!/bin/bash
#SBATCH --job-name="p18"
#SBATCH --time=00:05:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=8
#SBATCH --partition=physical
#SBATCH --output=python-1n8c.out

# Load required modules
module load Python/2.7.10-goolf-2015a

# Launch python code
echo "Processing bigInstagram.json"
time mpiexec python hpc_instagram_geoprocessing.py melbGrid.json bigInstagram.json
