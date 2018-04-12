#!/bin/bash
#SBATCH --time=00:10:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --partition=physical
#SBATCH --output=java-1n1c.out

# Load required modules
module load Java/1.8.0_71
module load mpj/0.44

# Launch java code
echo "Processing bigInstagram.json"
javac -cp .:$MPJ_HOME/lib/mpj.jar *.java
time mpjrun.sh HPCInstagramGeoprocessing melbGrid.json bigInstagram.json
