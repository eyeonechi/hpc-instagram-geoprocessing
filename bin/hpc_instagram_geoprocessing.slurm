#!/bin/bash
#SBATCH --nodes=1
#SBATCH --ntasks=8
#SBATCH --time=0-12:00:00

# Load required modules
module load Python/3.5.2-intel-2016.u3

# Launch multiple process python code
echo "Searching for mentions"
time mpiexec -n 8 python3 twitter_search_541635.py -i /data/projects/COMP90024/twitter.csv -m
echo "Searching for topics"
time mpiexec -n 8 python3 twitter_search_541635.py -i /data/projects/COMP90024/twitter.csv -t
echo "Searching for the keyword 'jumping'"
time mpiexec -n 8 python3 twitter_search_541635.py -i /data/projects/COMP90024/twitter.csv -s jumping
