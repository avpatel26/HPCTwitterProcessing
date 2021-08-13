#!/bin/bash
#SBATCH --partition=cloud
#SBATCH --nodes=2
#SBATCH --ntasks=4
#SBATCH --time=03:00:00

module load Python/3.4.3-goolf-2015a
echo '2 node and 4 core'
mpiexec python assignment.py