#!/bin/bash
#SBATCH --partition=cloud
#SBATCH --nodes=1
#SBATCH --ntasks=8
#SBATCH --time=03:00:00

module load Python/3.4.3-goolf-2015a
echo '1 node and 8 core'
mpiexec python assignment.py