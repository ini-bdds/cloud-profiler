#!/bin/bash
PACKAGE_BASE=/scratch/tools/tophat/1.3.0; export PACKAGE_BASE; . /scratch/tools/tophat/1.3.0/env.sh; PACKAGE_BASE=/scratch/tools/samtools/0.1.19; export PACKAGE_BASE; . /scratch/tools/samtools/0.1.19/env.sh;
echo "Hello from `hostname`"
echo "Going to source the file $$OSG_GRID/setup.sh"
echo "Resulting environment:"
printenv
echo "Output of lcg-cp --help (lcg-cp is a commonly used SRM client):"
echo 'Directories in \$$OSG_APP'
date
sleep 10
date
