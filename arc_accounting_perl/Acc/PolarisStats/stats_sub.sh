#!/bin/bash
#$ -cwd -V
#$ -l h_rt=00:15:00
#$ -l h_vmem=2G
#$ -P N8HPC_LDS_MCD_SUPPORT
#$ -t 4-6
#$ -m be
perl Polstats.pl $SGE_TASK_ID 2014
mv $SGE_TASK_ID.txt Resu/

