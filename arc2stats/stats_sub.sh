#!/bin/bash
#$ -cwd -V
#$ -l h_rt=00:15:00
#$ -l h_vmem=2G
#$ -t 5-6
perl Arc2Stats.pl $SGE_TASK_ID 2018




