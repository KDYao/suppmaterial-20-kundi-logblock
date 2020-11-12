#!/bin/bash
#SBATCH --mail-user=kundiyao@gmail.com
#SBATCH --mail-type=ALL
#SBATCH --nodes=1
#SBATCH --mem=64000M       # Memory proportional to GPUs: 32000 Cedar, 64000 Graham.
#SBATCH --time=9:00:00    # 03:00 or 12:00
#SBATCH --output=output.log

module load python/3
cd ../
python3 main.py HDFS,Spark
