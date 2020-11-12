#!/bin/bash
#SBATCH --mail-user=kundiyao@gmail.com
#SBATCH --mail-type=ALL
#SBATCH --cpus-per-task=16  # Cores proportional to GPUs: 6 on Cedar, 16 on Graham.
#SBATCH --mem=64000M       # Memory proportional to GPUs: 32000 Cedar, 64000 Graham.
#SBATCH --time=1:00:00    # 03:00 or 12:00
#SBATCH --output=output.log

module load python/3
cd ../
python3 main.py
