import sys
import pandas as pd

print(sys.argv)  # command line arguments we pass to the script

day = sys.argv[1]  # argument 1 is the information we pass

# Some logic with pandas
print(pd.__version__)

print(f'job finished successfully for day = {day}')