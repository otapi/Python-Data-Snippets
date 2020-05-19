import pandas as pd
import numpy as np
import logging
import dask.dataframe as dd

# Code snippets to ETL data with dask (instead of pandas) and CSV or TAB delimited files
"""
Requirements: Python 3.8+
python -m pip install dask[dataframe] --upgrade
"""

CSVFile = r"C:\test.csv"

# Load (with parse dates)
def mydateparser(x):
    if x != x:
        return np.nan
    elif not isinstance(x, str):
        return np.nan
    elif x.startswith("0001"):
        return np.nan
    else:
        ret = 0
        try:
            ret = datetime.datetime.strptime(x, "%Y-%m-%d %H:%M:%S")
        except:
            logging.warning(f"Can't parse value as datetime, replace to null: {x}")
            return np.nan
        return ret

logging.info(f"Load the file into dataframe: {CSVFile}")
headers = [
        'Field1',
        'Field2'
    ]
dtypes = {
        'Field1': 'str',
        'Field2': 'str'
    }
df = dd.read_csv(CSVFile, sep='\t', header=None, dtype=dtypes, names=headers, encoding='utf-8', parse_dates=['Field2'], date_parser=mydateparser)

logging.info("Fix nulls")
# pandas
#df.replace([np.inf, -np.inf], np.nan, inplace = True)
df = df.mask(df == np.inf, np.nan).mask(df == -np.inf, np.nan)

df = df.fillna("")

# Export from pandas dataframe into CSV
df.to_csv(file_name, sep='\t', encoding='utf-8', index=False)
