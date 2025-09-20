# Analyzes CSV files to extract column data types using pandas
# Reads file list and outputs TABLE,COLUMN,TYPE format

import time
import pandas as pd

start = time.time()
filepath = "../files/"
with open("files.list") as ff:
    file_names = ff.readlines()
    print("CSV Input Files:", str(len(file_names)))


def all_files():
    for f in file_names:
        f = f.strip()
        df = pd.read_csv(filepath + f)
        data_types = df.dtypes
        for name, dtype in data_types.items():
            x = str(dtype)
            print(f.replace(".csv", "") + "," + name + "," + x)


def a_file(f):
    f = f.strip()
    df = pd.read_csv(filepath + f)
    data_types = df.dtypes
    for name, dtype in data_types.iteritems():
        x = "{}".format(dtype)
        print(f.replace(".csv", "") + "," + name + "," + x)


try:
    print("TABLE,COLUMN,TYPE")
    # all_files()
    a_file(file_names[0])
except Exception as ex:
    print(ex)
finally:
    stop = time.time()
    print("Seconds: ", stop - start)

exit(0)
