import glob
import os
import pandas as pd

files = glob.glob('data/day/*.csv')
save_path = 'data/codis_data/2016-2018_day.csv'
# f = ['data/day/C0AC70-2016-01-01.csv', 'data/day/C0AC70-2016-01-02.csv']


def mergeFiles(files):
    # df = pd.concat(map(pd.read_csv, files), ignore_index=True)
    dfs = []
    for file in files:
        df = pd.read_csv(file, skiprows=1)
        name = os.path.split(file)[1]
        name = os.path.splitext(name)[0]
        name = name.replace('C0AC70-', '')
        
        df["ObsTime"] = name + '-' + df["ObsTime"].astype(str)
        dfs.append(df)
    df = pd.concat(dfs, ignore_index=True)
    df.to_csv(save_path, index=False)

    # print(df)
    # os._exit(0)
    # df.to_csv(save_path, index=False)

if __name__ == '__main__':
    mergeFiles(files)