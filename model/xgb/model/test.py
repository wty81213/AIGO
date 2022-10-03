import pandas as pd 
data = pd.read_parquet('./data_feature/F_base_cloud_03.parquet', engine='fastparquet')

output = pd.DataFrame(data.dtypes).reset_index().rename(columns = {0:'datatype'})

