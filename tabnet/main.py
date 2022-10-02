import pickle
import numpy as np 
import pandas as pd
import datetime
from data_preparation.dataloader import DataLoader
from data_preparation.data_processing import tabnet_preprocess
from conf.configuration import get_model_conf
from pytorch_tabnet.tab_model import TabNetRegressor
from evaluation import Accuracy5, Accuracy10, Accuracy15
from pytorch_tabnet.augmentations import RegressionSMOTE
from trainer import Trainer

model_name = 'tabnetregressor'
# data = pd.read_parquet('./data_feature/F_base_all_v5.parquet', engine='fastparquet')

data = pd.read_parquet('./dataset/data_10_02.parquet', engine='fastparquet')

# abnormal_table = pd.read_parquet('./dataset/abnormal_table.parquet', engine='fastparquet')
# data = data.merge(abnormal_table,how = 'left', on = 'Serial_Number')


excluding_feature = [
'Serial_Number',
'Queue_Time',
'time',
'Wait_Time',
'Meal_Time',
'Z04_cnt_for_date'
'Z04_leave_cnt_for_date'
'Z04_leave_ratio_for_date',
'Z04_cnt_for_period',
'Z04_leave_cnt_for_period',
'Z04_leave_ratio_for_period']
data_split_by_time = pd.to_datetime('2018-01-01')

config = get_model_conf(model_name)
data, features, config,corr_table = tabnet_preprocess(data, excluding_feature, config)
data['Wait_Time'] = np.expm1(data['Wait_Time'])

dataloader = DataLoader(k_fold = 1, split_size = 0.1, valid_split_date = '2017-07-01 00:00:00', test_split_date = '2018-01-30 00:00:00')
dataloader.transform(data, index_col = 'Serial_Number', 
                   date_col = 'Queue_Time', 
                   target_col = 'Wait_Time',
                   feature_col = features,
                   exculding_cols = excluding_feature)

ddf = dataloader.excluding_abnormal_data(data_type = 'train_dataset', method = 'isolation_forest',
                                        use_feature = ['Wait_Time','Z03_WAITTIME_cnt','Z03_WAITTIME_group_size'], 
                                        contamination = 0.42)
trainer = Trainer(config, model_name)
output = trainer.run(dataloader)
output_table = pd.DataFrame({'Serial_Number':output[0],'Queue_Time':output[1],'Wait_Time':output[2][:,0],'tabnet_pred_y':output[3][:,0]})