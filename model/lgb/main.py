# from db.db import DBConnection
# from mysql.connector import Error
# from services.googletrends_service import GoogleTrends
# from services.gov_schedule_sefvice import GovSchedule
# from services.holiday_cn_service import HolidayCN
# from services.holiday_jp_service import HolidayJP
# from services.order import order_inside
import pandas as pd
import os
import time
import lightgbm as lgb
from services.connection import *
from functools import reduce
from services.eda_by_plot import *
from data_preparation.dataloader import DataLoader
from utils.utils import check_correlation, time_period_accuracy, get_important_features
from sklearn import metrics
from sklearn.metrics import mean_absolute_error
from data_preparation.data_processing import lightgbm_process
from conf.config import get_model_conf
from utils.feature_generate import generate_features, generate_numerical_features
from model.model_lgb import Lgb
from model.model_xgb import Xgb

# # read base from excel
# base_path = 'data_population/base.csv'
# dfbase_dataframe = pd.read_csv(base_path)

# order_inside_path = 'data_population/orderinside.csv'
# df_order_insiede_dataframe = pd.read_csv(order_inside_path)

# order_achievement_path = 'data_population/order_achievement.csv'
# df_order_achievement_dataframe = pd.read_csv(order_achievement_path)

# read parquet
base_parquet_path = 'data/data/base.parquet'
dfbase_dataframe = pd.read_parquet(base_parquet_path)

order_inside_path = 'data/data/orderinside.parquet'
df_order_insiede_dataframe = pd.read_parquet(order_inside_path)

order_achievement_path = 'data/data/order_achievement.parquet'
df_order_achievement_dataframe = pd.read_parquet(order_achievement_path)

data_path = "data/data/F_base_all_v5.parquet"
df = pd.read_parquet(data_path)

model_name = "xgb"

def main():

  ############################# generate features #############################
  # generate_features(dfbase_dataframe, df_order_insiede_dataframe, df_order_achievement_dataframe)

  # numerical_features = generate_numerical_features(df)


  ####################### EDA by plot #######################
  # B098_WAITTIME_AVG_CONDITION(df)
  # people_by_weekDay(df)

  ####################### model #######################
  config = get_model_conf(model_name)

  excluding_feature = ['Serial_Number', 'Queue_Time', 'Wait_Time', 'Meal_Time', 'Z04_cnt_for_date', 'Z04_leave_cnt_for_date'
                        , 'Z04_leave_ratio_for_date', 'Z04_leave_ratio_for_date', 'Z04_cnt_for_period', 'Z04_leave_cnt_for_period'
                        , 'Z04_leave_ratio_for_period']
  
  if model_name == "lgb":
      data, features = lightgbm_process(df, excluding_feature, '2017-10-01 00:00:00')
      model = Lgb(model_name, config)
  
  elif model_name == "xgb":
      data, features = lightgbm_process(df, excluding_feature, '2017-10-01 00:00:00')
      model = Xgb(model_name, config)

  # # ###################################### DataLoader New ######################################

  dataloader = DataLoader(k_fold = 1, split_size = 0.3, valid_split_date = '2017-10-01 00:00:00', test_split_date = '2018-01-01 00:00:00')

  dataloader.transform(data, index_col = 'Serial_Number', date_col = 'Queue_Time', target_col = 'Wait_Time', 
                       feature_col = features, exculding_cols = excluding_feature)
  train_dataset = dataloader.training_data()
  _, (test_x, test_y) = dataloader.testing_data()

  # # ###################################### training model ######################################

  test_acc = []

  for _, (train_x, train_y), (valid_x, valid_y) in train_dataset:

    model_train = model.train(train_x, train_y, valid_x, valid_y)

    # train prediction
    train_predicted_y = model_train.prediction(train_x)
    # r2_score_stratified.append(metrics.r2_score(test_y, predicted_y))
    print(f"train r2 score: {metrics.r2_score(train_y, train_predicted_y)}")
    print(f"train mae: {mean_absolute_error(train_predicted_y, train_y)}")
    print(f"train mse: {metrics.mean_squared_log_error(train_y, train_predicted_y)}")

    # valid prediction
    valid_predicted_y = model_train.prediction(valid_x)
    # r2_score_stratified.append(metrics.r2_score(test_y, predicted_y))
    print(f"valid r2 score: {metrics.r2_score(valid_y, valid_predicted_y)}")
    print(f"valid mae: {mean_absolute_error(valid_predicted_y, valid_y)}")
    print(f"valid mse: {metrics.mean_squared_log_error(valid_y, valid_predicted_y)}")

    # test prediction
    test_predicted_y = model_train.prediction(test_x)
    # r2_score_stratified.append(metrics.r2_score(test_y, predicted_y))
    print(f"test r2 score: {metrics.r2_score(test_y, test_predicted_y)}")
    print(f"test mae: {mean_absolute_error(test_predicted_y, test_y)}")
    print(f"test mse: {metrics.mean_squared_log_error(test_y, test_predicted_y)}")

    train_acc_5, train_acc_10, train_acc_15 = model_train.evaluate(train_y, train_predicted_y)

    print(f"train_acc_15: {train_acc_15}")
    print(f"train_acc_10: {train_acc_10}")
    print(f"train_acc_5: {train_acc_5}")

    val_acc_5, val_acc_10, val_acc_15 = model_train.evaluate(valid_y, valid_predicted_y)

    print(f"val_acc_15: {val_acc_15}")
    print(f"val_acc_10: {val_acc_10}")
    print(f"val_acc_5: {val_acc_5}")

    test_acc_5, test_acc_10, test_acc_15 = model_train.evaluate(test_y, test_predicted_y)

    print(f"test_acc_15: {test_acc_15}")
    print(f"test_acc_10: {test_acc_10}")
    print(f"test_acc_5: {test_acc_5}")

    test_acc.append(test_acc_15)

    # get_important_features(train_x, model)
    os._exit(0)

  ############################################################## read data from DB ##############################################################
  # connect to DB
  # db = Base()
  # db.get_data_from_table()

  # df = time_period_queue_amount(dfbase_dataframe, orderinside=df_order_insiede_dataframe)
  # print(df)

  # df = time_period_need_queue(dfbase_dataframe, orderinside=df_order_insiede_dataframe, queue=df_queue_dataframe)
  # print(df)

  # df = time_period_orderInside_amount(dfbase_dataframe, orderinside=df_order_insiede_dataframe)
  # print(df)

  # df = time_period_waiting_time(dfbase_dataframe, orderinside=df_order_insiede_dataframe)
  # print(df)
  # print(len(df))

  # df = time_period_waiting_time_ratio(dfbase_dataframe, orderinside=df_order_insiede_dataframe)
  # print(df)

if __name__ == "__main__":
    main()