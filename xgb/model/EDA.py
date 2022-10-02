import os 
os.chdir('\\\\cloudsys\\user_space\\Cloud\\Side_Project\\AIGO-Predict_waiting')
# os.chdir('/Volumes/user_space/Cloud/Side_Project/AIGO-Predict_waiting')
import pandas as pd 
from visualization import * 
import plotly.io as pio
pio.renderers.default = 'notebook_connected'

queue = pd.read_parquet('./data_population/queue.parquet', engine='fastparquet')
orderinside = pd.read_parquet('./data_population/orderinside.parquet', engine='fastparquet')
orderoutside = pd.read_parquet('./data_population/orderoutside.parquet', engine='fastparquet')
order_achievement = pd.read_parquet('./data_population/order_achievement.parquet', engine='fastparquet')
base = pd.read_parquet('./data_population/base.parquet', engine='fastparquet')
data = pd.read_parquet('./data_feature/F_base_all_v3.parquet', engine='fastparquet')

distplot(data, 'Wait_Time',bin_size=3)
scatter_plot(data, x = 'B02_people_count', y = 'Wait_Time')
scatter_plot(data, x = 'B091_WAITTIME_SUM', y = 'Wait_Time')
scatter_plot(data, x = 'B092_WAITTIME_MAX', y = 'Wait_Time')

scatter_plot(D1, x = 'Z01_cnt_during_meal', y = 'Wait_Time')

data['A44_0_ratio_during_dinner_group_is_more_than_3'] = data['A44_0_ratio_during_dinner_group_is_more_than_3'].fillna(0)
scatter_plot(data, x = 'A44_0_ratio_during_dinner_group_is_more_than_3', y = 'Wait_Time')

scatter_plot(data, x = 'B02_people_count', y = 'Wait_Time')
scatter_plot(D2, x = 'Z02_WAITTIME_cnt', y = 'Wait_Time')
scatter_plot(D2, x = 'Z02_WAITTIME_group_size', y = 'Wait_Time')
distplot(data, 'B02_people_count',bin_size=3)


scatter_plot(data, x = 'B091_WAITTIME_SUM', y = 'Wait_Time')
scatter_plot(D2, x = 'Z02_WAITTIME_Time_sum', y = 'Wait_Time')


scatter_plot(data, x = 'B092_WAITTIME_MAX', y = 'Wait_Time')
scatter_plot(D2, x = 'Z02_WAITTIME_Time_max', y = 'Wait_Time')
distplot(D2, 'Z02_WAITTIME_Time_max',bin_size=3)
distplot(D2, 'Z02_WAITTIME_Time_max',bin_size=3)


scatter_plot(data, x = 'B093_WAITTIME_MIN', y = 'Wait_Time')
scatter_plot(D2, x = 'Z02_WAITTIME_Time_min', y = 'Wait_Time')
ddf
scatter_plot(predict_result, x = 'test_y', y = 'pred_y')

scatter_plot(ddf, x = 'Z03_WAITTIME_cnt', y = 'Wait_Time')
scatter_plot(ddf, x = 'Z03_WAITTIME_cnt', y = 'Z02_WAITTIME_Time_sum')
scatter_plot(predict_result, x = 'Z03_WAITTIME_cnt', y = 'Wait_Time')

scatter_plot(error_1, x = 'Z03_WAITTIME_group_size', y = 'test_y')

input_data = data[data['Z03_WAITTIME_group_size'] <= 20]
input_data = data[data['time_period'] == '晚餐時段']
input_data = data[data['time'] <= datetime.time(hour=11, minute=0, second=0)]

scatter_plot(input_data, x = 'Z02_WAITTIME_cnt', y = 'Wait_Time', color = 'time_period')
scatter_plot(input_data[input_data['time_period'].isin(['午間時段','開店前'])], x = 'Z02_WAITTIME_cnt', y = 'Wait_Time', color = 'time_period')
scatter_plot(input_data[input_data['time_period'].isin(['午餐時段','晚餐時段'])], x = 'Z02_WAITTIME_cnt', y = 'Wait_Time', color = 'time_period')

scatter_plot(input_data[input_data['time_period'].isin(['午間時段','開店前'])], x = 'Total_Count', y = 'Wait_Time', color = 'time_period')
scatter_plot(input_data[input_data['time_period'].isin(['午餐時段','晚餐時段'])], x = 'Total_Count', y = 'Wait_Time', color = 'time_period')


scatter_plot(predict_result, x = 'Z03_WAITTIME_cnt', y = 'Wait_Time', color = 'error')
scatter_plot(predict_result, x = 'Z03_WAITTIME_group_size', y = 'Wait_Time', color = 'error')
scatter_plot(predict_result, x = 'Z04_cnt_for_date', y = 'Wait_Time', color = 'error')

scatter_plot(data, x = 'Z02_WAITTIME_cnt', y = 'Z02_WAITTIME_Time_sum')


scatter_plot(error_table, x = 'Z02_WAITTIME_cnt', y = 'Wait_Time', color = 'error')
scatter_plot(error_table, x = 'Z02_WAITTIME_group_size', y = 'Wait_Time', color = 'error')
scatter_plot(error_table, x = 'Z04_cnt_for_date', y = 'Wait_Time', color = 'error')

feature_B15.columns
base_table = base.merge(feature_B15, how = 'left', on = 'Serial_Number')
col = 'B15_mean_of_waiting_time_for_the_same_cnt_and_month_1_aggfun_max'
mean_values = base_table[col].mean()
base_table[col] = base_table[col].fillna(mean_values)
scatter_plot(base_table, x = col, y = 'Wait_Time')

scatter_plot(data, x = col, y = 'Wait_Time')

from sklearn.ensemble import IsolationForest
clf = IsolationForest(contamination = 0.02).fit(D3[['Wait_Time','Z03_WAITTIME_group_size','Z03_WAITTIME_cnt']])
result = clf.predict(D3[['Wait_Time','Z03_WAITTIME_group_size','Z03_WAITTIME_cnt']])
# pd.Series(result).value_counts()
DDD = D3[result == 1]
DDD = DDD[~((D3['Z03_WAITTIME_cnt'] == 0)&(DDD['Wait_Time'] > 20))]
DD = D3[~((D3['Z03_WAITTIME_cnt'] == 0)&(D3['Wait_Time'] > 20))]

scatter_plot(DDD, x = 'Z03_WAITTIME_group_size', y = 'Wait_Time')
scatter_plot(DD, x = 'Z03_WAITTIME_group_size', y = 'Wait_Time')


scatter_plot(D4, x = 'Z04_leave_ratio_for_date_lag_1', y = 'Wait_Time')
scatter_plot(D4, x = 'Z04_leave_ratio_for_period_lag_7', y = 'Wait_Time', color = 'time_period')

AA = D4[D4['Z04_cnt_for_date'] == 0]
BB = AA['date'].unique()

D4[D4['Z04_cnt_for_period']]


scatter_plot(D2, x = 'Z02_WAITTIME_group_size', y = 'Wait_Time')
scatter_plot(D3, x = 'Z03_WAITTIME_group_size', y = 'Wait_Time')

DD5 = D5.fillna(0)
scatter_plot(DD5, x = 'Z05_period_15_min_info_for_customer_and_leave_ratio', y = 'Wait_Time')



def stardard_time_point(time_point):
    num_min_range = time_point.minute // 15 
    output_datetime = time_point.replace(minute = num_min_range * 15,second = 0,microsecond = 0)
    return output_datetime
input_base = linein.copy()
input_base = input_base[input_base['DT_INTIME'].notnull()]
input_base['Wait_Time'] = (input_base['DT_INTIME'] - input_base['DT_GETTIME']).dt.seconds/60
input_base = input_base[~input_base['FG_STATUS'].isin(['R'])]

input_base['Stardard_Queue_Time'] = input_base['DT_GETTIME'].apply(lambda x : stardard_time_point(x))
input_base['Stardard_Time'] = input_base['Stardard_Queue_Time'].dt.time
input_base['Stardard_week'] = input_base['Stardard_Queue_Time'].dt.dayofweek + 1
summary = input_base.groupby(['Stardard_week','Stardard_Time'],as_index = False)['Wait_Time'].mean()
summary = summary.sort_values('Stardard_Time')
import plotly.express as px
fig = px.line(summary, x="Stardard_Time", y='size',color = 'Stardard_week')
fig.show()


base = pd.read_parquet('./data_population/base.parquet', engine='fastparquet')
data = pd.read_parquet('./data_feature/F_base_cloud_03.parquet', engine='fastparquet')
base = base.merge(data, how = 'left', on = 'Serial_Number')


def stardard_time_point(time_point):
    num_min_range = time_point.minute // 20
    output_datetime = time_point.replace(minute = num_min_range * 15,second = 0,microsecond = 0)
    return output_datetime
input_base = orderinside.copy()
input_base = input_base[input_base['Queue_Time'].notnull()]


input_base['Stardard_Queue_Time'] = input_base['Queue_Time'].apply(lambda x : stardard_time_point(x))
input_base['Stardard_Time'] = input_base['Stardard_Queue_Time'].dt.time
input_base['Stardard_week'] = input_base['Stardard_Queue_Time'].dt.dayofweek + 1
summary = input_base.groupby(['Stardard_week','Stardard_Time'],as_index = False).size()
import plotly.express as px
fig = px.line(summary, x="Stardard_Time", y='size',color = 'Stardard_week')
fig.show()

