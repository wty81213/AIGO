import numpy as np 
import pandas as pd 
import os 
os.chdir('/Volumes/user_space/Cloud/Side_Project/AIGO-Predict_waiting/')

print('===== import data =====')
queue_2016 = pd.read_excel('./data/Queue.xlsx', sheet_name = '2016')
print('***** Sucessfully for queue_2016*****')
queue_2017 = pd.read_excel('./data/Queue.xlsx', sheet_name = '2017')
print('***** Sucessfully for queue_2017*****')
queue_2018 = pd.read_excel('./data/Queue_2018.xlsx', sheet_name = '2018')
print('***** Sucessfully for queue_2018*****')
queue_info = pd.read_excel('./data/Queue.xlsx', sheet_name = '欄位說明')
queue_info = queue_info[['欄位名稱','欄位說明','備註']]
print('***** Sucessfully for queue_info*****')

orderinside_2016 = pd.read_excel('./data/OrderInside.xlsx', sheet_name = '2016內用訂單')
print('***** Sucessfully for orderinside_2016*****')
orderinside_2017 = pd.read_excel('./data/OrderInside.xlsx', sheet_name = '2017內用訂單')
print('***** Sucessfully for orderinside_2017*****')
orderinside_2018 = pd.read_excel('./data/OrderInside_2018.xlsx', sheet_name = '2018內用訂單')
print('***** Sucessfully for orderinside_2018*****')
orderinside_info = pd.read_excel('./data/OrderInside.xlsx', sheet_name = '欄位說明')
orderinside_info = orderinside_info[['欄位名稱','欄位說明','註']]
print('***** Sucessfully for orderinside_info*****')

orderoutside_2016 = pd.read_excel('./data/OrderOutside.xlsx', sheet_name = '2016外帶訂單')
print('***** Sucessfully for orderoutside_2016*****')
orderoutside_2017 = pd.read_excel('./data/OrderOutside.xlsx', sheet_name = '2017外帶訂單')
print('***** Sucessfully for orderoutside_2017*****')
orderoutside_2018 = pd.read_excel('./data/OrderOutside_2018.xlsx', sheet_name = '2018外帶訂單')
print('***** Sucessfully for orderoutside_2018*****')
orderoutside_info = pd.read_excel('./data/OrderOutside.xlsx', sheet_name = '欄位說明')
orderoutside_info = orderoutside_info[['欄位名稱','欄位說明','註']]
orderoutside_info = orderoutside_info[orderoutside_info['欄位名稱'].notnull()]
print('***** Sucessfully for orderoutside_info*****')

order_achievement_2016_Q1 = pd.read_excel('./data/Order_Achievement_2016_Q1.xlsx',dtype = {'Item_Serial':str,'Priority_Serial':str})
print('***** Sucessfully for order_achievement_2016_Q1*****')
order_achievement_2016_Q2 = pd.read_excel('./data/Order_Achievement_2016_Q2.xlsx',dtype = {'Item_Serial':str,'Priority_Serial':str})
print('***** Sucessfully for order_achievement_2016_Q2*****')
order_achievement_2016_Q3 = pd.read_excel('./data/Order_Achievement_2016_Q3.xlsx',dtype = {'Item_Serial':str,'Priority_Serial':str})
print('***** Sucessfully for order_achievement_2016_Q3*****')
order_achievement_2016_Q4 = pd.read_excel('./data/Order_Achievement_2016_Q4.xlsx',dtype = {'Item_Serial':str,'Priority_Serial':str})
print('***** Sucessfully for order_achievement_2016_Q4*****')
order_achievement_2017_Q1 = pd.read_excel('./data/Order_Achievement_2017_Q1.xlsx',dtype = {'Item_Serial':str,'Priority_Serial':str})
print('***** Sucessfully for order_achievement_2017_Q1*****')
order_achievement_2017_Q2 = pd.read_excel('./data/Order_Achievement_2017_Q2.xlsx',dtype = {'Item_Serial':str,'Priority_Serial':str})
print('***** Sucessfully for order_achievement_2017_Q2*****')
order_achievement_2017_Q3 = pd.read_excel('./data/Order_Achievement_2017_Q3.xlsx',dtype = {'Item_Serial':str,'Priority_Serial':str})
print('***** Sucessfully for order_achievement_2017_Q3*****')
order_achievement_2017_Q4 = pd.read_excel('./data/Order_Achievement_2017_Q4.xlsx',dtype = {'Item_Serial':str,'Priority_Serial':str})
print('***** Sucessfully for order_achievement_2017_Q4*****')
order_achievement_2018_Q1 = pd.read_excel('./data/Order_Achievement_2018-Q1.xlsx',dtype = {'Item_Serial':str,'Priority_Serial':str})
print('***** Sucessfully for order_achievement_2018_Q1*****')
order_achievement_2018_Q2 = pd.read_excel('./data/Order_Achievement_2018-Q2.xlsx',dtype = {'Item_Serial':str,'Priority_Serial':str})
print('***** Sucessfully for order_achievement_2018_Q2*****')
order_achievement_2018_Q3 = pd.read_excel('./data/Order_Achievement_2018-Q3.xlsx',dtype = {'Item_Serial':str,'Priority_Serial':str})
print('***** Sucessfully for order_achievement_2018_Q3*****')
order_achievement_2018_Q4 = pd.read_excel('./data/Order_Achievement_2018-Q4.xlsx',dtype = {'Item_Serial':str,'Priority_Serial':str})
print('***** Sucessfully for order_achievement_2018_Q4*****')
order_achievement_info = pd.read_excel('./data/Order_Achievement_2016_Q1.xlsx', sheet_name = '欄位說明')
order_achievement_info = order_achievement_info[['欄位名稱','欄位說明','註']]
print('***** Sucessfully for order_achievement *****')

linein_2016 = pd.read_excel('./data/Linein_D.xlsx', sheet_name = '2016')
print('***** Sucessfully for Linein_2016*****')
linein_2017 = pd.read_excel('./data/Linein_D.xlsx', sheet_name = '2017')
print('***** Sucessfully for Linein_2017*****')
linein_2018 = pd.read_excel('./data/Linein_D_2018.xlsx', sheet_name = '2018')
print('***** Sucessfully for Linein_2018*****')
linein_info = pd.read_excel('./data/Linein_D.xlsx', sheet_name = '欄位說明')
linein_info = linein_info[['欄位名稱','中文註釋','備註']]
print('***** Sucessfully for linein_info*****')

prob_table = pd.read_excel('./data/產品分類.xlsx',sheet_name='產品分類',dtype ={'產品分類':str,'產品代號':str})
print('***** Sucessfully for 產品分類 *****')

print('===== merge data =====')
order_achievement = pd.concat([order_achievement_2016_Q1, order_achievement_2016_Q2,\
                               order_achievement_2016_Q3,order_achievement_2016_Q4,\
                               order_achievement_2017_Q1, order_achievement_2017_Q2,\
                               order_achievement_2017_Q3,order_achievement_2017_Q4,\
                               order_achievement_2018_Q1, order_achievement_2018_Q2,\
                               order_achievement_2018_Q3,order_achievement_2018_Q4],ignore_index = True)

queue = pd.concat([queue_2016, queue_2017,queue_2018], ignore_index = True)
orderinside = pd.concat([orderinside_2016, orderinside_2017,orderinside_2018], ignore_index = True)
orderoutside = pd.concat([orderoutside_2016, orderoutside_2017,orderoutside_2018], ignore_index = True)
linein = pd.concat([linein_2016, linein_2017,linein_2018], ignore_index = True)

print('===== handling queue data =====')
queue['Reserve_No'] = queue['Reserve_No'].astype(str).str.strip()
queue['Serial_Number'] = queue['Serial_Number'].astype(str).str.replace('\.0','')
queue['Queue_Type'] = queue['Queue_Type'].astype(str).str.replace('\.0','')
queue['Nation_Code'] = queue['Nation_Code'].astype(str).str.replace('\.0','')
queue['Team_Flag'] = queue['Team_Flag'].astype(bool)
queue['Unhandy_Flag'] = queue['Unhandy_Flag'].astype(bool)
queue['Queue_No'] = queue['Queue_No'].astype(str).str.replace('\.0','')

queue.loc[queue['Reserve_No'] == 'nan','Reserve_No'] = np.nan
queue.loc[queue['Serial_Number'] == 'nan','Serial_Number'] = np.nan
queue.loc[queue['Queue_Type'] == 'nan','Queue_Type'] = np.nan
queue.loc[queue['Nation_Code'] == 'nan','Nation_Code'] = np.nan
queue.loc[queue['Queue_No'] == 'nan','Queue_No'] = np.nan
# queue.dtypes

print('===== handling orderinside data =====')
orderinside['Serial_Number'] = orderinside['Serial_Number'].astype(str).str.replace('\.0','')
orderinside['Order_Type'] = orderinside['Order_Type'].astype(str).str.replace('\.0','')
orderinside['Nation_Code'] = orderinside['Nation_Code'].astype(str).str.replace('\.0','')
orderinside['Nation_People'] = orderinside['Nation_People'].astype(str).str.replace('\.0','')
orderinside['Team_Flag'] = orderinside['Team_Flag'].astype(bool)
orderinside['Unhandy_Flag'] = orderinside['Unhandy_Flag'].astype(bool)
orderinside['TakeOut_Flag'] = orderinside['TakeOut_Flag'].astype(bool)
orderinside['Queue_No'] = orderinside['Queue_No'].astype(str).str.replace('\.0','')
orderinside['Table_No'] = orderinside['Table_No'].astype(str).str.replace('\.0','')

orderinside.loc[orderinside['Serial_Number'] == 'nan','Serial_Number'] = np.nan
orderinside.loc[orderinside['Order_Type'] == 'nan','Order_Type'] = np.nan
orderinside.loc[orderinside['Nation_Code'] == 'nan','Nation_Code'] = np.nan
orderinside.loc[orderinside['Nation_People'] == 'nan','Nation_People'] = np.nan
orderinside.loc[orderinside['Queue_No'] == 'nan','Queue_No'] = np.nan
orderinside.loc[orderinside['Table_No'] == 'nan','Table_No'] = np.nan
# orderinside.dtypes

print('===== handling orderoutside data =====')
orderoutside['Serial_Number'] = orderoutside['Serial_Number'].astype(str).str.replace('\.0','')
orderoutside['Order_Type'] = orderoutside['Order_Type'].astype(str).str.replace('\.0','')
orderoutside['Nation_Code'] = orderoutside['Nation_Code'].astype(str).str.replace('\.0','')

orderoutside.loc[orderoutside['Serial_Number'] == 'nan','Serial_Number'] = np.nan
orderoutside.loc[orderoutside['Order_Type'] == 'nan','Order_Type'] = np.nan
orderoutside.loc[orderoutside['Nation_Code'] == 'nan','Nation_Code'] = np.nan
# orderoutside.dtypes

print('===== handling order_achievement data =====')
order_achievement['Serial_Number'] = order_achievement['Serial_Number'].astype(str).str.replace('\.0','')
order_achievement['Export_Type'] = order_achievement['Export_Type'].astype(str).str.replace('\.0','')
order_achievement['Order_No'] = order_achievement['Order_No'].astype(str).str.replace('\.0','')
order_achievement['Product_No'] = order_achievement['Product_No'].astype(str).str.replace('\.0','')
order_achievement['Handle_No'] = order_achievement['Handle_No'].astype(str).str.replace('\.0','')
order_achievement['CookRoom_No'] = order_achievement['CookRoom_No'].astype(str).str.replace('\.0','')
order_achievement['CanHurry_Flag'] = order_achievement['CanHurry_Flag'].astype(bool)
order_achievement['CanLock_Flag'] = order_achievement['CanLock_Flag'].astype(bool)
order_achievement['Estimate_Time'] = pd.to_datetime(order_achievement['Estimate_Time'])

order_achievement.loc[order_achievement['Serial_Number'] == 'nan','Serial_Number'] = np.nan
order_achievement.loc[order_achievement['Export_Type'] == 'nan','Export_Type'] = np.nan
order_achievement.loc[order_achievement['Order_No'] == 'nan','Order_No'] = np.nan
order_achievement.loc[order_achievement['Product_No'] == 'nan','Product_No'] = np.nan
order_achievement.loc[order_achievement['Handle_No'] == 'nan','Handle_No'] = np.nan
order_achievement.loc[order_achievement['CookRoom_No'] == 'nan','CookRoom_No'] = np.nan
# order_achievement.dtypes

print('===== handling linein data =====')
linein['ID_NUMBER'] = linein['ID_NUMBER'].astype(str).str.replace('\.0','')
linein['FG_STATUS'] = linein['FG_STATUS'].astype(str)
linein['FG_FILM'] = linein['FG_FILM'].astype(str)

linein.loc[linein['ID_NUMBER'] == 'nan','ID_NUMBER'] = np.nan
# linein.dtypes

print('===== handling 產品分類 =====')
prob_table['產品分類'] = prob_table['產品分類'].astype(str)
prob_table['產品代號'] = prob_table['產品代號'].astype(str)

print('===== handling error data =====')
# 訂單編號出現在queue但不出現在orderinside
error1_table = queue[queue['Serial_Number'].notnull()]
error1_table = error1_table[~error1_table['Serial_Number'].isin(orderinside['Serial_Number'].unique())]
error1_S_idx = error1_table['Serial_Number'].unique().tolist()

# 訂單編號出現在orderinside但不出現在queue
error2_table = orderinside[(orderinside['Serial_Number'].notnull()) & (orderinside['Queue_Time'].notnull())]
error2_table = error2_table[~error2_table['Serial_Number'].isin(queue['Serial_Number'].unique())]
######### 應該屬於沒有預點的情況，要保留

# 訂單編號出現在queue與orderinside但沒有排隊時間
error3_table = queue[queue['Serial_Number'].notnull()]
error3_table = orderinside[orderinside['Serial_Number'].isin(error3_table['Serial_Number'].unique())]
error3_table = error3_table[error3_table['Queue_Time'].isnull()]
error3_S_idx = error3_table['Serial_Number'].unique().tolist()

# Reserve_No 保留前置字母為M、N的資料，排掉A
error4_table = queue[queue['Reserve_No'].str.slice(0,1) == 'A']
error4_S_idx = error4_table['Serial_Number'].tolist()

# 正常狀態應該會有4碼，若異常筆數不多的話建議可以直接整個刪掉
error5_table = queue[~queue['Serial_Number'].isin(error4_S_idx)]
error5_table = error5_table[error5_table['Queue_No'].str.len() != 4]

error6_table = orderinside[~orderinside['Serial_Number'].isin(error4_S_idx)]
error6_table = error6_table[(error6_table['Queue_No'].str.len() != 4) & error6_table['Queue_Time'].notnull()]
######### 發現通常Queue_No不足4碼，其實都是"A" 系統測試訂單

# 產品分類問題
excluding_prod_id = prob_table[prob_table['產品分類'].isin(['41','42','43','44','53'])]['產品代號'].tolist()
error7_table = order_achievement[order_achievement['Product_No'].isin(excluding_prod_id)]
error7_S_idx = error7_table['Serial_Number'].unique().tolist()

# Adult_Count、Kid_Count 情況為NA值(如下)
del_S_idx = error1_S_idx + error3_S_idx + error4_S_idx + error7_S_idx
error8_table = queue[~queue['Serial_Number'].isin(del_S_idx)]
error8_table['Total_Count'] = error8_table['Adult_Count'].fillna(0) + error8_table['Kid_Count'].fillna(0)
error8_table = error8_table[error8_table['Total_Count'] == 0]


error9_table = orderinside[~orderinside['Serial_Number'].isin(del_S_idx)]
error9_table['Total_Count'] = error9_table['Adult_Count'].fillna(0) + error9_table['Kid_Count'].fillna(0)
error9_table = error9_table[error9_table['Total_Count'] == 0]
# error9_table[error9_table['Queue_Time'].notnull()]
#########  這個需要問一下

AA = error9_table[['Serial_Number','Total_Count']].merge(order_achievement[['Serial_Number','Product_No']],how = 'left',on = 'Serial_Number')
order_achievement[order_achievement['Serial_Number'] == '201712311450']

# 排除等候時間大於180分鐘
queue_time = (orderinside['Enter_Time'] - orderinside['Queue_Time']).dt.seconds/60
error10_S_idx = orderinside[queue_time>180]['Serial_Number'].unique().tolist()

# 排除用餐時間大於180分鐘
meal_time = (orderinside['WalkOut_Time'] - orderinside['Enter_Time']).dt.seconds/60
error11_S_idx = orderinside[meal_time>180]['Serial_Number'].unique().tolist()

# 成人數與小孩數任一個超過20位
error12_S_idx = orderinside[(orderinside['Adult_Count']>20)|(orderinside['Kid_Count']>20)]['Serial_Number'].unique().tolist()

#########  這些為有入店註記，但入店時間為NULL，則會排除。
linein = linein[~((linein['FG_STATUS'] == 'I') & (linein['DT_INTIME'].isnull()))]

######### 異常桌號
seat_table = pd.read_excel('./data/產品分類.xlsx',sheet_name='座位分佈說明')
seat_table['桌號'] = seat_table['桌號'].astype(str)
orderinside[~orderinside['Table_No'].isin(seat_table['桌號'])]['Table_No'].unique().tolist()

print('===== filter data =====')
del_S_idx = error1_S_idx + error3_S_idx + error4_S_idx + error7_S_idx + error10_S_idx + error11_S_idx 
F_queue = queue[~queue['Serial_Number'].isin(del_S_idx)]
F_orderinside = orderinside[~orderinside['Serial_Number'].isin(del_S_idx)]
F_orderoutside = orderoutside[~queue['Serial_Number'].isin(del_S_idx)]
F_order_achievement = order_achievement[~order_achievement['Serial_Number'].isin(del_S_idx)]
F_linein = linein

print('===== generate feature =====')
print('***** orderoutside *****')
F_orderoutside['Final_Time'] = F_orderoutside[['Final_Dish_Time','Edit_Time']].apply(lambda x : np.nanmax(x), axis = 1)
F_orderoutside.loc[F_orderoutside['Final_Dish_Time'].isnull(),'Final_Time'] = F_orderoutside.loc[F_orderoutside['Final_Dish_Time'].isnull(),'Edit_Time']

Final_Time_info = {'欄位名稱':'Final_Time','欄位說明':'最後時間','註':"max(['Final_Dish_Time','Edit_Time'])"}
orderoutside_info = pd.concat([orderoutside_info, pd.DataFrame(Final_Time_info,index = [0])], ignore_index = False)
print('***** orderinside *****')
F_orderinside['Total_Count'] = F_orderinside['Adult_Count'].fillna(0) + F_orderinside['Kid_Count'].fillna(0)
F_orderinside['Wait_Time'] = (F_orderinside['Enter_Time'] - F_orderinside['Queue_Time']).dt.seconds/60
F_orderinside['Meal_Time'] = (F_orderinside['WalkOut_Time'] - F_orderinside['Enter_Time']).dt.seconds/60

Total_Count_info = [{'欄位名稱':'Total_Count','欄位說明':'總人數','註':None},
                    {'欄位名稱':'Wait_Time','欄位說明':'等候時間','註':None},
                    {'欄位名稱':'Meal_Time','欄位說明':'用餐時間','註':None}]
orderinside_info = pd.concat([orderinside_info, pd.DataFrame(Total_Count_info)], ignore_index = False)

print('***** base *****')
F_base = F_orderinside[['Serial_Number','Queue_Time','Adult_Count','Kid_Count','Total_Count','Wait_Time','Meal_Time']]
F_base = F_base[F_base['Queue_Time'].notnull()]
Final_Time_info = [{'欄位名稱':'Serial_Number','欄位說明':'訂單編號','註':""},\
                   {'欄位名稱':'Queue_Time','欄位說明':'排隊時間','註':None},\
                   {'欄位名稱':'Adult_Count','欄位說明':'大人','註':None},\
                   {'欄位名稱':'Kid_Count','欄位說明':'小人','註':None},\
                   {'欄位名稱':'Total_Count','欄位說明':'總人數','註':None},\
                   {'欄位名稱':'Wait_Time','欄位說明':'等候時間','註':None},
                   {'欄位名稱':'Meal_Time','欄位說明':'用餐時間','註':None}]
base_info = pd.DataFrame(Final_Time_info)

print('***** Other *****')
del_idx_table = pd.DataFrame({'異常被篩除Serial_Number':list(set(del_S_idx))})

print('===== summary for data =====')
def descriptive_statistics(col):
    # col = orderoutside['First_Order_Time']
    summary_cols = ['variable_type','min','max','mean','total_count','missing_count','per_of_missing','n_unique','sample']
    summary_info = dict(list(zip(summary_cols,[np.nan] * len(summary_cols))))

    if col.dtype == np.object:
        summary_info['variable_type'] = 'string'
        summary_info['total_count'] = len(col)
        summary_info['missing_count'] = np.sum(col.isnull())
        summary_info['per_of_missing'] = np.round(summary_info['missing_count']/summary_info['total_count'],4)
        summary_info['n_unique'] = col.nunique()

        if summary_info['n_unique'] >= 20:
            summary_info['sample'] = col.unique()[:20]
        else:
            summary_info['sample'] = dict(col.value_counts())
    elif (col.dtype == np.int64)|(col.dtype == np.float64):
        if col.dtype == np.int64:
            summary_info['variable_type'] = 'int'
        else:
            summary_info['variable_type'] = 'float'
        summary_info['min'] = col.min()
        summary_info['max'] = col.max()
        summary_info['mean'] = col.mean()
        summary_info['missing_count'] = np.sum(col.isnull())
        summary_info['total_count'] = len(col)
        summary_info['per_of_missing'] = np.round(summary_info['missing_count']/summary_info['total_count'],4)
    elif col.dtype ==  np.dtype('<M8[ns]'):
        summary_info['variable_type'] = 'datetime'
        summary_info['min'] = col.min()
        summary_info['max'] = col.max()
        summary_info['mean'] = col.mean()
        summary_info['total_count'] = len(col)
        summary_info['missing_count'] = np.sum(col.isnull())
        summary_info['per_of_missing'] = np.round(summary_info['missing_count']/summary_info['total_count'],4)
    elif col.dtype ==  np.bool:
        summary_info['variable_type'] = 'bool'
        summary_info['min'] = col.min()
        summary_info['max'] = col.max()
        summary_info['mean'] = col.mean()
        summary_info['missing_count'] = np.sum(col.isnull())
        summary_info['total_count'] = len(col)
        summary_info['per_of_missing'] = np.round(summary_info['missing_count']/summary_info['total_count'],4)
    return summary_info

queue_summary_table = F_queue.apply(descriptive_statistics, axis = 0, result_type='expand').T
orderoutside_summary_table = F_orderoutside.apply(descriptive_statistics, axis = 0, result_type='expand').T
orderinside_summary_table = F_orderinside.apply(descriptive_statistics, axis = 0, result_type='expand').T
order_achievement_summary_table = F_order_achievement.apply(descriptive_statistics, axis = 0, result_type='expand').T
linein_summary_table = F_linein.apply(descriptive_statistics, axis = 0, result_type='expand').T
base_summary_table = F_base.apply(descriptive_statistics, axis = 0, result_type='expand').T

queue_summary_table = queue_info.merge(queue_summary_table, how = 'left',left_on = '欄位名稱', right_index=True)
orderoutside_summary_table = orderoutside_info.merge(orderoutside_summary_table, how = 'left',left_on = '欄位名稱', right_index=True)
orderinside_summary_table = orderinside_info.merge(orderinside_summary_table, how = 'left',left_on = '欄位名稱', right_index=True)
order_achievement_summary_table = order_achievement_info.merge(order_achievement_summary_table, how = 'left',left_on = '欄位名稱', right_index=True)
linein_summary_table = linein_info.merge(linein_summary_table, how = 'left',left_on = '欄位名稱', right_index=True)
base_summary_table = base_info.merge(base_summary_table, how = 'left',left_on = '欄位名稱', right_index=True)

queue_summary_table.to_excel('./data_population/summary_for_queue.xlsx',index = False)
orderoutside_summary_table.to_excel('./data_population/summary_for_orderoutside.xlsx',index = False)
orderinside_summary_table.to_excel('./data_population/summary_for_orderinside.xlsx',index = False)
order_achievement_summary_table.to_excel('./data_population/summary_for_order_achievement.xlsx',index = False)
linein_summary_table.to_excel('./data_population/summary_for_linein.xlsx',index = False)
base_summary_table.to_excel('./data_population/summary_for_base.xlsx',index = False)

print('===== saving data =====')
F_queue.to_parquet('./data_population/queue.parquet', engine='fastparquet',index = False)
F_orderinside.to_parquet('./data_population/orderinside.parquet', engine='fastparquet',index = False)
F_orderoutside.to_parquet('./data_population/orderoutside.parquet', engine='fastparquet',index = False)
F_order_achievement.to_parquet('./data_population/order_achievement.parquet', engine='fastparquet',index = False)
F_linein.to_parquet('./data_population/linein.parquet', engine='fastparquet',index = False)
F_base.to_parquet('./data_population/base.parquet', engine='fastparquet',index = False)
del_idx_table.to_excel('./data_population/被刪除的Serial_Number.xlsx',index = False)

F_queue.to_csv('./data_population/queue.csv', index = False)
F_orderinside.to_csv('./data_population/orderinside.csv', index = False)
F_orderoutside.to_csv('./data_population/orderoutside.csv', index = False)
F_order_achievement.to_csv('./data_population/order_achievement.csv', index = False)
F_linein.to_csv('./data_population/linein.csv', index = False)
F_base.to_csv('./data_population/base.csv', index = False)