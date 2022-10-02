import os
import numpy as np 
import pandas as pd
from tqdm import tqdm, trange
from sklearn.utils import shuffle
from sklearn.preprocessing import MinMaxScaler
from sklearn.preprocessing import StandardScaler
import warnings
warnings.filterwarnings("ignore")


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

def percentile_binner(data, input_cols, numerical_cols, categorical_cols,binner_size):
    
    quantile_list = list(np.arange(0, 1, binner_size)) + [1]
    quantile_table = data[input_cols].quantile(quantile_list)

    add_columns = []
    for col in tqdm(input_cols):
        if col+ '_{}cut'.format(binner_size) not in data.columns:
            quantile_list = quantile_table[col].tolist()
            binner_list = sorted(pd.Series(quantile_list[1:-1]).unique().tolist())
            binner_list = [quantile_list[0] - 0.00001] + binner_list
            binner_list =  binner_list + [quantile_list[-1] + 0.00001]

            if (len(binner_list) > 2) and (len(data[col].unique())>2):
                biner_cnt = len(binner_list) - 1
                biner_labels = [ 'a' + str(i) for i in range(biner_cnt)]
                add_columns.append(col+ '_{}cut'.format(binner_size))
                data[col+ '_{}cut'.format(binner_size)] = pd.cut(data[col], binner_list, labels = biner_labels)
    
    categorical_cols = categorical_cols + add_columns
    
    return data, categorical_cols, numerical_cols

def target_encoding(data, 
                    df_train,
                    input_cols,
                    target_col, 
                    categorical_cols, 
                    numerical_cols): 
    assert target_col in data.columns
    assert target_col in df_train.columns
    key_col  = data['Serial_Number']
    data.index = key_col
    add_columns = []
    for col in tqdm(input_cols):
        basic_data = data[[col]]
        TE_df = df_train.groupby(col).agg({target_col:[np.mean, np.max, np.min, np.median]})
        TE_df.columns = [col + '_' + i + '_' + j for (i,j) in TE_df.columns]
        add_cols = TE_df.columns.tolist()
        add_columns.extend(add_cols)
        
        TE_df = TE_df.reset_index()
        # data = data.merge(TE_df,how = 'left',on = col)
        basic_data = basic_data.merge(TE_df,how = 'left',on = col)
        basic_data.index = key_col
        data = data.merge(basic_data[add_cols],how = 'left',left_index = True, right_index=True)
    
    data = data.reset_index(drop=True)
    numerical_cols  = numerical_cols + add_columns
    
    return data, categorical_cols, numerical_cols

def cross_target_encoding(data, 
                          df_train,
                          groupby_cols,
                          target_col, 
                          categorical_cols, 
                          numerical_cols): 
    assert target_col in data.columns
    assert target_col in df_train.columns
    key_col  = data['Serial_Number']
    data.index = key_col
    add_columns = []
    for idx,groupby_col in enumerate(tqdm(groupby_cols)):
        basic_data = data[groupby_col]
        # idx,groupby_col = 0, groupby_cols[0]
        TE_df = df_train.groupby(groupby_col).agg({target_col:[np.mean, np.max, np.min, np.median]})
        TE_df.columns = ['combin_'+ str(idx) + '_' + i + '_' + j for (i,j) in TE_df.columns]
        add_cols = TE_df.columns.tolist()
        add_columns.extend(TE_df.columns.tolist())
        
        TE_df = TE_df.reset_index()
        basic_data = basic_data.merge(TE_df,how = 'left',on = groupby_col)
        basic_data.index = key_col
        # data = data.merge(TE_df,how = 'left',on = groupby_col)
        data = data.merge(basic_data[add_cols],how = 'left',left_index = True, right_index=True)
    
    numerical_cols  = numerical_cols + add_columns
    
    return data, categorical_cols, numerical_cols

def spliting_training_data(data,data_split_by_time):
    df_train = None
    if data_split_by_time is None:
        ratio = 0.3
        df_train = shuffle(data)[:int(len(data)*ratio)]
    else:
        df_train = data[data['Queue_Time'] < data_split_by_time]

    return df_train

def dataprocess(data, excluding_feature, data_split_by_time = None):
    
    print('##### get categorical_features and  numerical_features #####')
    data_info = pd.DataFrame(data.dtypes).reset_index().rename(columns = {'index':'feature_name', 0:'feature_dtype'})
    data_info = data_info[~data_info['feature_name'].isin(excluding_feature)]

    categorical_features_table = data_info[data_info['feature_dtype'] == np.object]
    numerical_features_table = data_info[data_info['feature_dtype'].isin([np.dtype('int64'),np.dtype('float64')])]
    
    categorical_cols = categorical_features_table['feature_name'].tolist()
    numerical_cols = numerical_features_table['feature_name'].tolist()
    raw_categorical_cols = categorical_cols
    raw_numerical_cols = numerical_cols
    assert data_info.shape[0] == (len(categorical_cols) + len(numerical_cols))
    
    print('##### convert all numerical_features to categorical features #####')
    
    data,categorical_cols,numerical_cols = percentile_binner(data, 
                                                             input_cols = numerical_cols,
                                                             numerical_cols = numerical_cols, 
                                                             categorical_cols = categorical_cols, 
                                                             binner_size = 0.25)

    print('===== split train and test data  ======')
    df_train = spliting_training_data(data,data_split_by_time)
    print('***** data = {} *****'.format(data.shape))
    
    print('##### target encoding for single col #####')
    data,categorical_cols,numerical_cols = target_encoding(data, 
                                                           df_train,
                                                           input_cols = categorical_cols,
                                                           target_col = 'Wait_Time', 
                                                           categorical_cols = categorical_cols, 
                                                           numerical_cols = numerical_cols)
    print('***** data = {} *****'.format(data.shape))
    
    print('##### target encoding for combinations  #####')
    
    combination_col = [(['Z03_WAITTIME_cnt','B111_DIFF_QUEUETIME'], 0.2)]
    input_binner = {}
    groupby_cols = []
    for cols, binner in combination_col:
        if binner in input_binner.keys():
            input_binner[binner] = input_binner[binner] + cols
        else:
            input_binner[binner] = cols
        groupby_cols.append([ c + '_{}cut'.format(binner) for c in cols])

    binners = list(input_binner.keys())
    combin_cols = list(input_binner.values())
    for input_cols, binner in zip(combin_cols,binners):
        data,categorical_cols,numerical_cols = percentile_binner(data, 
                                                             input_cols = input_cols,
                                                             numerical_cols = numerical_cols, 
                                                             categorical_cols = categorical_cols, 
                                                             binner_size = binner)

    print('===== split train and test data  ======')
    df_train = spliting_training_data(data,data_split_by_time)

    data,categorical_cols,numerical_cols = cross_target_encoding(data, 
                                                                 df_train,
                                                                 groupby_cols = groupby_cols,
                                                                 target_col = 'Wait_Time', 
                                                                 categorical_cols = categorical_cols, 
                                                                 numerical_cols = numerical_cols)
    print('***** data = {} *****'.format(data.shape))

    print('##### saving dataset  #####')
    data.to_parquet('../dataset/dataset_0930_1024.parquet', engine='fastparquet',index = False)

if __name__ == '__main__':
    import pandas as pd 
    data = pd.read_parquet('../data_feature/F_base_all_v5.parquet', engine='fastparquet')
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
    dataprocess(data, excluding_feature, data_split_by_time =data_split_by_time)


    # data  = pd.read_parquet('../dataset/dataset_0930_1024.parquet', engine='fastparquet')
    # data_summary = data.apply(descriptive_statistics, axis = 0, result_type='expand').T
    # data_summary.to_excel('../dataset/summary_for_data.xlsx')