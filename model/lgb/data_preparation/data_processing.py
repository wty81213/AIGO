from pyexpat import features
from statistics import median
from unicodedata import category
import numpy as np 
import pandas as pd
import os
from tqdm import tqdm
from sklearn.preprocessing import LabelEncoder
from sklearn.utils import shuffle
from sklearn.preprocessing import MinMaxScaler
from sklearn.preprocessing import StandardScaler
from utils.utils import check_correlation, get_important_features

def dummy_process(df, categorical_features):
    # object_list = [col for col in df.columns if df[col].dtype == "object"]
    # exclude_list = [col for col in df.columns if df[col].dtype == "object" and col != "Serial_Number"]
    return pd.get_dummies(df[categorical_features['feature_name']])

def target_encoding(df, df_train, categorical_features):
    # print(df.columns)
    # print(len(categorical_features))
    # corr = check_correlation(df)
    # print(corr.columns)
    # corr_list = [corr[col][0] for col in categorical_features['feature_name']]
    # print(corr_list)
    data = df.copy()
    for col in categorical_features['feature_name']:

        mean_df = df_train.groupby([col])['Wait_Time'].mean().reset_index()
        mean_df.columns = [col, f'{col}_mean']
        reshape_mean = np.array(mean_df[f'{col}_mean']).reshape(-1, 1)
        # mean_df[f'{col}_mean'] = MinMaxScaler().fit_transform(reshape_mean)
        mean_df[f'{col}_mean'] = StandardScaler().fit_transform(reshape_mean)


        mode_df = df_train.groupby([col])['Wait_Time'].apply(lambda x: x.mode()[0]).reset_index()
        mode_df.columns = [col, f'{col}_mode']
        reshape_mode = np.array(mode_df[f'{col}_mode']).reshape(-1, 1)
        # mode_df[f'{col}_mode'] = MinMaxScaler().fit_transform(reshape_mode)
        mode_df[f'{col}_mode'] = StandardScaler().fit_transform(reshape_mode)

        median_df = df_train.groupby([col])['Wait_Time'].median().reset_index()
        median_df.columns = [col, f'{col}_median']
        reshape_median = np.array(median_df[f'{col}_median']).reshape(-1, 1)
        # median_df[f'{col}_median'] = MinMaxScaler().fit_transform(reshape_median)
        median_df[f'{col}_median'] = StandardScaler().fit_transform(reshape_median)

        max_df = df_train.groupby([col])['Wait_Time'].max().reset_index()
        max_df.columns = [col, f'{col}_max']
        reshape_max = np.array(max_df[f'{col}_max']).reshape(-1, 1)
        # max_df[f'{col}_max'] = MinMaxScaler().fit_transform(reshape_max)
        max_df[f'{col}_max'] = StandardScaler().fit_transform(reshape_max)


        min_df = df_train.groupby([col])['Wait_Time'].min().reset_index()
        min_df.columns = [col, f'{col}_min']
        reshape_min = np.array(min_df[f'{col}_min']).reshape(-1, 1)
        # min_df[f'{col}_min'] = MinMaxScaler().fit_transform(reshape_min)
        min_df[f'{col}_min'] = StandardScaler().fit_transform(reshape_min)

        data = pd.merge(data, mean_df, how='left', on=[col])
        data = pd.merge(data, mode_df, how='left', on=[col])
        data = pd.merge(data, median_df, how='left', on=[col])
        data = pd.merge(data, max_df, how='left', on=[col])
        data = pd.merge(data, min_df, how='left', on=[col])

        print(col)

        data = data.drop([col], axis=1)

    return data

def target_encoding_more(df, df_train, categorical_features):
    important_features = ["Z03_WAITTIME_cnt", "Z03_WAITTIME_group_size"]

    data = df.copy()
    for col in categorical_features["feature_name"]:

        # 除了自己以外的所有columns做groupby
        # categorical_features_ = categorical_features["feature_name"][categorical_features["feature_name"] != col].tolist()
        categorical_features_ = important_features

        print(col)

        mean_df = df_train.groupby(categorical_features_)['Wait_Time'].mean().reset_index()
        mean_df.rename(columns = {'Wait_Time':f'{col}_mean'}, inplace = True)
        reshape_mean = np.array(mean_df[f'{col}_mean']).reshape(-1, 1)
        # mean_df[f'{col}_mean'] = StandardScaler().fit_transform(reshape_mean)
        mean_df[f'{col}_mean'] = MinMaxScaler().fit_transform(reshape_mean)

        mode_df = df_train.groupby(categorical_features_)['Wait_Time'].apply(lambda x: x.mode()[0]).reset_index()
        mode_df.rename(columns = {'Wait_Time':f'{col}_mode'}, inplace = True)
        reshape_mode = np.array(mode_df[f'{col}_mode']).reshape(-1, 1)
        # mode_df[f'{col}_mode'] = StandardScaler().fit_transform(reshape_mode)
        mode_df[f'{col}_mode'] = MinMaxScaler().fit_transform(reshape_mode)

        median_df = df_train.groupby(categorical_features_)['Wait_Time'].median().reset_index()
        median_df.rename(columns = {'Wait_Time': f'{col}_median'}, inplace = True)
        reshape_median = np.array(median_df[f'{col}_median']).reshape(-1, 1)
        # median_df[f'{col}_median'] = StandardScaler().fit_transform(reshape_median)
        median_df[f'{col}_median'] = MinMaxScaler().fit_transform(reshape_median)

        max_df = df_train.groupby(categorical_features_)['Wait_Time'].max().reset_index()
        max_df.rename(columns = {'Wait_Time': f'{col}_max'}, inplace = True)
        reshape_max = np.array(max_df[f'{col}_max']).reshape(-1, 1)
        # max_df[f'{col}_max'] = StandardScaler().fit_transform(reshape_max)
        max_df[f'{col}_max'] = MinMaxScaler().fit_transform(reshape_max)

        min_df = df_train.groupby(categorical_features_)['Wait_Time'].min().reset_index()
        min_df.rename(columns = {'Wait_Time': f'{col}_min'}, inplace = True)
        reshape_min = np.array(min_df[f'{col}_min']).reshape(-1, 1)
        # min_df[f'{col}_min'] = StandardScaler().fit_transform(reshape_min)
        min_df[f'{col}_min'] = MinMaxScaler().fit_transform(reshape_min)

        data = pd.merge(data, mean_df, how='left', on=categorical_features_)
        data = pd.merge(data, mode_df, how='left', on=categorical_features_)
        data = pd.merge(data, median_df, how='left', on=categorical_features_)
        data = pd.merge(data, max_df, how='left', on=categorical_features_)
        data = pd.merge(data, min_df, how='left', on=categorical_features_)

        data[f'{col}_mean'] = data[f'{col}_mean'].fillna(data[f'{col}_mean'].mean())
        data[f'{col}_mode'] = data[f'{col}_mode'].fillna(data[f'{col}_mode'].mean())
        data[f'{col}_median'] = data[f'{col}_median'].fillna(data[f'{col}_median'].mean())
        data[f'{col}_max'] = data[f'{col}_max'].fillna(data[f'{col}_max'].mean())
        data[f'{col}_min'] = data[f'{col}_min'].fillna(data[f'{col}_min'].mean())

    data = data.drop(categorical_features["feature_name"].tolist(), axis=1)

    return data

def percentile_cut_val_4(df, numerical_features):
    # DF = df[['Serial_Number']]
    data = df.copy()
    for i in numerical_features["feature_name"]:
        num1 = np.percentile(np.array(df[i].fillna(0)).tolist(), 25)
        num2 = np.percentile(np.array(df[i].fillna(0)).tolist(), 50)
        num3 = np.percentile(np.array(df[i].fillna(0)).tolist(), 75)
        tmp_column = data.columns.tolist()
        # print(tmp_column)
        # os._exit(0)
        data = pd.concat([data,pd.DataFrame(np.where(df[i] <= num1, 'a', np.where(df[i] <= num2, 'b', np.where(df[i] <= num3, 'c', 'd'))))], axis = 1)
        data.columns = tmp_column + [i+'_4cut']
    # data = data.drop(numerical_features["feature_name"].tolist(), axis=1)
    # print(data["Adult_Count_4cut"])
    return data



def objectType_mean_value(df, df_train, categorical_features):

    data = df.copy()

    for c in tqdm(categorical_features['feature_name']):
        # if c == "Serial_Number":
        #     continue
        mean_df = df_train.groupby([c])['Wait_Time'].mean().reset_index()
        mean_df.columns = [c, f'{c}_mean']

        data = pd.merge(data, mean_df, on=c, how='left')
        data = data.drop([c] , axis=1)
        # data[f'{c}_mean']
        data.rename(columns={f'{c}_mean': c}, inplace=True)

        reshape = np.array(data[c]).reshape(-1, 1)
        df[c] = MinMaxScaler().fit_transform(reshape)
    # data = data.drop(["Wait_Time"], axis=1)
    return data

def objectType_count_value(df, categorical_features):
    # df_temp = df.copy()
    # df_temp = df_train.copy()
    data = df.copy()

    for col in categorical_features['feature_name']:
        # if col == "Serial_Number":
        #     continue
        # count_df = data.groupby([col]).agg([('Wait_Time','size')]).reset_index()
        count_df = data[col].value_counts().reset_index()
        count_df.columns = [col, f'{col}_count']

        data = pd.merge(data, count_df, on=col, how='left')
        data = data.drop([col] , axis=1)

        data.rename(columns={f'{col}_count': col}, inplace=True)

    return data

def min_max_Scaler(df, numerical_features):
    # numerical_list = [col for col in df.columns if (df[col].dtype != "object")]

    # df[numerical_features['feature_name']] = df[numerical_features['feature_name']].fillna(df[numerical_features['feature_name']].mean())
    for col in numerical_features['feature_name']:
        df[col] = df[col].fillna(df[col].mean())
        reshape = np.array(df[col]).reshape(-1, 1)
        df[col] = MinMaxScaler().fit_transform(reshape)
    return df

def lightgbm_process(data, excluding_feature, data_split_by_time=None):

    data_info = pd.DataFrame(data.dtypes).reset_index().rename(columns = {'index':'feature_name', 0:'feature_dtype'})
    data_info = data_info[~data_info['feature_name'].isin(excluding_feature)]

    # features = [c for c in data.columns if c not in excluding_feature]
    categorical_features = data_info[data_info['feature_dtype'] == np.object]
    numerical_features = data_info[data_info['feature_dtype'].isin([np.dtype('int64'),np.dtype('float64')])]
    assert data_info.shape[0] == (len(categorical_features) + len(numerical_features))

    # convert all numerical_features to categorical features

    # data = percentile_cut_val_4(data, numerical_features)

    # get new categorical_features after percentile process
    # data_info = pd.DataFrame(data.dtypes).reset_index().rename(columns = {'index':'feature_name', 0:'feature_dtype'})
    # data_info = data_info[~data_info['feature_name'].isin(excluding_feature)]
    # categorical_features = data_info[data_info["feature_dtype"] == np.object]
    # numerical_features = data_info[data_info['feature_dtype'].isin([np.dtype('int64'),np.dtype('float64')])]

    if data_split_by_time is None:
        ratio = 0.7
        df_train = shuffle(data)[:int(len(data)*ratio)]
    else:
        df_train = data[data['Queue_Time'] < data_split_by_time]


    # category process

    # data = dummy_process(data, categorical_features)
    data = objectType_mean_value(data, df_train, categorical_features)
    # data = objectType_count_value(data, categorical_features)
    # data = target_encoding(data, df_train, categorical_features)
    # data = target_encoding_more(data, df_train, categorical_features)

    # numerical process
    data = min_max_Scaler(data, numerical_features)
    features = [c for c in data.columns if c not in excluding_feature]
    data["Wait_Time"] = np.log1p(data["Wait_Time"])

    return data, features

def tabnet_preprocess(data, excluding_feature, config):

    data_info = pd.DataFrame(data.dtypes).\
                   reset_index().\
                   rename(columns = {'index':'feature_name', 0:'feature_dtype'})
    data_info = data_info[~data_info['feature_name'].isin(excluding_feature)]

    features = [c for  c in data.columns if c not in excluding_feature]

    categorical_features = data_info[data_info['feature_dtype'] == np.object]
    numerical_features = data_info[data_info['feature_dtype'].isin([np.dtype('int64'),np.dtype('float64')])]
    assert data_info.shape[0] == (len(categorical_features) + len(numerical_features))
    
    # handling categorical columns 
    categorical_col = {}
    categorical_encoder = {}
    for col in categorical_features['feature_name']:
        missing_cnt = sum(data[col].isnull())
        if missing_cnt > 0:
            print('{} have {} missing'.format(col, missing_cnt))
            data[col] = data[col].fillna('unknow')

        cat_encoder = LabelEncoder()
        cat_encoder.fit(data[col].values)
        data[col] = cat_encoder.transform(data[col].values)
        data[col] = data[col].astype('int64')

        categorical_encoder[col] = cat_encoder
        categorical_col[col] = len(cat_encoder.classes_)

    cat_info = [(i,f) for i,f in enumerate(features) if f in categorical_features['feature_name'].tolist()]
    cat_idxs = [ i for i,_ in cat_info]
    cat_dims = [ categorical_col[j] for _,j in cat_info]

    emb_dim_biner = [0,2,4,7]
    cat_emb_dim = pd.cut(np.array(cat_dims), emb_dim_biner, labels=[1,2,5]).tolist()
    
    assert len(cat_idxs) == len(cat_dims)
    assert len(cat_dims) == len(cat_emb_dim)
    
    config['model_conf']['cat_idxs'] = cat_idxs
    config['model_conf']['cat_dims'] = cat_dims
    config['model_conf']['cat_emb_dim'] = cat_emb_dim

    # handling numerical columns 
    for col in numerical_features['feature_name']:
        missing_cnt = sum(data[col].isnull())
        if missing_cnt > 0:
            print('{} have {} missing'.format(col, missing_cnt))
            data[col] = data[col].fillna(data[col].mean())

    return data, features, config

# if __name__ == '__main__':
#     data = pd.read_parquet('./data_feature/F_base_part_01.parquet', engine='fastparquet')
#     excluding_feature = ['Serial_Number','Queue_Time','Diff_Queue_Enter']
#     config = tabnet_config

#     output = tabnet_preprocess(data, excluding_feature, config)