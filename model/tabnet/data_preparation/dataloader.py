import numpy as np 
import pandas as pd 
from functools import partial
from sklearn.model_selection import KFold, train_test_split, StratifiedKFold
from sklearn.ensemble import IsolationForest

class DataLoader(object):
    def __init__(self, 
                 k_fold = 1, 
                 split_size = 0.3, 
                 stratified_split_date = None,
                 valid_split_date = None,
                 test_split_date = None,
                 shuffle = True,
                 random_state = None):
        
        if split_size is not None:
            assert split_size > 0, "split_size must be greater than 0"
            if split_size >= 1:
                split_size = int(split_size)
        
        self.stratified_split_date = stratified_split_date
        if stratified_split_date is not None:
            self.stratified_split_date = pd.Timestamp(stratified_split_date)

        self.valid_split_date = valid_split_date
        if valid_split_date is not None:
            self.valid_split_date = pd.Timestamp(valid_split_date)

        self.test_split_date = test_split_date
        if test_split_date is not None:
            self.test_split_date = pd.Timestamp(test_split_date)
        
        assert k_fold is not None, "k_fold must be greater then 0 and integer"
        self.k_fold = k_fold
        self.split_method = None
        if (k_fold == 1) and (valid_split_date is None): 
            self.split_method = 'train_test_split'
            self.split_fn = partial(train_test_split, 
                                    test_size = split_size, 
                                    shuffle = shuffle,
                                    random_state = random_state)
        
        if (k_fold == 1) and (valid_split_date is not None): 
            self.split_method = 'train_test_split_for_time'

        elif (k_fold > 1) and (stratified_split_date is not None):
            self.split_method = 'stratified_fkold'
            self.split_fn = StratifiedKFold(k_fold,
                                            shuffle = shuffle,
                                            random_state = random_state)
        elif (k_fold > 1) and (stratified_split_date is None):
            self.split_method = 'fkold'
            self.split_fn = KFold(k_fold,
                                  shuffle = shuffle,
                                  random_state = random_state)
        else:
            ValueError("We do not find the method of split.")

    def transform(self, df, index_col, date_col, target_col, feature_col = None, exculding_cols = None):
        
        # check dtype of important columns
        dtype_table = df.dtypes
        assert dtype_table[index_col] == np.dtype('object'), 'Please {} must convert into object '.format(index_col)
        assert dtype_table[date_col] == np.dtype('datetime64[ns]'), 'Please {} must convert into datetime64[ns] '.format(date_col)
        # assert dtype_table[target_col] in [np.dtype('float64')], 'Please {} must convert into float64 '.format(target_col)

        # numerical verification
        assert df.shape[0] == df[index_col].nunique(),\
             'Please check the shape of df and nunique of index_col, must be the same'

        self.df = df
        self.index_col = index_col
        self.date_col = date_col
        self.target_col = target_col
        self.data_type = None
        if exculding_cols is not None:
            self.exculding_cols = [index_col, date_col, target_col] + exculding_cols
        else:
            self.exculding_cols = [index_col, date_col, target_col]

        if feature_col is not None:
            self.feature_col = feature_col
        else:
            self.feature_col = [c for  c in df.columns if c not in self.exculding_cols]
    
    def excluding_abnormal_data(self, data_type, method, use_feature , **kwargs):
        self.data_type = data_type
        self.method = method
        self.use_feature = use_feature
        self.kwargs = kwargs
        if data_type == 'all':
            self.df = self._abnormal_detection(self.df, self.method, self.use_feature, **self.kwargs)
            return self.df 
    
    @staticmethod
    def _abnormal_detection(df, method, use_feature, **kwargs):
        if  method == 'rule_based':
            assert 'Z03_WAITTIME_cnt' in df.columns
            assert 'Wait_Time' in df.columns
            assert 'Wait_Time_threshold' in kwargs.keys(), 'Please, importing Wait_Time_threshold'

            df = df[~((df['Z03_WAITTIME_cnt'] == 0)&(df['Wait_Time'] > kwargs['Wait_Time_threshold']))]
        elif method == 'isolation_forest':
            assert isinstance(use_feature, list)
            clf = IsolationForest(**kwargs)
            clf.fit(df[use_feature])
            labels = clf.predict(df[use_feature])

            df = df[labels == 1]
        else:
            raise ValueError

        return df
    
    def all_data(self):
        target_y = self.df[self.target_col]
        return 0, (self.df['Serial_Number'], self.df['Queue_Time'], self.df[self.feature_col], target_y)

    def training_data(self):
        # df = pd.read_parquet('./data_feature/F_base_part_01.parquet')
        # index_col, date_col, target_col = 'Serial_Number', 'Queue_Time', 'Diff_Queue_Enter'

        if self.test_split_date is not None:
            input_df = self.df[self.df[self.date_col] < self.test_split_date].copy() 
        else:
            input_df = self.df.copy()                      
        
        group_flag = None
        data_id = np.array(input_df[self.index_col])

        split_dataset_set = []
        if self.split_method == 'train_test_split':
            train_id, valid_id = self.split_fn(data_id)
            split_dataset_set.append((0, train_id, valid_id))
        
        elif self.split_method  == 'train_test_split_for_time':
            time_series = input_df[self.date_col]
            train_id = data_id[time_series<self.valid_split_date]
            valid_id = data_id[time_series >= self.valid_split_date]
            split_dataset_set.append((0, train_id, valid_id))

        elif self.split_method in ['stratified_fkold','fkold']:
            if self.split_method == 'stratified_fkold':
                group_flag = input_df[self.date_col] >= self.stratified_split_date
                split_set = self.split_fn.split(data_id, group_flag)
            else:
                split_set = self.split_fn.split(data_id)
            
            for i, (train_index, valid_index) in enumerate(split_set):
                train_id, valid_id = data_id[train_index], data_id[valid_index]
                split_dataset_set.append((i, train_id, valid_id))

        return self.exporting_data(input_df, split_dataset_set)
    
    def testing_data(self):
        
        if self.test_split_date is not None:
            test_df = self.df[self.df[self.date_col] >= self.test_split_date].copy() 
        else:
            return None       

        test_y = test_df[self.target_col]
        test_x = test_df#.drop(columns = [self.index_col, self.date_col, self.target_col])

        return 0, (test_df['Serial_Number'], test_x[self.feature_col], test_y)

    def exporting_data(self, df, split_set):
        for i, train_id, valid_id in split_set:
            
            train_df = df[df[self.index_col].isin(train_id)]
            if self.data_type == 'train_dataset':
                train_df= self._abnormal_detection(train_df, self.method, self.use_feature, **self.kwargs)

            valid_df = df[df[self.index_col].isin(valid_id)]
            print(valid_df[self.date_col].min(), valid_df[self.date_col].max())
            train_y = train_df[self.target_col]
            valid_y = valid_df[self.target_col]

            train_x = train_df
            valid_x = valid_df

            yield i, (train_x[self.feature_col], train_y), (valid_x[self.feature_col], valid_y)
    
    def num_training_dataset(self):
        return self.k_fold
        
if __name__ == '__main__':
    import os 
    # os.chdir('/Volumes/user_space/Cloud/Side_Project/AIGO-Predict_waiting')
    os.chdir('\\\\cloudsys\\user_space\\Cloud\\Side_Project\\AIGO-Predict_waiting\\')
    from  visualization import * 
    import plotly.io as pio
    pio.renderers.default = 'notebook_connected'

    df = pd.read_parquet('./data_feature/F_base_all_v3.parquet')

    selected_col = [c for c in D3.columns if c.startswith('Z03') or c == 'Serial_Number']
    df = df.merge(D3[selected_col], how = 'left', on = 'Serial_Number')

    selected_col = [c for c in D4.columns if c.startswith('Z03') or c == 'Serial_Number']
    df = df.merge(D4[selected_col], how = 'left', on = 'Serial_Number')
    
    excluding_feature = ['Serial_Number','Queue_Time','Wait_Time','Meal_Time']
    features = [c for c in df.columns if c not in excluding_feature]
    dataloader = DataLoader(k_fold = 1, split_size = 0.1, valid_split_date = '2017-07-01 00:00:00', test_split_date = '2017-10-01 00:00:00')
    dataloader.transform(df, index_col = 'Serial_Number', 
                   date_col = 'Queue_Time', 
                   target_col = 'Wait_Time',
                   feature_col = features,
                   exculding_cols = excluding_feature)
    ddf = dataloader.excluding_abnormal_data(data_type = 'all', method = 'rule_based', use_feature = None, Wait_Time_threshold = 20 )
    ddf = dataloader.excluding_abnormal_data(data_type = 'all', method = 'isolation_forest',
                                             use_feature = ['Wait_Time','Z03_WAITTIME_group_size','Z03_WAITTIME_cnt'], 
                                             contamination = 0.01 )
    scatter_plot(df, x = 'Z03_WAITTIME_group_size', y = 'Wait_Time')
    scatter_plot(ddf, x = 'Z03_WAITTIME_group_size', y = 'Wait_Time')
    
    train_dataset = dataloader.training_data()
    


