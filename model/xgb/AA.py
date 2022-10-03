import numpy as np 
import pandas as pd
from data_preparation.dataloader import DataLoader
from data_preparation.data_processing import tabnet_preprocess
from conf.configuration import get_model_conf
from pytorch_tabnet.tab_model import TabNetRegressor
from evaluation import Accuracy5, Accuracy10, Accuracy15
from pytorch_tabnet.augmentations import RegressionSMOTE
from trainer import Trainer

model_name = 'tabnet'

data = pd.read_parquet('./data_feature/F_base_part_01.parquet', engine='fastparquet')
excluding_feature = ['Serial_Number','Queue_Time','Diff_Queue_Enter']

config = get_model_conf(model_name)
data, features, config = tabnet_preprocess(data, excluding_feature, config)

dataloader = DataLoader(k_fold = 1, split_size = 0.3, valid_split_date = '2017-07-01 00:00:00', test_split_date = '2017-10-01 00:00:00')
dataloader.transform(data, index_col = 'Serial_Number', 
                   date_col = 'Queue_Time', 
                   target_col = 'Diff_Queue_Enter',
                   feature_col = features,
                   exculding_cols = excluding_feature)

num_train_dataset = dataloader.num_training_dataset()
train_dataset = dataloader.training_data()
test_dataset = dataloader.testing_data()

for i, train_set, valid_set in train_dataset:
    
    train_X, train_y = train_set
    valid_X, valid_y = valid_set
    
    train_X = train_X.values
    valid_X = valid_X.values
    train_y = train_y.values.reshape(-1,1)
    valid_y = valid_y.values.reshape(-1,1)

import ray
from ray import tune
from ray.air import session
from ray.tune.search import ConcurrencyLimiter
from ray.tune.schedulers import AsyncHyperBandScheduler
from ray.tune.search.optuna import OptunaSearch

algo = OptunaSearch()
algo = ConcurrencyLimiter(algo, max_concurrent=4)
scheduler = AsyncHyperBandScheduler()

def training_process(config):
    print(config['n_steps'])
    model = TabNetRegressor(n_steps = config['n_steps'], device_name='cpu')
    aug = RegressionSMOTE(p=0.2, device_name='cpu')
    model.fit(
        X_train=train_X, y_train=train_y,
        eval_set=[(train_X, train_y),(valid_X, valid_y)],
        eval_name=['train', 'valid'],
        eval_metric=[Accuracy5,Accuracy15],
        max_epochs=100,
        patience=10,
        batch_size=1024, virtual_batch_size=128,
        num_workers=0,
        drop_last=False,
        augmentations=aug,
        ) 
    return {'scores':max(model.history['valid_Accuracy15'])}

from ray.tune.schedulers import ASHAScheduler
scheduler = ASHAScheduler(
        max_t=10 ,
        grace_period=1,
        reduction_factor=2)

tuner = tune.Tuner(
        tune.with_resources(
            training_process, 
            resources = {"cpu": 6, "gpu": 0}),
        tune_config= tune.TuneConfig(
        metric="scores",
        mode="max",
        # search_alg = algo,
        scheduler = scheduler,
        num_samples = 8
        ),
        param_space = {
             'n_steps': tune.choice([3, 5, 7, 10])
        },
        )
results = tuner.fit()
print("Best hyperparameters found were: ", results.get_best_result().config)