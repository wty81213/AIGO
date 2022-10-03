from pytorch_tabnet.augmentations import RegressionSMOTE
from evaluation import Accuracy5, Accuracy10, Accuracy15

tabnet_config = {
    'model_conf':{
       # 'n_steps': tune.choice([3, 5, 7, 10])
    },
    'fit_conf':{
        'eval_metric':[Accuracy5, Accuracy15],
        'max_epochs':100,
        'patience':10,
        'batch_size':1024, 
        'virtual_batch_size':128,
        'num_workers':0,
        'drop_last':False,
        'augmentations':RegressionSMOTE(p=0.2)
    },
    'training_conf':{
    }
}

lgb_config = {
    'task': 'train',
    'boosting_type': 'gbdt',
    'objective': 'regression',
    'metric': ['l1','l2'],
    'learning_rate': 0.05,
    'feature_fraction': 0.9,
    'bagging_fraction': 0.7,
    'bagging_freq': 10,
    'verbose': 0,
    "max_depth": 6,
    "num_leaves":128,
    "max_bin": 512,
    "num_iterations": 50000
}

xgb_config = {
    "colsample_bytree": 0.4,
    "gamma": 0,
    "learning_rate": 0.07,
    "max_depth": 3,
    "min_child_weight": 1.5,
    "n_estimators": 10000,
    "reg_alpha": 0.75,
    "reg_lambda": 0.45,
    "subsample": 0.6,
    "seed": 42
}