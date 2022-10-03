# from ray import tune
# import torch
# from pytorch_tabnet.augmentations import RegressionSMOTE,ClassificationSMOTE
# from evaluation import Accuracy5, Accuracy10, Accuracy15,MAE

# tabnetregressor_config = {
#     'model_conf':{
#        # 'n_steps': tune.choice([3, 5, 7, 10])
#     },
#     'fit_conf':{
#         'eval_metric':[Accuracy5, Accuracy10, Accuracy15,MAE],
#         'max_epochs':100,
#         'patience':10,
#         'batch_size':1024,
#         'virtual_batch_size':128,
#         'num_workers':0,
#         'drop_last':False,
#         'loss_fn': torch.nn.functional.mse_loss,
#         'augmentations':RegressionSMOTE(p=0.2)
#     },
#     'training_conf':{
#     }
# }

# tabNetclassifier_config = {
#     'model_conf':{
#        # 'n_steps': tune.choice([3, 5, 7, 10])
#     },
#     'fit_conf':{
#         'eval_metric':["accuracy"],
#         'max_epochs':100,
#         'patience':10,
#         'batch_size':1024,
#         'virtual_batch_size':128,
#         'num_workers':0,
#         'drop_last':False,
#         'augmentations':ClassificationSMOTE(p=0.2)
#     },
#     'training_conf':{
#     }
# }
xgb_config = {
    'model_conf':{
       # 'n_steps': tune.choice([3, 5, 7, 10])
    },
    'fit_conf':{

    },
    'training_conf':{
    }
}
