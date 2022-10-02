import numpy as np 
from pytorch_tabnet.metrics import Metric
from sklearn.metrics import (
    roc_auc_score,
    mean_squared_error,
    mean_absolute_error,
    accuracy_score,
    log_loss,
    balanced_accuracy_score,
    mean_squared_log_error,
)

class MAE(Metric):
    def __init__(self):
        self._name = "MAE"
        self._maximize = False

    def __call__(self, y_true, y_score):
        #y_true = np.expm1(np.array(y_true)[:,0])
        #y_score = np.expm1(np.array(y_score)[:,0])
        y_true = np.array(y_true)[:,0]
        y_score = np.array(y_score)[:,0]
        return mean_absolute_error(y_true, y_score)

class Accuracy5(Metric):
    def __init__(self):
        self._name = "Accuracy5"
        self._maximize = True
        self.time_range = 5

    def __call__(self, y_true, y_score):
        y_true = np.array(y_true)[:,0]
        y_score = np.array(y_score)[:,0]
        #y_true = np.expm1(np.array(y_true)[:,0])
        #y_score = np.expm1(np.array(y_score)[:,0])
        time_diff = abs(y_true - y_score)
        hitting_cnt = sum(time_diff <= self.time_range)
        return hitting_cnt/len(y_true)

class Accuracy10(Metric):
    def __init__(self):
        self._name = "Accuracy10"
        self._maximize = True
        self.time_range = 10

    def __call__(self, y_true, y_score):
        y_true = np.array(y_true)[:,0]
        y_score = np.array(y_score)[:,0]
        #y_true = np.expm1(np.array(y_true)[:,0])
        #y_score = np.expm1(np.array(y_score)[:,0])
        time_diff = abs(y_true - y_score)
        hitting_cnt = sum(time_diff <= self.time_range)
        return hitting_cnt/len(y_true)

class Accuracy15(Metric):
    def __init__(self):
        self._name = "Accuracy15"
        self._maximize = True
        self.time_range = 15

    def __call__(self, y_true, y_score):
        y_true = np.array(y_true)[:,0]
        y_score = np.array(y_score)[:,0]
        #y_true = np.expm1(np.array(y_true)[:,0])
        #y_score = np.expm1(np.array(y_score)[:,0])
        time_diff = abs(y_true - y_score)
        hitting_cnt = sum(time_diff <= self.time_range)
        return hitting_cnt/len(y_true)