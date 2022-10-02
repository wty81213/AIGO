import numpy as np 
import pandas as pd
from pytorch_tabnet.metrics import Metric

class Accuracy5(Metric):
    def __init__(self):
        self._name = "Accuracy5"
        self._maximize = True
        self.time_range = 5

    def __call__(self, y_true, y_score):
        y_true = np.array(y_true)[:,0]
        y_score = np.array(y_score)[:,0]
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
        time_diff = abs(y_true - y_score)
        hitting_cnt = sum(time_diff <= self.time_range)
        return hitting_cnt/len(y_true)

def time_period_accuracy(y_test, y_predict, time_period):
    # y_predict  = pd.DataFrame(y_predict, columns=["Wait_Time"])

    diff = (abs(np.expm1(y_test) - np.expm1(y_predict)) < time_period).value_counts().sort_index()

    diff_index = diff.index.to_list()

    return round(diff[diff_index[1]] / len(y_test), 2)