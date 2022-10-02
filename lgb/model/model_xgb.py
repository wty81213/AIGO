from math import gamma
from unittest import result
from xgboost import XGBRegressor
from model.model_base import Model
from evaluation import time_period_accuracy

class Xgb(Model):
    def __init__(self, model_name, config):
        super().__init__(model_name, config)
        self.early_stopping_rounds = 30
        self.model = XGBRegressor(
            colsample_bytree=self.config["colsample_bytree"],
            gamma=self.config["gamma"],
            learning_rate=self.config["learning_rate"],
            max_depth=self.config["max_depth"],
            min_child_weight=self.config["min_child_weight"],
            n_estimators=self.config["n_estimators"],
            reg_alpha=self.config["reg_alpha"],
            reg_lambda=self.config["reg_lambda"],
            subsample=self.config["subsample"],
            seed=self.config["seed"],
        )

    def train(self, train_x, train_y, valid_x, valid_y):
        self.model = self.model.fit(train_x, train_y, eval_set=[(valid_x, valid_y)], early_stopping_rounds=self.early_stopping_rounds)
        return self

    def prediction(self, x):
        return self.model.predict(x)

    def evaluate(self, y_test, y_predict):
        acc_5 = time_period_accuracy(y_test, y_predict, 5)
        acc_10 = time_period_accuracy(y_test, y_predict, 10)
        acc_15 = time_period_accuracy(y_test, y_predict, 15)
        return acc_5, acc_10, acc_15