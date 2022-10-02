import lightgbm as lgb
from model.model_base import Model
from evaluation import time_period_accuracy

class Lgb(Model):
    def __init__(self, model_name, config):
        super().__init__(model_name, config)
        self.model = None
        self.early_stopping_rounds = 30

    def train(self, train_x, train_y, valid_x, valid_y):
        train_dataset = lgb.Dataset(train_x, label=train_y)
        valid_dataset = lgb.Dataset(valid_x, label=valid_y)

        self.model = lgb.train(self.config,
                    train_set=train_dataset,
                    valid_sets=valid_dataset,
                    early_stopping_rounds=self.early_stopping_rounds)

        return self

    def prediction(self, x):
        return self.model.predict(x, num_iteration=self.model.best_iteration)

    def evaluate(self, y_test, y_predict):
        acc_5 = time_period_accuracy(y_test, y_predict, 5)
        acc_10 = time_period_accuracy(y_test, y_predict, 10)
        acc_15 = time_period_accuracy(y_test, y_predict, 15)
        return acc_5, acc_10, acc_15
