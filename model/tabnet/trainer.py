import model 

class Trainer(object):
    def __init__(self, config, model_name):

        self.config = config
        self.train_model = self._get_model(model_name)()

    
    def _set_dataset(self, train_X, train_y, valid_X, valid_y):
        
        self.config['fit_conf']['X_train'] = train_X
        self.config['fit_conf']['y_train'] = train_y
        self.config['fit_conf']['eval_set'] = [(train_X, train_y),(valid_X, valid_y)]
        self.config['fit_conf']['eval_name'] = ['train', 'valid']
        if type(self.config['fit_conf']['eval_metric'][-1]) == str:
            self.config['training_conf']['scores'] = self.config['fit_conf']['eval_name'][-1] + '_' + self.config['fit_conf']['eval_metric'][-1]
        else:
            self.config['training_conf']['scores'] = self.config['fit_conf']['eval_name'][-1] + '_' + self.config['fit_conf']['eval_metric'][-1].__name__
    
    @staticmethod
    def _get_model(model_name):
        
        if hasattr(model, model_name + '_model'):
            train_model = getattr(model, model_name + '_model')
        return train_model

    def run(self, dataloader):
        self.dataloader = dataloader
        num_train_dataset = self.dataloader.num_training_dataset()
        train_dataset = self.dataloader.training_data()
        

        for i, train_set, valid_set in train_dataset:

            train_X, train_y = train_set
            valid_X, valid_y = valid_set
    
            train_X = train_X.values
            valid_X = valid_X.values
            train_y = train_y.values.reshape(-1,1)
            valid_y = valid_y.values.reshape(-1,1)
            self._set_dataset(train_X, train_y, valid_X, valid_y)
            
            result = self.training_process(self.config)

            text_output = self.predict_precess(result['best_model'])
            output = self.predict_all_precess(result['best_model'])

        return output
    
    def training_process(self,config):

        TM = self.train_model(**config['model_conf'])
        TM.fit(**config['fit_conf'])

        return {'scores':max(TM.history[config['training_conf']['scores']]),
                'best_model':TM}
    
    
    def predict_precess(self, best_model):
        test_dataset = self.dataloader.testing_data()
        
        _,(serial_number,test_X, test_y) = test_dataset
        test_X = test_X.values
        test_y =  test_y.values.reshape(-1,1)

        pred_y = best_model.predict(test_X)

        eval_metrics = self.config['fit_conf']['eval_metric']
        for eval_metric in eval_metrics:
            print(eval_metric.__name__ + ': {}'.format(eval_metric()(pred_y,test_y)))

        return (serial_number, test_y, pred_y)

    def predict_all_precess(self, best_model):
        dataset = self.dataloader.all_data()
        _,(serial_number,Queue_Time,data_X, data_y) = dataset
        data_X = data_X.values
        data_y =  data_y.values.reshape(-1,1)

        pred_y = best_model.predict(data_X)

        return (serial_number, Queue_Time, data_y, pred_y)

        


