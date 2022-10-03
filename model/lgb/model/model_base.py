class Model:
    def __init__(self, model_name, config):
        self.model_name = model_name
        self.config = config

    def train(self):
        print("training model...")