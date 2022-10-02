import conf.training_conf as model_config

def get_model_conf(model_name):
    config = None 
    if hasattr(model_config, model_name + '_config'):
        config = getattr(model_config, model_name + '_config')
    else:
        raise ValueError('do not have the {} conf'.format(model_name))

    return config