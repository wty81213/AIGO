from calendar import c
import pandas as pd
import os
import lightgbm as lgb
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

def check_correlation(df):
    corr = df.corr()
    return corr

def get_important_features(train_x, model):
    feature_imp = pd.DataFrame(sorted(zip(model.feature_importance(), train_x.columns)), columns=['Value', 'Feature'])
    plt.figure(figsize=(20, 10))
    sns.barplot(x="Value", y="Feature", data=feature_imp.sort_values(by="Value", ascending=False)[:10])
    plt.title('LightGBM Features (avg over folds)')
    plt.tight_layout()
    plt.savefig('images/lgbm_importances.png')
    return feature_imp
    
