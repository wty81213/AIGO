from cmath import nan
import queue
from re import T, X
from tkinter import N, Y
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
import seaborn as sns
from enum_label.nation_code import Country
from collections import defaultdict
from scipy.stats import norm
from plotnine import *

def B02_total_count(df):
    df = df[["Serial_Number", "B02_totalCount", "B02_adult_count", "B02_kid_count", "B02_adult_min",
                "B02_kid_min", "B02_people_min"]]
    # draw B02_totalCount
    x_total_count = df["B02_totalCount"].value_counts().sort_index(ascending=True).keys().tolist()
    y_total_count = df["B02_totalCount"].value_counts().sort_index(ascending=True).tolist()
    plt.bar(x_total_count, y_total_count)
    plt.savefig("images/B02_total_count.png")
    plt.clf()

    # draw B02_adult_count
    x_adult_count = df["B02_adult_count"].value_counts().sort_index(ascending=True).keys().tolist()
    y_adult_count = df["B02_adult_count"].value_counts().sort_index(ascending=True).tolist()
    plt.bar(x_adult_count, y_adult_count)
    plt.savefig("images/B02_adult_count.png")
    plt.clf()

    # draw B02_kid_count
    x_kid_count = df["B02_kid_count"].value_counts().sort_index(ascending=True).keys().tolist()
    y_kid_count = df["B02_kid_count"].value_counts().sort_index(ascending=True).tolist()
    plt.bar(x_kid_count, y_kid_count)
    plt.savefig("images/B02_kid_count.png")
    plt.clf()

    # draw B02_adult_min
    x_adult_min = df["B02_adult_min"].value_counts().sort_index(ascending=True).keys().tolist()
    y_adult_min = df["B02_adult_min"].value_counts().sort_index(ascending=True).tolist()
    plt.bar(x_adult_min, y_adult_min)
    plt.savefig("images/B02_adult_min.png")
    plt.clf()

    # draw B02_kid_min
    x_kid_min = df["B02_kid_min"].value_counts().sort_index(ascending=True).keys().tolist()
    y_kid_min = df["B02_kid_min"].value_counts().sort_index(ascending=True).tolist()
    plt.bar(x_kid_min, y_kid_min)
    plt.savefig("images/B02_kid_min.png")
    plt.clf()

    # # draw B02_people_min
    x_people_min = df["B02_people_min"].value_counts().sort_index(ascending=True).keys().tolist()
    y_people_min = df["B02_people_min"].value_counts().sort_index(ascending=True).tolist()
    plt.bar(x_people_min, y_people_min)
    plt.savefig("images/B02_people_min.png")
    plt.clf()

def B098_WAITTIME_AVG_CONDITION(df):
    df_new = df["B098_WAITTIME_AVG_CONDITION"]

    max_min = df_new.max()
    bins = [i for i in range(0, int((max_min / 60 + 1)* 60), 30)]

    df_cut = pd.cut(df_new, bins, include_lowest=True, labels=["0-30", "30-60", "60-90", "90-120", "120-150", "150-180"]) #labels=["0-30", "30-60", "60-90", "90-120", "120-150", "150-180"]
    df_final = df_cut.value_counts().reset_index()
    df_final.columns = ["time_range", "count"]

    # draw by ggpot
    p = ggplot(df_final) + aes(x='time_range', y="count") + geom_col()
    print(p)

    ggsave(p,"images/B098_WAITTIME_AVG_CONDITION.jpg")

    # draw by matplotlib
    # plt.bar(df_final["time_range"], df_final["count"])
    # plt.show()

def people_by_weekDay(df):
    # print(df["A04_queue_weekday"].min())
    # print(df["A04_queue_weekday"].max())
    print(df["Queue_Time"].min())
    print(df["Queue_Time"].max())
    # print(df["Queue_Time"][0].strftime('%X'))
    # print(df["Queue_Time"])
    df['time_str'] = df["Queue_Time"].dt.strftime('%X')
    print(df['time_str'].min())
    print(df['time_str'].max())
    print(df['time_str'].dtypes)

    data_range = pd.date_range(start='09:00:00', end='22:00:00', freq='15T')
    time_range_list = data_range.strftime('%X')
    bin_time = [pd.Timestamp(t) for t in time_range_list]
    # pd.Timestamp(time_range_list[0])
    print(bin_time)

    # queue_time_cut = pd.cut(df['time_str'], time_range_list)
    # print(queue_time_cut)
    os._exit(0)


def numerical_features_with_y(df, numerical_features):
    # for col in numerical_features:
    #     sns.regplot(x=df[col][:])
    
    # x = df["Adult_Count"]
    # y = df["Wait_Time"]
    # sns.regplot(x, y)
    # plt.show()
    os._exit(0)


