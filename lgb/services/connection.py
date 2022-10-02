from cgi import print_directory
from cmath import nan
import math
from turtle import delay
from xmlrpc.client import DateTime
from joblib import Parallel, delayed, effective_n_jobs
from datetime import datetime
from db.db import DBConnection
import os
import time
import numpy as np
import pandas as pd
from math import sqrt
from tqdm import tqdm
# from services.order.googletrends_service import GoogleTrends
from .googletrends_service import GoogleTrends
from .gov_schedule_sefvice import GovSchedule
from .holiday_cn_service import HolidayCN
from .holiday_jp_service import HolidayJP
from .dtf_base import DTFBase
from .dtf_orderinside import DTFOrderInside
from datetime import timedelta
from functools import partial

def gen_even_slices(n, n_packs, *, n_samples=None):
    start = 0
    if n_packs < 1:
        raise ValueError("gen_even_slices got n_packs=%s, must be >=1" % n_packs)
    for pack_num in range(n_packs):
        this_n = n // n_packs
        if pack_num < n % n_packs:
            this_n += 1
        if this_n > 0:
            end = start + this_n
            if n_samples is not None:
                end = min(n_samples, end)
            yield slice(start, end, None)
            start = end

def generate_batch(data, n_works):

    batch_size = math.ceil(len(data) / n_works)

    if batch_size<=0:
        return
    for i in range(0, len(data), batch_size):
        yield i, i + batch_size

def parallel_process(data, n_works, proc_data_function, orderinside=None, order_achievement=None):

    batches = generate_batch(data, n_works)

    result = Parallel(n_jobs=n_works)(delayed(proc_data_function)(data[start:end], orderinside=orderinside, order_achievement=order_achievement) for start, end in batches)

    return pd.concat(result)

def parallel_apply(df, func, n_jobs=-1, **kwargs):

    batches = generate_batch(df, n_jobs)
    
    if effective_n_jobs(n_jobs) == 1:
        return df.apply(func, **kwargs)
    else:
        ret = Parallel(n_jobs=n_jobs)(
            delayed(type(df).apply)(df[start:end], func, **kwargs)
            for start, end in batches)
        return pd.concat(ret)


def queue_customer_amount(base, queue = None, orderinside = None, orderoutside = None, order_achievement = None, time_range = None):

    result = []
    columns = ["Serial_Number", "B02_totalCount", "B02_adult_count", "B02_kid_count", "B02_people_count",
                "B02_adult_max", "B02_kid_max", "B02_people_max","B02_adult_min",
                "B02_kid_min", "B02_people_min", "B02_adult_avg", "B02_kid_avg", "B02_people_avg"]

    # for i in range(len(base)):
    for queue_time, serial_number in base[["Queue_Time", "Serial_Number"]].values:

        data = []
        # serial_number = base.loc[i + start, "Serial_Number"]
        # queue_time = base.loc[i+start, "Queue_Time"]

        data.append(serial_number)

        queue_records = orderinside[(orderinside["Enter_Time"] > queue_time) & (orderinside["Queue_Time"] < queue_time)]

        adult_count, kid_count, people_count = 0, 0, 0
        adult_max, kid_max, people_max = 0, 0, 0
        adult_avg, kid_avg, people_avg = 0, 0, 0
        adult_min, kid_min, people_min = 0, 0, 0

        queue_records = queue_records[["Adult_Count", "Kid_Count"]]

        if len(queue_records) > 0:

            #calculate sum
            adult_count = int(queue_records["Adult_Count"].sum())
            kid_count = int(queue_records["Kid_Count"].sum())
            people_count = int(adult_count + kid_count)

            # calculate max
            adult_max = int(queue_records["Adult_Count"].fillna(0).max())

            kid_max = int(queue_records["Kid_Count"].fillna(0).max())

            people_max = int((queue_records["Adult_Count"] + queue_records["Kid_Count"]).fillna(0).max())

            # calculate min
            adult_min = queue_records["Adult_Count"].min()

            kid_min = queue_records["Kid_Count"].min()

            people_min = (queue_records["Adult_Count"] + queue_records["Kid_Count"]).min()

            # calculate avg
            adult_avg = round(queue_records["Adult_Count"].sum() / len(queue_records), 2)
            kid_avg = round(queue_records["Kid_Count"].sum() / len(queue_records), 2)
            people_avg = round((queue_records["Adult_Count"] + queue_records["Kid_Count"]).sum() / len(queue_records), 2)

        data.append(len(queue_records))

        data.append(adult_count)
        data.append(kid_count)
        data.append(people_count)

        data.append(adult_max)
        data.append(kid_max)
        data.append(people_max)

        data.append(adult_min)
        data.append(kid_min)
        data.append(people_min)

        data.append(adult_avg)
        data.append(kid_avg)
        data.append(people_avg)

        result.append(data)

    return pd.DataFrame(result, columns=columns)

def time_period_queue_amount(base, queue = None, orderinside = None, orderoutside = None, order_achievement = None, time_range = None):

        result = []
        columns = ["Serial_Number", "B04_five_min_queue_amount", "B04_ten_min_queue_amount", "B04_fifteen_min_queue_amount"]

        # for i in range(len(base)):
        for queue_time, serial_number in base[["Queue_Time", "Serial_Number"]].values:
            data = []
            # serial_number = base.loc[i + start, "Serial_Number"]
            # queue_time = base.loc[i + start, "Queue_Time"]
            # queue_time = base.loc[i, "Queue_Time"]
            five_min_ago = queue_time - timedelta(minutes=5)
            ten_min_ago = queue_time - timedelta(minutes=10)
            fifteen_min_ago = queue_time - timedelta(minutes=15)

            five_queue_records = orderinside[(orderinside["Queue_Time"] < queue_time) & (orderinside["Queue_Time"] > five_min_ago)]
            ten_queue_records = orderinside[(orderinside["Queue_Time"] < queue_time) & (orderinside["Queue_Time"] > ten_min_ago)]
            fifteen_queue_records = orderinside[(orderinside["Queue_Time"] < queue_time) & (orderinside["Queue_Time"] > fifteen_min_ago)]

            data.append(serial_number)
            data.append(len(five_queue_records))
            data.append(len(ten_queue_records)) 
            data.append(len(fifteen_queue_records)) # data = [5, 8, 4]

            result.append(data) # result = [[5, 8, 4]]

        return pd.DataFrame(result, columns=columns)

def time_period_need_queue(base, queue = None, orderinside = None, orderoutside = None, order_achievement = None, time_range = None):

    result = []
    columns = ["Serial_Number", "B05_five_minute_queue_status", "B05_ten_minute_queue_status", "B05_fifteen_minute_queue_status"]

    # for i in range(len(base)):
    for queue_time, serial_number in base[["Queue_Time", "Serial_Number"]].values:

        data = []

        data.append(serial_number)

        five_min_ago = queue_time - timedelta(minutes=5)
        ten_min_ago = queue_time - timedelta(minutes=10)
        fifteen_min_ago = queue_time - timedelta(minutes=15)

        five_queue_records = orderinside[((orderinside["Enter_Time"] < queue_time) & (orderinside["Enter_Time"] > five_min_ago))]["Queue_Time"]
        ten_queue_records = orderinside[((orderinside["Enter_Time"] < queue_time) & (orderinside["Enter_Time"] > ten_min_ago))]["Queue_Time"]
        fifteen_queue_records = orderinside[((orderinside["Enter_Time"] < queue_time) & (orderinside["Enter_Time"] > fifteen_min_ago))]["Queue_Time"]

        if len(five_queue_records):
            if five_queue_records.isnull().any():
                five_minute_status = "N"
            else:
                five_minute_status = "Y"
        else:
            five_minute_status = "X"

        data.append(five_minute_status)

        if len(ten_queue_records):
            if ten_queue_records.isnull().any():
                ten_minute_status = "N"
            else:
                ten_minute_status = "Y"
        else:
            ten_minute_status = "X"

        data.append(ten_minute_status)

        if len(fifteen_queue_records):
            if fifteen_queue_records.isnull().any():
                fifteen_minute_status = "N"
            else:
                fifteen_minute_status = "Y"
        else:
            fifteen_minute_status = "X"

        data.append(fifteen_minute_status)

        result.append(data)

    return pd.DataFrame(result, columns=columns)

def time_period_orderInside_amount(base, queue = None, orderinside = None, orderoutside = None, order_achievement = None, time_range = None):
    result = []
    columns = ["Serial_Number", "B06_five_min_orderInside_amount", "B06_ten_min_orderInside_amount", "B06_fifteen_min_orderInside_amount", 
              "B06_five_min_orderInside_adult_amount", "B06_five_min_orderInside_kid_amount", "B06_five_min_orderInside_people_amount",
              "B06_ten_min_orderInside_adult_amount", "B06_ten_min_orderInside_kid_amount", "B06_ten_min_orderInside_people_amount", 
              "B06_fifteen_min_orderInside_adult_amount", "B06_fiftee_min_orderInside_kid_amount", "B06_fiftee_min_orderInside_people_amount"]

    for queue_time, serial_number in base[["Queue_Time", "Serial_Number"]].values:

        data = []

        data.append(serial_number)

        five_min_ago = queue_time - timedelta(minutes=5)
        ten_min_ago = queue_time - timedelta(minutes=10)
        fifteen_min_ago = queue_time - timedelta(minutes=15)

        five_queue_records = orderinside[(orderinside["Enter_Time"] < queue_time) & (orderinside["Enter_Time"] > five_min_ago)]
        ten_queue_records = orderinside[(orderinside["Enter_Time"] < queue_time) & (orderinside["Enter_Time"] > ten_min_ago)]
        fifteen_queue_records = orderinside[(orderinside["Enter_Time"] < queue_time) & (orderinside["Enter_Time"] > fifteen_min_ago)]

        five_min_adult_amount = five_queue_records["Adult_Count"].sum()
        five_min_kid_amount = five_queue_records["Kid_Count"].sum()
        five_min_people = five_min_adult_amount + five_min_kid_amount

        ten_min_adult_amount = ten_queue_records["Adult_Count"].sum()
        ten_min_kid_amount = ten_queue_records["Kid_Count"].sum()
        ten_min_people = ten_min_adult_amount + ten_min_kid_amount

        fifteen_min_adult_amount = fifteen_queue_records["Adult_Count"].sum()
        fifteen_min_kid_amount = fifteen_queue_records["Kid_Count"].sum()
        fifteen_min_people = fifteen_min_adult_amount + fifteen_min_kid_amount

        data.append(len(five_queue_records))
        data.append(len(ten_queue_records))
        data.append(len(fifteen_queue_records))

        data.append(five_min_adult_amount)
        data.append(five_min_kid_amount)
        data.append(five_min_people)

        data.append(ten_min_adult_amount)
        data.append(ten_min_kid_amount)
        data.append(ten_min_people)

        data.append(fifteen_min_adult_amount)
        data.append(fifteen_min_kid_amount)
        data.append(fifteen_min_people)

        result.append(data)

    return pd.DataFrame(result, columns=columns)


def time_period_waiting_time(base, queue = None, orderinside = None, orderoutside = None, order_achievement = None, time_range = None):

    result = []
    columns = ["Serial_Number", "B07_five_mean_waiting_time", "B07_ten_mean_waiting_time", "B07_fifteen_mean_waiting_time", 
               "B07_five_max_waiting_time", "B07_ten_max_waiting_time", "B07_fifteen_max_waiting_time",
               "B07_five_min_waiting_time", "B07_ten_min_waiting_time", "B07_fifteen_min_waiting_time"]

    # for i in range(len(base)):
    for queue_time, serial_number in base[["Queue_Time", "Serial_Number"]].values:

        data = []

        data.append(serial_number)

        five_min_ago = queue_time - timedelta(minutes=5)

        ten_min_ago = queue_time - timedelta(minutes=10)

        fifteen_min_ago = queue_time - timedelta(minutes=15)

        # 找出五分鐘內入席的人(不管要不要排隊)
        five_queue_records = orderinside[((orderinside["Enter_Time"] < queue_time) & (orderinside["Enter_Time"] > five_min_ago))]

        # 找出十分鐘內入席的人(不管要不要排隊)
        ten_queue_records = orderinside[((orderinside["Enter_Time"] < queue_time) & (orderinside["Enter_Time"] > ten_min_ago))]

        # 找出十五分鐘內入席的人(不管要不要排隊)
        fifteen_queue_records = orderinside[((orderinside["Enter_Time"] < queue_time) & (orderinside["Enter_Time"] > fifteen_min_ago))]

        five_waiting_time_mean, ten_waiting_time_mean, fifteen_waiting_time_mean = 0, 0, 0
        five_waiting_time_max, ten_waiting_time_max, fifteen_waiting_time_max = 0, 0, 0
        five_waiting_time_min, ten_waiting_time_min, fifteen_waiting_time_min = 0, 0, 0

        if len(five_queue_records):
            # 找出要排隊排隊的
            need_queue = five_queue_records[~five_queue_records["Queue_Time"].isnull()]

            # 如果需要排隊的大於0，才需要計算平均排隊時間
            if len(need_queue):
                df_waiting_time = (need_queue["Enter_Time"] - need_queue["Queue_Time"]).dt.total_seconds() / 60
                five_waiting_time_mean = df_waiting_time.mean()
                five_waiting_time_min = df_waiting_time.min()
                five_waiting_time_max = df_waiting_time.max()

        if len(ten_queue_records):
            # 找出要排隊排隊的
            need_queue = ten_queue_records[~ten_queue_records["Queue_Time"].isnull()]

            # 如果需要排隊的大於0，才需要計算平均排隊時間
            if len(need_queue):
                df_waiting_time = (need_queue["Enter_Time"] - need_queue["Queue_Time"]).dt.total_seconds() / 60
                ten_waiting_time_mean = df_waiting_time.mean()
                ten_waiting_time_min = df_waiting_time.min()
                ten_waiting_time_max = df_waiting_time.max()

        if len(fifteen_queue_records):
            # 找出要排隊排隊的
            need_queue = fifteen_queue_records[~fifteen_queue_records["Queue_Time"].isnull()]

            # 如果需要排隊的大於0，才需要計算平均排隊時間
            if len(need_queue):
                df_waiting_time = (need_queue["Enter_Time"] - need_queue["Queue_Time"]).dt.total_seconds() / 60
                fifteen_waiting_time_mean = df_waiting_time.mean()
                fifteen_waiting_time_min = df_waiting_time.min()
                fifteen_waiting_time_max = df_waiting_time.max()


        data.append(five_waiting_time_mean)
        data.append(ten_waiting_time_mean)
        data.append(fifteen_waiting_time_mean)

        data.append(five_waiting_time_min)
        data.append(ten_waiting_time_min)
        data.append(fifteen_waiting_time_min)

        data.append(five_waiting_time_max)
        data.append(ten_waiting_time_max)
        data.append(fifteen_waiting_time_max)

        result.append(data)

    return pd.DataFrame(result, columns=columns)


def time_period_waiting_time_ratio(base, queue = None, orderinside = None, orderoutside = None, order_achievement = None, time_range = None):

    result = []

    columns = ["Serial_Number", "B08_five_minute_waiting_time_ratio", "B08_ten_minute_waiting_time_ratio", "B08_fifteen_minute_waiting_time_ratio"]

    for queue_time, serial_number in base[["Queue_Time", "Serial_Number"]].values:

        data = []

        data.append(serial_number)

        five_min_ago = queue_time - timedelta(minutes=5)

        ten_min_ago = queue_time - timedelta(minutes=10)

        fifteen_min_ago = queue_time - timedelta(minutes=15)

        # 找出五分鐘內入席的人(不管要不要排隊)
        five_queue_records_total = orderinside[((orderinside["Enter_Time"] < queue_time) & (orderinside["Enter_Time"] > five_min_ago))]

        # 找出十分鐘內入席的人(不管要不要排隊)
        ten_queue_records_total = orderinside[((orderinside["Enter_Time"] < queue_time) & (orderinside["Enter_Time"] > ten_min_ago))]

        # 找出十五分鐘內入席的人(不管要不要排隊)
        fifteen_queue_records_total = orderinside[((orderinside["Enter_Time"] < queue_time) & (orderinside["Enter_Time"] > fifteen_min_ago))]

        # 五分鐘內進場需要排隊的人
        five_queue_records = five_queue_records_total[~five_queue_records_total["Queue_Time"].isnull()]
        # orderinside[(~orderinside["Queue_Time"].isnull() & ((orderinside["Enter_Time"] > five_min_ago) & (orderinside["Enter_Time"] < queue_time)))]
        # 五分鐘內進場不須排隊的人
        five_noqueue_records = five_queue_records_total[five_queue_records_total["Queue_Time"].isnull()]
        # orderinside[(orderinside["Queue_Time"].isnull() & ((orderinside["Enter_Time"] < queue_time) & (orderinside["Enter_Time"] > five_min_ago)))]

        # 十分鐘內進場需要排隊的人
        ten_queue_records = ten_queue_records_total[~ten_queue_records_total["Queue_Time"].isnull()]
        # orderinside[(~orderinside["Queue_Time"].isnull() & ((orderinside["Enter_Time"] > ten_min_ago) & (orderinside["Enter_Time"] < queue_time)))]
        # 十分鐘內進場不須排隊的人
        ten_noqueue_records = ten_queue_records_total[ten_queue_records_total["Queue_Time"].isnull()]
        # orderinside[(orderinside["Queue_Time"].isnull() & ((orderinside["Enter_Time"] < queue_time) & (orderinside["Enter_Time"] > ten_min_ago)))]

        # 十五分鐘內進場需要排隊的人
        fifteen_queue_records = fifteen_queue_records_total[~fifteen_queue_records_total["Queue_Time"].isnull()]
        # orderinside[(~orderinside["Queue_Time"].isnull() & ((orderinside["Enter_Time"] > fifteen_min_ago) & (orderinside["Enter_Time"] < queue_time)))]
        # 十五分鐘內進場不須排隊的人
        fifteen_noqueue_records = fifteen_queue_records_total[fifteen_queue_records_total["Queue_Time"].isnull()]
        # orderinside[(orderinside["Queue_Time"].isnull() & ((orderinside["Enter_Time"] < queue_time) & (orderinside["Enter_Time"] > fifteen_min_ago)))]


        five_ratio, ten_ratio, fifteen_ratio = 0, 0, 0

        if (len(five_queue_records) + len(five_noqueue_records)) > 0:
            five_ratio = round(len(five_queue_records) / (len(five_queue_records) + len(five_noqueue_records)), 2)
        if (len(ten_queue_records) + len(ten_noqueue_records)) > 0:
            ten_ratio = round(len(ten_queue_records) / (len(ten_queue_records) + len(ten_noqueue_records)), 2)
        if (len(fifteen_queue_records) + len(fifteen_noqueue_records)) > 0:
            fifteen_ratio = round(len(fifteen_queue_records) / (len(fifteen_queue_records) + len(fifteen_noqueue_records)), 2)

        data.append(five_ratio)
        data.append(ten_ratio)
        data.append(fifteen_ratio)

        result.append(data)

    return pd.DataFrame(result, columns=columns)

def dining_order_number(base, queue = None, orderinside = None, orderoutside = None, order_achievement = None, time_range = None):

    def get_order_number(order_achievement, dining):

        order_number = order_achievement[order_achievement["Serial_Number"].isin(dining["Serial_Number"])]

        return order_number

    result = []

    columns = ["Serial_Number", "C09_order_count"]

    order_achievement = order_achievement[["Serial_Number", "Edit_Time", "Order_No"]]
    order_achievement["Order_No"] = order_achievement["Order_No"].astype(int)
    order_achievement = order_achievement.groupby(by=["Serial_Number", "Edit_Time"], as_index = False)["Order_No"].max()

    for queue_time, serial_number in base[["Queue_Time", "Serial_Number"]].values:

        data = []

        #取得用餐中的人
        dining = orderinside[((queue_time > orderinside['Enter_Time']) & (queue_time < orderinside['WalkOut_Time']))]
        dining = dining[dining["Serial_Number"] != serial_number]

        order_achievement_new = order_achievement[(order_achievement["Edit_Time"] < queue_time)]

        order_number_list = get_order_number(order_achievement_new, dining)
        order_number_sum = order_number_list["Order_No"].sum()
        data.append(serial_number)
        data.append(order_number_sum)
        result.append(data)

    return pd.DataFrame(result, columns=columns)

def dining_order_ratio(base, queue = None, orderinside = None, orderoutside = None, order_achievement = None, time_range = None):

    def get_order_bool(order_achievement, dining):

        # order = pd.merge(order_achievement, dining["Serial_Number"], on="Serial_Number")
        order = order_achievement[order_achievement["Serial_Number"].isin(dining["Serial_Number"])]["Order_No"]
        # order = order[(order["Edit_Time"] < queue_time)]
        # order["Order_No"] = order["Order_No"].astype(int)
        # order = order_achievement.groupby(by="Serial_Number")["Order_No"].max()

        return order

    result = []

    columns = ["Serial_Number", "C10_order_ratio"]

    order_achievement = order_achievement[["Serial_Number", "Edit_Time", "Order_No"]]
    order_achievement["Order_No"] = order_achievement["Order_No"].astype(int)

    order_achievement = order_achievement.groupby(by=["Serial_Number", "Edit_Time"], as_index = False)["Order_No"].max()

    for queue_time, serial_number in base[["Queue_Time", "Serial_Number"]].values:

        data = []

        #取得用餐中的人
        dining = orderinside[((queue_time > orderinside["Enter_Time"]) & (queue_time < orderinside["WalkOut_Time"]))]
        dining = dining[dining["Serial_Number"] != serial_number]

        order_achievement_new = order_achievement[(order_achievement["Edit_Time"] < queue_time)]

        orderNo_list = get_order_bool(order_achievement_new, dining)

        ratio = 0
        if len(orderNo_list) > 0:
            ratio = orderNo_list.sum() / len(orderNo_list)

        data.append(serial_number)
        data.append(ratio)
        result.append(data)

    return pd.DataFrame(result, columns=columns)

def dining_unique_order_amount(base, queue = None, orderinside = None, orderoutside = None, order_achievement = None, time_range = None):

    def get_order_item_groupby_serial_number(order_achievement, dining):

        prodct_No_groupby_serial_number = order_achievement[order_achievement["Serial_Number"].isin(dining["Serial_Number"])]["Product_No"]

        return prodct_No_groupby_serial_number


    result = []

    columns = ["Serial_Number", "C11_unique_order_sum", "C11_unique_order_max", "C11_unique_order_min", "C11_unique_order_avg"]

    order_achievement = order_achievement[["Serial_Number", "Order_Time", "Product_No"]]

    order_achievement = order_achievement[(((order_achievement["Product_No"].astype(int) >= 1) & (order_achievement["Product_No"].astype(int) <= 31)) | (order_achievement["Product_No"].astype(int) == 90))]

    order_achievement = order_achievement.groupby(by=["Serial_Number", "Order_Time"], as_index = False)["Product_No"].nunique()

    for queue_time, serial_number in base[["Queue_Time", "Serial_Number"]].values:

        data = []

        data.append(str(serial_number))

        dining = orderinside[((queue_time > orderinside["Enter_Time"]) & (queue_time < orderinside["WalkOut_Time"]))]
        dining = dining[dining["Serial_Number"] != serial_number]

        order_achievement_new = order_achievement[(order_achievement["Order_Time"] < queue_time)]

        order_item_number_list = get_order_item_groupby_serial_number(order_achievement_new, dining)

        sum_order_item_number, max_order_item_number, min_order_item_number, avg_order_item_number = 0, 0, 0, 0

        if len(order_item_number_list) > 0:
            sum_order_item_number = order_item_number_list.sum()
            max_order_item_number = order_item_number_list.max()
            min_order_item_number = order_item_number_list.min()
            avg_order_item_number = sum_order_item_number / len(order_item_number_list)


        data.append(sum_order_item_number)
        data.append(max_order_item_number)
        data.append(min_order_item_number)
        data.append(avg_order_item_number)

        result.append(data)

    return pd.DataFrame(result, columns=columns)


def dining_enter_to_first_order_waiting_time(base, queue = None, orderinside = None, orderoutside = None, order_achievement = None, time_range = None):

    result = []

    columns = ["Serial_Number", "C12_avg_time", "C12_sum_time", "C12_max_time", "C12_min_time"]

    order_achievement = order_achievement[["Serial_Number", "OrderOut_Time"]]

    firstOrder_orderOut_time = order_achievement.groupby(by="Serial_Number", as_index = False)["OrderOut_Time"].min()

    def get_first_order_dish(order_achievement, dining):

        order = order_achievement[order_achievement["Serial_Number"].isin(dining["Serial_Number"])]

        order = order.set_index("Serial_Number")

        # firstOrder_orderOut_time = order.groupby(by="Serial_Number")["OrderOut_Time"].min()
        # firstOrder_orderOut_time = firstOrder_orderOut_time[firstOrder_orderOut_time < queue_time]
        dining_enterTime = dining.set_index("Serial_Number")["Enter_Time"]
        time_diff = (order["OrderOut_Time"] - dining_enterTime).dt.total_seconds() / 60

        time_diff = time_diff.dropna()

        return time_diff

    for queue_time, serial_number in base[["Queue_Time", "Serial_Number"]].values:

        data = []

        data.append(serial_number)

        dining = orderinside[((queue_time > orderinside["Enter_Time"]) & (queue_time < orderinside["WalkOut_Time"]))]
        dining = dining[dining["Serial_Number"] != serial_number]

        # order_achievement_new = order_achievement[(order_achievement["OrderOut_Time"] < queue_time)]
        order_achievement_new = firstOrder_orderOut_time[firstOrder_orderOut_time["OrderOut_Time"] < queue_time]

        first_dish_waiting_time = get_first_order_dish(order_achievement_new, dining)

        avg_time, sum_time, max_time, min_time = 0, 0, 0, 0

        if len(first_dish_waiting_time) > 0:
            avg_time = round(first_dish_waiting_time.sum() / len(first_dish_waiting_time), 2)
            sum_time = first_dish_waiting_time.sum()
            max_time = first_dish_waiting_time.max()
            min_time = first_dish_waiting_time.min()

            # if min_time < 0:
            #     print(min_time)
            #     print(first_dish_waiting_time)
            #     os._exit(0)

        data.append(avg_time)
        data.append(sum_time)
        data.append(max_time)
        data.append(min_time)

        result.append(data)

    return pd.DataFrame(result, columns=columns)

def dining_first_order_to_order_out_time(base, queue = None, orderinside = None, orderoutside = None, order_achievement = None, time_range = None):

    result = []

    columns = ["Serial_Number", "C13_avg_time", "C13_sum_time", "C13_max_time", "C13_min_time"]

    order_achievement = order_achievement[["Serial_Number", "OrderOut_Time", "Order_Time"]]

    firstOrder_orderOut_time = order_achievement.groupby(by="Serial_Number", as_index = False)[["OrderOut_Time", "Order_Time"]].min()

    def get_first_order_dish(order_achievement, dining):

        order = order_achievement[order_achievement["Serial_Number"].isin(dining["Serial_Number"])]

        time_diff = (order["OrderOut_Time"] - order["Order_Time"]).dt.total_seconds() / 60
        time_diff[time_diff < 0] = 0

        return time_diff

    for queue_time, serial_number in base[["Queue_Time", "Serial_Number"]].values:

        data = []

        data.append(serial_number)

        dining = orderinside[((queue_time > orderinside["Enter_Time"]) & (queue_time < orderinside["WalkOut_Time"]))]
        dining = dining[dining["Serial_Number"] != serial_number]

        order_achievement_new = firstOrder_orderOut_time[firstOrder_orderOut_time["OrderOut_Time"] < queue_time]

        first_dish_waiting_time = get_first_order_dish(order_achievement_new, dining)

        avg_time, sum_time, max_time, min_time = 0, 0, 0, 0

        if len(first_dish_waiting_time) > 0:
            avg_time = round(first_dish_waiting_time.sum() / len(first_dish_waiting_time), 2)
            sum_time = first_dish_waiting_time.sum()
            max_time = first_dish_waiting_time.max()
            min_time = first_dish_waiting_time.min()

        data.append(avg_time)
        data.append(sum_time)
        data.append(max_time)
        data.append(min_time)

        result.append(data)

    return pd.DataFrame(result, columns=columns)


def dining_lastOrderTime_to_currentTime(base, queue = None, orderinside = None, orderoutside = None, order_achievement = None, time_range = None):

    result = []

    columns = ["Serial_Number", "C14_avg_time", "C14_sum_time", "C14_max_time", "C14_min_time"]

    order_achievement = order_achievement[["Serial_Number", "Order_Time"]]

    lastOrder_order_time = order_achievement.groupby(by="Serial_Number", as_index = False)["Order_Time"].max()

    def get_last_order_dish_time_diff(order_achievement, dining):
        # order = pd.merge(order_achievement, dining["Serial_Number"], on="Serial_Number")
        order = order_achievement[order_achievement["Serial_Number"].isin(dining["Serial_Number"])]
        # firstOrder_orderOut_time = order.groupby(by="Serial_Number")[["Order_Time"]].max()
        time_diff = (queue_time - order["Order_Time"]).dt.total_seconds() / 60

        return time_diff

    for queue_time, serial_number in base[["Queue_Time", "Serial_Number"]].values:

        data = []

        data.append(serial_number)

        dining = orderinside[((queue_time > orderinside["Enter_Time"]) & (queue_time < orderinside["WalkOut_Time"]))]
        dining = dining[dining["Serial_Number"] != serial_number]

        order_achievement_new = lastOrder_order_time[lastOrder_order_time["Order_Time"] < queue_time]

        last_order_dish_time_diff = get_last_order_dish_time_diff(order_achievement_new, dining)

        avg_time, sum_time, max_time, min_time = 0, 0, 0, 0

        if len(last_order_dish_time_diff) > 0:
            avg_time = round(last_order_dish_time_diff.sum() / len(last_order_dish_time_diff), 2)
            sum_time = last_order_dish_time_diff.sum()
            max_time = last_order_dish_time_diff.max()
            min_time = last_order_dish_time_diff.min()

        data.append(avg_time)
        data.append(sum_time)
        data.append(max_time)
        data.append(min_time)

        result.append(data)

    return pd.DataFrame(result, columns=columns)

def dining_last_orderOutTime_to_currentTime(base, queue = None, orderinside = None, orderoutside = None, order_achievement = None, time_range = None):

    result = []

    columns = ["Serial_Number", "C15_avg_time", "C15_sum_time", "C15_max_time", "C15_min_time"]

    order_achievement = order_achievement[["Serial_Number", "OrderOut_Time"]]

    lastOrder_orderOut_time = order_achievement.groupby(by="Serial_Number", as_index = False)["OrderOut_Time"].max()

    def get_last_order_dish_time_diff(order_achievement, dining):

        order = order_achievement[order_achievement["Serial_Number"].isin(dining["Serial_Number"])]

        time_diff = (queue_time - order["OrderOut_Time"]).dt.total_seconds() / 60

        return time_diff

    for queue_time, serial_number in base[["Queue_Time", "Serial_Number"]].values:

        data = []

        data.append(serial_number)

        dining = orderinside[((queue_time > orderinside["Enter_Time"]) & (queue_time < orderinside["WalkOut_Time"]))]
        dining = dining[dining["Serial_Number"] != serial_number]

        order_achievement_new = lastOrder_orderOut_time[lastOrder_orderOut_time["OrderOut_Time"] < queue_time]

        last_order_dish_time_diff = get_last_order_dish_time_diff(order_achievement_new, dining)

        avg_time, sum_time, max_time, min_time = 0, 0, 0, 0

        if len(last_order_dish_time_diff) > 0:
            avg_time = round(last_order_dish_time_diff.sum() / len(last_order_dish_time_diff), 2)
            sum_time = last_order_dish_time_diff.sum()
            max_time = last_order_dish_time_diff.max()
            min_time = last_order_dish_time_diff.min()

        data.append(avg_time)
        data.append(sum_time)
        data.append(max_time)
        data.append(min_time)

        result.append(data)

    return pd.DataFrame(result, columns=columns)

def C11_dining_unique_order_amount(base, queue = None, orderinside = None, orderoutside = None, order_achievement = None, time_range = None):

    result = []

    columns = ["Serial_Number", "C11_unique_order_sum", "C11_unique_order_max", "C11_unique_order_min", "C11_unique_order_avg"]

    def func(base, orderinside = orderinside, order_achievement = order_achievement):
        
        data = []

        queue_time, serial_number = base[["Queue_Time", "Serial_Number"]].values
        dining = orderinside[((queue_time > orderinside["Enter_Time"]) & (queue_time < orderinside["WalkOut_Time"]))]

        dining = dining[dining["Serial_Number"] != serial_number]

        order = pd.merge(order_achievement, dining["Serial_Number"], on = "Serial_Number", how = "inner")

        product_No = order[(((order["Product_No"].astype(int) >= 1) & (order["Product_No"].astype(int) <= 31)) | (order["Product_No"].astype(int) == 90))]["Product_No"].nunique()

        sum_order_item_number, max_order_item_number, min_order_item_number, avg_order_item_number = 0, 0, 0, 0

        if len(product_No) > 0:
            sum_order_item_number = product_No.sum()
            max_order_item_number = product_No.max()
            min_order_item_number = product_No.min()
            avg_order_item_number = sum_order_item_number / len(product_No)
        
        data.append(serial_number)
        data.append(sum_order_item_number)
        data.append(max_order_item_number)
        data.append(min_order_item_number)
        data.append(avg_order_item_number)

        result.append(data)

        return pd.DataFrame(result, columns=columns)

    Parallel_func = partial(func, orderinside = orderinside, order_achievement = order_achievement)

    output_df = parallel_apply(base, Parallel_func, n_jobs = 10, axis = 1)


# class Base:
#     def __init__(self):
#         self.db = None
#         self.init()

#     def init(self):
#         #get data from db
#         db = DBConnection()
#         db.get_connnection()

#         self.db = db

#     # get data from table
#     # get_data_from_table(db)


#     def get_data_from_table(self):

#         dtf_base = DTFBase(self.db.cursor)
#         base_records = dtf_base.get_dataInfo("select * from dtf_base")

#         # get queue customer amount
#         order_inside_servoce = DTFOrderInside(self.db.cursor)
#         self.queue_customer_amount(base_records, orderinside = order_inside_servoce)

#         # self.unoccupied_seats(base_records)


#         # for record in base_records:

#         # googleTrendsService = GoogleTrends(self.db.cursor)
#         # googleTrendsService.get_dataInfo()

#         # govScheduleService = GovSchedule(self.db.cursor)
#         # govScheduleService.get_dataInfo()

#         # holidayCNService = HolidayCN(self.db.cursor)
#         # holidayCNService.get_dataInfo()

#         # holidayJPService = HolidayJP(self.db.cursor)
#         # holidayJPService.get_dataInfo()
    
#     def unoccupied_seats(self, base, queue = None, orderinside = None, orderoutside = None, order_achievement = None, time_range = None):
#         # total_table – occupied_table 
#         for serial_number, queue_time in base:
#             pass
#             os._exit(0)

#     def queue_customer_amount(self, base, queue = None, orderinside = None, orderoutside = None, order_achievement = None, time_range = None):
        
#         result = []
#         columns = ["totalCount", "adult_count", "kid_count", "people_count", 
#                                  "adult_max", "kid_max", "people_max", 
#                                  "adult_min", "kid_min", "people_min",
#                                  "adult_avg", "kid_avg", "people_avg"]
        
#         for serial_number, queue_time in base:
#             data = []
#             queue_records = orderinside.get_dataInfo("SELECT * FROM dtf_orderinside WHERE `Enter_Time` > %s AND `Queue_Time` < %s", queue_time)
#             adult_count, kid_count = 0, 0
#             adult_max, kid_max, people_max = 0, 0, 0
#             adult_min, kid_min, people_min = 1000000, 1000000, 1000000

#             for record in queue_records:
#                 adult_count += record[3]
#                 kid_count += record[4]
#                 people_count = adult_count + kid_count

#                 # get max number 
#                 if adult_count > adult_max:
#                     adult_max = adult_count
#                 if kid_count > kid_max:
#                     kid_max = kid_count
#                 if people_count > people_max:
#                     people_max = people_count

#                 # get min number 
#                 if adult_count < adult_min:
#                     adult_min = adult_count
#                 if kid_count < kid_min:
#                     kid_min = kid_count
#                 if people_count < people_min:
#                     people_min = people_count

#             data.append(adult_count)
#             data.append(kid_count)
#             data.append(people_count)

#             data.append(adult_max)
#             data.append(kid_max)
#             data.append(people_max)

#             data.append(adult_min)
#             data.append(kid_min)
#             data.append(people_min)

#             # get average number
#             adult_avg = adult_count / len(queue_records)
#             kid_avg = kid_count / len(queue_records)
#             people_avg = people_count / len(queue_records)

#             data.append(adult_avg)
#             data.append(kid_avg)
#             data.append(people_avg)

#             result.append(data)

#         return pd.DataFrame(result, columns=columns)
 
#     def timeperiod_queue_amount(self, base, queue = None, orderinside = None, orderoutside = None, order_achievement = None, time_range = None):
        
#         result = []
#         columns = ["five_min_queue_amount", "ten_min_queue_amount", "fifteen_min_queue_amount"]
#         for serial_number, queue_time in base:
#             data = []
#             five_queue_records = orderinside.get_dataInfo(f"SELECT * FROM dtf_orderinside WHERE `Queue_Time` BETWEEN date_sub({queue_time}, interval 5 minute) AND {queue_time}")
#             ten_queue_records = orderinside.get_dataInfo(f"SELECT * FROM dtf_orderinside WHERE `Queue_Time` BETWEEN date_sub({queue_time}, interval 10 minute) AND {queue_time}")
#             fifteen_queue_records = orderinside.get_dataInfo(f"SELECT * FROM dtf_orderinside WHERE `Queue_Time` BETWEEN date_sub({queue_time}, interval 15 minute) AND {queue_time}")
            
#             data.append(len(five_queue_records))
#             data.append(len(ten_queue_records)) 
#             data.append(len(fifteen_queue_records)) # data = [5, 8, 4]

#             result.append(data) # result = [[5, 8, 4]]
        
#         return pd.DataFrame(result, columns=columns)