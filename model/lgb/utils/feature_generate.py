import time
from services.connection import *
from functools import reduce

def generate_features(dfbase_dataframe, df_order_insiede_dataframe, df_order_achievement_dataframe):
    ######################################### create variable #########################################

    start = time.time()
    df_B02 = parallel_process(dfbase_dataframe, 5, queue_customer_amount, df_order_insiede_dataframe)
    # B02_total_count(df_B02)
    print(f"Runtime is {time.time() - start}")

    start = time.time()
    df_B04 = parallel_process(dfbase_dataframe, 5, time_period_queue_amount, df_order_insiede_dataframe)
    print(len(df_B04))
    print(f"Runtime is {time.time() - start}")

    start = time.time()
    df_B05 = parallel_process(dfbase_dataframe, 5, time_period_need_queue, df_order_insiede_dataframe)
    print(len(df_B05))
    print(f"Runtime is {time.time() - start}")

    start = time.time()
    df_B06 = parallel_process(dfbase_dataframe, 5, time_period_orderInside_amount, df_order_insiede_dataframe)
    print(len(df_B06))
    print(f"Runtime is {time.time() - start}")

    start = time.time()
    df_B07 = parallel_process(dfbase_dataframe, 5, time_period_waiting_time, df_order_insiede_dataframe)
    print(len(df_B07))
    print(f"Runtime is {time.time() - start}")

    start = time.time()
    df_B08 = parallel_process(dfbase_dataframe, 5, time_period_waiting_time_ratio, df_order_insiede_dataframe)
    print(len(df_B08))
    print(f"Runtime is {time.time() - start}")

    start = time.time()
    df_C09 = parallel_process(dfbase_dataframe, 5, dining_order_number, df_order_insiede_dataframe, df_order_achievement_dataframe)
    print(df_C09)
    print(f"Runtime is {time.time() - start}")

    start = time.time()
    df_C10 = parallel_process(dfbase_dataframe, 5, dining_order_ratio, df_order_insiede_dataframe, df_order_achievement_dataframe)
    print(df_C10)
    print(f"Runtime is {time.time() - start}")

    start = time.time()
    df_C11 = parallel_process(dfbase_dataframe, 5, dining_unique_order_amount, df_order_insiede_dataframe, df_order_achievement_dataframe)
    print(df_C11)
    print(f"Runtime is {time.time() - start}")

    start = time.time()
    df_C12 = parallel_process(dfbase_dataframe, 5, dining_enter_to_first_order_waiting_time, df_order_insiede_dataframe, df_order_achievement_dataframe)
    print(df_C12)
    print(f"Runtime is {time.time() - start}") 

    start = time.time()
    df_C13 = parallel_process(dfbase_dataframe, 5, dining_first_order_to_order_out_time, df_order_insiede_dataframe, df_order_achievement_dataframe)
    print(df_C13)
    print(f"Runtime is {time.time() - start}") 

    start = time.time()
    df_C14 = parallel_process(dfbase_dataframe, 5, dining_lastOrderTime_to_currentTime, df_order_insiede_dataframe, df_order_achievement_dataframe)
    print(df_C14)
    print(f"Runtime is {time.time() - start}")  

    start = time.time()
    df_C15 = parallel_process(dfbase_dataframe, 5, dining_last_orderOutTime_to_currentTime, df_order_insiede_dataframe, df_order_achievement_dataframe)
    print(df_C15)
    print(f"Runtime is {time.time() - start}") 

    dfs = [df_B02, df_B04, df_B05, df_B06, df_B08, df_C09, df_C10, df_C11, df_C12, df_C13, df_C14, df_C15]

    merged_df = reduce(lambda l, r: pd.merge(l, r, on='Serial_Number', how='inner'), dfs)

    print(merged_df)

    merged_df.to_csv('data/data/final_variable.csv', index=False)


def generate_numerical_features(df):

    numerical_features = []
    for dtype, feature in zip(df.dtypes, df.columns):
        if dtype == 'float64' or dtype == 'int64':
            numerical_features.append(feature)
    # print(f'{len(numerical_features)} Numeric Features : {numerical_features}\n')

    return numerical_features