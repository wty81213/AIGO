/*第一階段開發*/
--B09等候中的組數中平均的已等候時間(分鐘)
IF OBJECT_ID('AIGO2.dbo.B09') IS NOT NULL DROP TABLE AIGO2.dbo.B09;
WITH V1 AS (
SELECT MA.Serial_Number
      ,SUM(CASE WHEN J1.DT_GETTIME IS NULL THEN 0 ELSE DATEDIFF(MINUTE,J1.DT_GETTIME,MA.Queue_Time) END) AS B091_WAITTIME_SUM
	  ,MAX(CASE WHEN J1.DT_GETTIME IS NULL THEN 0 ELSE DATEDIFF(MINUTE,J1.DT_GETTIME,MA.Queue_Time) END) AS B092_WAITTIME_MAX
	  ,MIN(CASE WHEN J1.DT_GETTIME IS NULL THEN 0 ELSE DATEDIFF(MINUTE,J1.DT_GETTIME,MA.Queue_Time) END) AS B093_WAITTIME_MIN
      ,AVG(CASE WHEN J1.DT_GETTIME IS NULL THEN 0 ELSE DATEDIFF(MINUTE,J1.DT_GETTIME,MA.Queue_Time) END) AS B094_WAITTIME_AVG
FROM AIGO2.dbo.base AS MA
INNER JOIN AIGO2.dbo.Linein AS J1 ON MA.Queue_Time > J1.DT_GETTIME AND MA.Queue_Time < J1.DT_INTIME
GROUP BY MA.Serial_Number
)
,V2 AS (
SELECT MA.Serial_Number
      ,SUM(CASE WHEN J1.DT_GETTIME IS NULL THEN 0 ELSE DATEDIFF(MINUTE,J1.DT_GETTIME,MA.Queue_Time) END) AS B095_WAITTIME_SUM_CONDITION
	  ,MAX(CASE WHEN J1.DT_GETTIME IS NULL THEN 0 ELSE DATEDIFF(MINUTE,J1.DT_GETTIME,MA.Queue_Time) END) AS B096_WAITTIME_MAX_CONDITION
	  ,MIN(CASE WHEN J1.DT_GETTIME IS NULL THEN 0 ELSE DATEDIFF(MINUTE,J1.DT_GETTIME,MA.Queue_Time) END) AS B097_WAITTIME_MIN_CONDITION
      ,AVG(CASE WHEN J1.DT_GETTIME IS NULL THEN 0 ELSE DATEDIFF(MINUTE,J1.DT_GETTIME,MA.Queue_Time) END) AS B098_WAITTIME_AVG_CONDITION
FROM AIGO2.dbo.base AS MA
INNER JOIN AIGO2.dbo.Linein AS J1 ON MA.Queue_Time > J1.DT_GETTIME AND MA.Queue_Time < J1.DT_INTIME AND J1.NQ_PERSON >= (MA.Adult_Count+MA.Kid_Count)
GROUP BY MA.Serial_Number
)
,V3_1 AS (
SELECT MA.Serial_Number
      ,DATEDIFF(MINUTE,J1.DT_GETTIME,MA.Queue_Time) AS DIFF_WAIT_TIME
      ,DENSE_RANK() OVER(PARTITION BY MA.Serial_Number ORDER BY DATEDIFF(MINUTE,J1.DT_GETTIME,MA.Queue_Time) ASC, J1.DT_GETTIME ASC) AS RID
FROM AIGO2.dbo.base AS MA
INNER JOIN AIGO2.dbo.Linein AS J1 ON MA.Queue_Time > J1.DT_GETTIME AND MA.Queue_Time < J1.DT_INTIME
)
,V3_2 AS (
SELECT MA.Serial_Number
      ,(COUNT(1)+1)/2 AS MEDIAN_NUM
	  ,CASE WHEN COUNT(1) % 2 = 1 THEN '基數直接取' ELSE '偶數取平均' END AS TAG
FROM V3_1 AS MA
GROUP BY MA.Serial_Number
)
,V3_3 AS (
SELECT MA.Serial_Number,J1.MEDIAN_NUM,MA.DIFF_WAIT_TIME
FROM V3_1 AS MA
INNER JOIN V3_2 AS J1 ON MA.Serial_Number=J1.Serial_Number AND MA.RID=J1.MEDIAN_NUM
WHERE TAG = '偶數取平均'
)
,V3_4 AS (
SELECT MA.Serial_Number,DIFF_WAIT_TIME AS B099_WAITTIME_MEDIAN
FROM V3_1 AS MA
INNER JOIN V3_2 AS J1 ON MA.Serial_Number=J1.Serial_Number AND MA.RID=J1.MEDIAN_NUM
WHERE TAG = '基數直接取'
UNION
SELECT MA.Serial_Number
      ,(J1.DIFF_WAIT_TIME+MA.DIFF_WAIT_TIME)/2 AS B099_WAITTIME_MEDIAN
FROM V3_1 AS MA
INNER JOIN V3_3 AS J1 ON MA.Serial_Number=J1.Serial_Number
INNER JOIN V3_2 AS J2 ON MA.Serial_Number=J2.Serial_Number AND MA.RID=J2.MEDIAN_NUM+1
WHERE J2.TAG = '偶數取平均'
)
SELECT MA.Serial_Number
      ,V1.B091_WAITTIME_SUM
	  ,V1.B092_WAITTIME_MAX
	  ,V1.B093_WAITTIME_MIN
	  ,V1.B094_WAITTIME_AVG
	  ,V2.B095_WAITTIME_SUM_CONDITION
	  ,V2.B096_WAITTIME_MAX_CONDITION
	  ,V2.B097_WAITTIME_MIN_CONDITION
	  ,V2.B098_WAITTIME_AVG_CONDITION
	  ,V3_4.B099_WAITTIME_MEDIAN
INTO AIGO2.dbo.B09
FROM AIGO2.dbo.base AS MA
LEFT JOIN V1 ON MA.Serial_Number=V1.Serial_Number
LEFT JOIN V2 ON MA.Serial_Number=V2.Serial_Number
LEFT JOIN V3_4 ON MA.Serial_Number=V3_4.Serial_Number;



--B10前一組入店用餐時間距本次開始排隊的時間(分鐘)
IF OBJECT_ID('AIGO2.dbo.B10') IS NOT NULL DROP TABLE AIGO2.dbo.B10;
SELECT MA.Serial_Number,MIN(DATEDIFF(MINUTE,J1.Enter_Time,MA.Queue_Time)) AS B101_DIFF_ENTERTIME
INTO AIGO2.dbo.B10
FROM AIGO2.dbo.base AS MA
INNER JOIN AIGO2.dbo.OrderInside AS J1 ON MA.Queue_Time > J1.Enter_Time AND CAST(MA.Queue_Time AS DATE) = CAST(J1.Enter_Time AS DATE)
GROUP BY MA.Serial_Number;

--B11前一組開始排隊距離本次開始排隊的時間(分鐘)
IF OBJECT_ID('AIGO2.dbo.B11') IS NOT NULL DROP TABLE AIGO2.dbo.B11;
WITH V1 AS (
SELECT MA.Serial_Number,MIN(DATEDIFF(MINUTE,J1.Queue_Time,MA.Queue_Time)) AS B111_DIFF_QUEUETIME
FROM AIGO2.dbo.base AS MA
INNER JOIN AIGO2.dbo.OrderInside AS J1 ON MA.Queue_Time > J1.Queue_Time AND CAST(MA.Queue_Time AS DATE) = CAST(J1.Queue_Time AS DATE)
GROUP BY MA.Serial_Number
)
,V2 AS (
SELECT MA.Serial_Number
      ,MIN(DATEDIFF(MINUTE,J1.DT_GETTIME,MA.Queue_Time)) AS B112_DIFF_QUEUETIME
FROM AIGO2.dbo.base AS MA
INNER JOIN AIGO2.dbo.Linein AS J1 ON MA.Queue_Time > J1.DT_GETTIME AND CAST(MA.Queue_Time AS DATE) = CAST(J1.DT_GETTIME AS DATE)
GROUP BY MA.Serial_Number
)
SELECT MA.Serial_Number
      ,V1.B111_DIFF_QUEUETIME
	  ,V2.B112_DIFF_QUEUETIME
INTO AIGO2.dbo.B11
FROM AIGO2.dbo.base AS MA
LEFT JOIN V1 ON MA.Serial_Number=V1.Serial_Number
LEFT JOIN V2 ON MA.Serial_Number=V2.Serial_Number;


--B12前一組離席時間距本次開始排隊的時間(分鐘)
IF OBJECT_ID('AIGO2.dbo.B12') IS NOT NULL DROP TABLE AIGO2.dbo.B12;
WITH V1 AS (
SELECT Serial_Number,CASE WHEN WalkOut_Time > Edit_Time THEN WalkOut_Time ELSE Edit_Time END FINAL_TIME
FROM AIGO2.dbo.OrderInside
)
SELECT MA.Serial_Number
      ,MIN(DATEDIFF(MINUTE,V1.FINAL_TIME,MA.Queue_Time)) AS B121_DIFF_LEAVETIME
INTO AIGO2.dbo.B12
FROM AIGO2.dbo.base AS MA
INNER JOIN V1 ON  MA.Queue_Time > V1.FINAL_TIME AND CAST(MA.Queue_Time AS DATE) = CAST(V1.FINAL_TIME AS DATE) 
GROUP BY MA.Serial_Number;


--B13與前一組入店用餐的組相比，兩者的開始等候的相距時間(分鐘)
IF OBJECT_ID('AIGO2.dbo.B13') IS NOT NULL DROP TABLE AIGO2.dbo.B13;
WITH V1 AS (
SELECT MA.Serial_Number
      ,MA.Queue_Time
	  ,DENSE_RANK() OVER (PARTITION BY MA.Serial_Number ORDER BY J1.Enter_Time DESC, J1.Serial_Number DESC) AS RID
	  ,J1.Serial_Number AS Serial_Number_Before
FROM AIGO2.dbo.base AS MA
INNER JOIN AIGO2.dbo.OrderInside AS J1 ON MA.Queue_Time > J1.Enter_Time AND CAST(MA.Queue_Time AS DATE) = CAST(J1.Enter_Time AS DATE)
)
SELECT MA.Serial_Number,DATEDIFF(MINUTE,J1.Enter_Time,MA.Queue_Time) AS B131_DIFF_QueueTime
INTO AIGO2.dbo.B13
FROM (SELECT Serial_Number,Queue_Time,Serial_Number_Before FROM V1 WHERE RID = 1) MA
INNER JOIN AIGO2.dbo.OrderInside AS J1 ON MA.Serial_Number_Before = J1.Serial_Number;


--B14近5分鐘是否皆排隊1分鐘以上
IF OBJECT_ID('AIGO2.dbo.B14') IS NOT NULL DROP TABLE AIGO2.dbo.B14;
WITH V1_1 AS (
SELECT MA.Serial_Number
      ,Queue_Time
	  ,DT_GETTIME 
      ,DATEDIFF(MINUTE,J1.DT_GETTIME,MA.Queue_Time) AS Queue_minute
	  ,'5min' AS Tag
FROM AIGO2.dbo.base AS MA
INNER JOIN AIGO2.dbo.Linein AS J1 ON MA.Queue_Time > J1.DT_GETTIME AND DATEADD(MINUTE,-5,MA.Queue_Time) < J1.DT_GETTIME
)
,V1_2 AS (
SELECT MA.Serial_Number
      ,DATEDIFF(MINUTE,J1.DT_GETTIME,MA.Queue_Time) AS Queue_minute
	  ,'10min' AS Tag
FROM AIGO2.dbo.base AS MA
INNER JOIN AIGO2.dbo.Linein AS J1 ON MA.Queue_Time > J1.DT_GETTIME AND DATEADD(MINUTE,-10,MA.Queue_Time) < J1.DT_GETTIME
)
,V1_3 AS (
SELECT MA.Serial_Number
      ,DATEDIFF(MINUTE,J1.DT_GETTIME,MA.Queue_Time) AS Queue_minute
	  ,'15min' AS Tag
FROM AIGO2.dbo.base AS MA
INNER JOIN AIGO2.dbo.Linein AS J1 ON MA.Queue_Time > J1.DT_GETTIME AND DATEADD(MINUTE,-15,MA.Queue_Time) < J1.DT_GETTIME
)
,V2_1 AS (
SELECT Serial_Number
      ,MIN(CASE WHEN Queue_minute > 1 OR Queue_minute IS NULL THEN 'Y' ELSE 'N' END) AS B141_5min_1minup_wait
	  ,MIN(CASE WHEN Queue_minute > 3 OR Queue_minute IS NULL THEN 'Y' ELSE 'N' END) AS B142_5min_3minup_wait
FROM V1_1
GROUP BY Serial_Number
)
,V2_2 AS (
SELECT Serial_Number
      ,MIN(CASE WHEN Queue_minute > 1 OR Queue_minute IS NULL THEN 'Y' ELSE 'N' END) AS B143_10min_1minup_wait
	  ,MIN(CASE WHEN Queue_minute > 3 OR Queue_minute IS NULL THEN 'Y' ELSE 'N' END) AS B144_10min_3minup_wait
	  ,MIN(CASE WHEN Queue_minute > 5 OR Queue_minute IS NULL THEN 'Y' ELSE 'N' END) AS B145_10min_5minup_wait
FROM V1_2
GROUP BY Serial_Number
)
,V2_3 AS (
SELECT Serial_Number
      ,MIN(CASE WHEN Queue_minute > 1 OR Queue_minute IS NULL THEN 'Y' ELSE 'N' END) AS B146_15min_1minup_wait
	  ,MIN(CASE WHEN Queue_minute > 3 OR Queue_minute IS NULL THEN 'Y' ELSE 'N' END) AS B147_15min_3minup_wait
	  ,MIN(CASE WHEN Queue_minute > 5 OR Queue_minute IS NULL THEN 'Y' ELSE 'N' END) AS B148_15min_5minup_wait
	  ,MIN(CASE WHEN Queue_minute > 10 OR Queue_minute IS NULL THEN 'Y' ELSE 'N' END) AS B149_15min_10minup_wait
FROM V1_3
GROUP BY Serial_Number
)
SELECT MA.Serial_Number
      ,B141_5min_1minup_wait
	  ,B142_5min_3minup_wait
	  ,B143_10min_1minup_wait
	  ,B144_10min_3minup_wait
	  ,B145_10min_5minup_wait
	  ,B146_15min_1minup_wait
	  ,B147_15min_3minup_wait
	  ,B148_15min_5minup_wait
	  ,B149_15min_10minup_wait
INTO AIGO2.dbo.B14
FROM AIGO2.dbo.base AS MA
LEFT JOIN V2_1 AS J1 ON MA.Serial_Number=J1.Serial_Number
LEFT JOIN V2_2 AS J2 ON MA.Serial_Number=J2.Serial_Number
LEFT JOIN V2_3 AS J3 ON MA.Serial_Number=J3.Serial_Number;



--C01用餐中的客戶組數
--C02用餐中的平均每組人數
--C03用餐中的組數有外國人的比例
IF OBJECT_ID('AIGO2.dbo.C01_C02_C03') IS NOT NULL DROP TABLE AIGO2.dbo.C01_C02_C03;
WITH V1 AS (
SELECT Serial_Number
      ,Enter_Time
      ,CASE WHEN WalkOut_Time > Edit_Time THEN WalkOut_Time ELSE Edit_Time END FINAL_TIME
	  ,CASE WHEN Nation_Code = '1' OR Nation_Code IS NULL THEN 'native' ELSE '' END AS Native_Tag
	  ,Adult_Count
	  ,Kid_Count
	  ,Adult_Count + Kid_Count AS ALLCUS
FROM AIGO2.dbo.OrderInside
)
SELECT MA.Serial_Number
      --C01用餐中的客戶組數
      ,COUNT(DISTINCT J1.Serial_Number) AS C011_ING_Serial_Number
	  ,SUM(J1.ALLCUS) AS C012_ING_TTLCUS_NUMBER
	  ,SUM(J1.Adult_Count) AS C013_ING_Adult_NUMBER
	  ,SUM(J1.Kid_Count) AS C014_ING_Kid_NUMBER
      --C02用餐中的平均每組人數
      ,COUNT(DISTINCT J1.Serial_Number) AS C021_ING_Serial_Number
	  ,AVG(J1.ALLCUS) AS C022_ING_CUS_AVG
	  ,MAX(J1.ALLCUS) AS C023_ING_CUS_MAX
	  ,MIN(J1.ALLCUS) AS C024_ING_CUS_MIN
      --C03用餐中的組數有外國人的比例
      ,1 - (COUNT(DISTINCT CASE WHEN J1.Native_Tag = 'native' THEN J1.Serial_Number ELSE NULL END) * 1.0/COUNT(DISTINCT J1.Serial_Number)) AS C031_ING_FOREIGNER_PERCENT
INTO AIGO2.dbo.C01_C02_C03
FROM AIGO2.dbo.base AS MA
INNER JOIN V1 AS J1 ON MA.Queue_Time > J1.Enter_Time AND MA.Queue_Time < J1.FINAL_TIME
GROUP BY MA.Serial_Number;



/*第二階段開發*/
--B18外帶等候中組數
IF OBJECT_ID('AIGO2.dbo.B18') IS NOT NULL DROP TABLE AIGO2.dbo.B18;
SELECT MA.Serial_Number
      ,COUNT(DISTINCT J1.Serial_Number) AS B181_Outside_Serial_Number
INTO AIGO2.dbo.B18
FROM AIGO2.dbo.base AS MA
INNER JOIN AIGO2.dbo.orderoutside AS J1 ON MA.Queue_Time > J1.First_Order_Time AND MA.Queue_Time < J1.FINAL_TIME
GROUP BY MA.Serial_Number;


--B19外帶的製餐中餐點總數
--B20外帶的製餐中的去重複餐點品項數
IF OBJECT_ID('AIGO2.dbo.B19_B20') IS NOT NULL DROP TABLE AIGO2.dbo.B19_B20;
SELECT MA.Serial_Number
      ,SUM(J2.Amount) AS B191_Outside_AchievementWait_Amount
	  ,COUNT(DISTINCT J2.Product_No) AS B201_Outside_AchievementWait_DisCount
INTO AIGO2.dbo.B19_B20
FROM AIGO2.dbo.base AS MA
INNER JOIN AIGO2.dbo.orderoutside AS J1 ON MA.Queue_Time > J1.First_Order_Time AND MA.Queue_Time < J1.FINAL_TIME
INNER JOIN AIGO2.dbo.order_achievement AS J2 ON J1.Serial_Number=J2.Serial_Number AND MA.Queue_Time > J2.Order_Time AND MA.Queue_Time < J2.OrderOut_Time
GROUP BY MA.Serial_Number;


--B21近5分鐘外帶組數
IF OBJECT_ID('AIGO2.dbo.B21') IS NOT NULL DROP TABLE AIGO2.dbo.B21;
WITH V1 AS (
SELECT MA.Serial_Number,COUNT(DISTINCT J1.Serial_Number) AS B211_Outside5min_Serial_Number
FROM AIGO2.dbo.base AS MA
INNER JOIN AIGO2.dbo.orderoutside AS J1 ON MA.Queue_Time > J1.First_Order_Time AND DATEADD(MINUTE,-5,MA.Queue_Time) < J1.First_Order_Time
GROUP BY MA.Serial_Number
)
,V2 AS (
SELECT MA.Serial_Number,COUNT(DISTINCT J1.Serial_Number) AS B212_Outside10min_Serial_Number
FROM AIGO2.dbo.base AS MA
INNER JOIN AIGO2.dbo.orderoutside AS J1 ON MA.Queue_Time > J1.First_Order_Time AND DATEADD(MINUTE,-10,MA.Queue_Time) < J1.First_Order_Time
GROUP BY MA.Serial_Number
)
,V3 AS (
SELECT MA.Serial_Number,COUNT(DISTINCT J1.Serial_Number) AS B213_Outside15min_Serial_Number
FROM AIGO2.dbo.base AS MA
INNER JOIN AIGO2.dbo.orderoutside AS J1 ON MA.Queue_Time > J1.First_Order_Time AND DATEADD(MINUTE,-15,MA.Queue_Time) < J1.First_Order_Time
GROUP BY MA.Serial_Number
)
SELECT MA.Serial_Number,V1.B211_Outside5min_Serial_Number,V2.B212_Outside10min_Serial_Number,V3.B213_Outside15min_Serial_Number
INTO AIGO2.dbo.B21
FROM AIGO2.dbo.base AS MA
LEFT JOIN V1 ON MA.Serial_Number=V1.Serial_Number
LEFT JOIN V2 ON MA.Serial_Number=V2.Serial_Number
LEFT JOIN V3 ON MA.Serial_Number=V3.Serial_Number;



--B22近5分鐘外帶的點餐總數
--B23近5分鐘外帶的去重複點餐品項數
IF OBJECT_ID('AIGO2.dbo.B22_B23') IS NOT NULL DROP TABLE AIGO2.dbo.B22_B23;
WITH V1 AS (
SELECT MA.Serial_Number
      ,SUM(J2.Amount) AS B221_Outside_Achievementing_5minAmount
	  ,COUNT(DISTINCT J2.Product_No) AS B231_Outside_Achievementing_5minDisCount
FROM AIGO2.dbo.base AS MA
INNER JOIN AIGO2.dbo.orderoutside AS J1 ON MA.Queue_Time > J1.First_Order_Time AND MA.Queue_Time < J1.FINAL_TIME
INNER JOIN AIGO2.dbo.order_achievement AS J2 ON J1.Serial_Number=J2.Serial_Number AND MA.Queue_Time > J2.Order_Time AND DATEADD(MINUTE,-5,MA.Queue_Time) < J2.Order_Time
GROUP BY MA.Serial_Number
)
,V2 AS (
SELECT MA.Serial_Number
	  ,SUM(J2.Amount) AS B222_Outside_Achievementing_10minAmount
	  ,COUNT(DISTINCT J2.Product_No) AS B232_Outside_Achievementing_10minDisCount
FROM AIGO2.dbo.base AS MA
INNER JOIN AIGO2.dbo.orderoutside AS J1 ON MA.Queue_Time > J1.First_Order_Time AND MA.Queue_Time < J1.FINAL_TIME
INNER JOIN AIGO2.dbo.order_achievement AS J2 ON J1.Serial_Number=J2.Serial_Number AND MA.Queue_Time > J2.Order_Time AND DATEADD(MINUTE,-10,MA.Queue_Time) < J2.Order_Time
GROUP BY MA.Serial_Number
)
,V3 AS (
SELECT MA.Serial_Number
	  ,SUM(J2.Amount) AS B223_Outside_Achievementing_15minAmount
	  ,COUNT(DISTINCT J2.Product_No) AS B233_Outside_Achievementing_15minDisCount
FROM AIGO2.dbo.base AS MA
INNER JOIN AIGO2.dbo.orderoutside AS J1 ON MA.Queue_Time > J1.First_Order_Time AND MA.Queue_Time < J1.FINAL_TIME
INNER JOIN AIGO2.dbo.order_achievement AS J2 ON J1.Serial_Number=J2.Serial_Number AND MA.Queue_Time > J2.Order_Time AND DATEADD(MINUTE,-15,MA.Queue_Time) < J2.Order_Time
GROUP BY MA.Serial_Number
)
SELECT MA.Serial_Number
      ,V1.B221_Outside_Achievementing_5minAmount
	  ,V2.B222_Outside_Achievementing_10minAmount
	  ,V3.B223_Outside_Achievementing_15minAmount
	  ,V1.B231_Outside_Achievementing_5minDisCount
	  ,V2.B232_Outside_Achievementing_10minDisCount
	  ,V3.B233_Outside_Achievementing_15minDisCount
INTO AIGO2.dbo.B22_B23
FROM AIGO2.dbo.base AS MA
LEFT JOIN V1 ON MA.Serial_Number=V1.Serial_Number
LEFT JOIN V2 ON MA.Serial_Number=V2.Serial_Number
LEFT JOIN V3 ON MA.Serial_Number=V3.Serial_Number;





--B24近5分鐘外帶完成的組數從開始點餐至離開的平均時間(分鐘)
IF OBJECT_ID('AIGO2.dbo.B24') IS NOT NULL DROP TABLE AIGO2.dbo.B24;
WITH V1 AS (
SELECT MA.Serial_Number
      ,SUM(DATEDIFF(MINUTE,J1.First_Order_Time,J1.FINAL_TIME)) AS B2401_Outside_leave5min_sumwaittime
      ,AVG(DATEDIFF(MINUTE,J1.First_Order_Time,J1.FINAL_TIME)) AS B2404_Outside_leave5min_avgwaittime
      ,MAX(DATEDIFF(MINUTE,J1.First_Order_Time,J1.FINAL_TIME)) AS B2407_Outside_leave5min_maxwaittime
      ,MIN(DATEDIFF(MINUTE,J1.First_Order_Time,J1.FINAL_TIME)) AS B2410_Outside_leave5min_minwaittime
FROM AIGO2.dbo.base AS MA
INNER JOIN AIGO2.dbo.orderoutside AS J1 ON MA.Queue_Time > J1.FINAL_TIME AND DATEADD(MINUTE,-5,MA.Queue_Time) < J1.FINAL_TIME
GROUP BY MA.Serial_Number
)
,V2 AS (
SELECT MA.Serial_Number
	  ,SUM(DATEDIFF(MINUTE,J1.First_Order_Time,J1.FINAL_TIME)) AS B2402_Outside_leave10min_sumwaittime
	  ,AVG(DATEDIFF(MINUTE,J1.First_Order_Time,J1.FINAL_TIME)) AS B2405_Outside_leave10min_avgwaittime
	  ,MAX(DATEDIFF(MINUTE,J1.First_Order_Time,J1.FINAL_TIME)) AS B2408_Outside_leave10max_maxwaittime
	  ,MIN(DATEDIFF(MINUTE,J1.First_Order_Time,J1.FINAL_TIME)) AS B2411_Outside_leave10min_minwaittime
FROM AIGO2.dbo.base AS MA
INNER JOIN AIGO2.dbo.orderoutside AS J1 ON MA.Queue_Time > J1.FINAL_TIME AND DATEADD(MINUTE,-10,MA.Queue_Time) < J1.FINAL_TIME
GROUP BY MA.Serial_Number
)
,V3 AS (
SELECT MA.Serial_Number
	  ,SUM(DATEDIFF(MINUTE,J1.First_Order_Time,J1.FINAL_TIME)) AS B2403_Outside_leave15min_sumwaittime
	  ,AVG(DATEDIFF(MINUTE,J1.First_Order_Time,J1.FINAL_TIME)) AS B2406_Outside_leave15min_avgwaittime
	  ,MAX(DATEDIFF(MINUTE,J1.First_Order_Time,J1.FINAL_TIME)) AS B2409_Outside_leave15max_maxwaittime
	  ,MIN(DATEDIFF(MINUTE,J1.First_Order_Time,J1.FINAL_TIME)) AS B2412_Outside_leave15min_minwaittime
FROM AIGO2.dbo.base AS MA
INNER JOIN AIGO2.dbo.orderoutside AS J1 ON MA.Queue_Time > J1.FINAL_TIME AND DATEADD(MINUTE,-15,MA.Queue_Time) < J1.FINAL_TIME
GROUP BY MA.Serial_Number
)
SELECT MA.Serial_Number
      ,V1.B2401_Outside_leave5min_sumwaittime
	  ,V2.B2402_Outside_leave10min_sumwaittime
	  ,V3.B2403_Outside_leave15min_sumwaittime
	  ,V1.B2404_Outside_leave5min_avgwaittime
	  ,V2.B2405_Outside_leave10min_avgwaittime
	  ,V3.B2406_Outside_leave15min_avgwaittime
	  ,V1.B2407_Outside_leave5min_maxwaittime
	  ,V2.B2408_Outside_leave10max_maxwaittime
	  ,V3.B2409_Outside_leave15max_maxwaittime
	  ,V1.B2410_Outside_leave5min_minwaittime
	  ,V2.B2411_Outside_leave10min_minwaittime
	  ,V3.B2412_Outside_leave15min_minwaittime
INTO AIGO2.dbo.B24
FROM AIGO2.dbo.base AS MA
LEFT JOIN V1 ON MA.Serial_Number=V1.Serial_Number
LEFT JOIN V2 ON MA.Serial_Number=V2.Serial_Number
LEFT JOIN V3 ON MA.Serial_Number=V3.Serial_Number;



/*第三階段開發*/
--E01是否為美國節日
--E02有美/中/日/台節日的國家數
--E03是否為國小至高中假期
IF OBJECT_ID('AIGO2.dbo.E01_E02_E03') IS NOT NULL DROP TABLE AIGO2.dbo.E01_E02_E03;
SELECT MA.Serial_Number
      ,J1.holiday_usa AS E011_holiday_usa
	  ,J1.holiday_jp AS E012_holiday_jp
	  ,J1.holiday_cn AS E013_holiday_cn
	  ,J1.holiday_tw AS E014_holiday_tw
	  ,J1.holiday_counts AS E021_holiday_counts
	  ,J1.highsch_tag AS E031_highsch_tag
	  ,J1.university_tag AS E032_university_tag
INTO AIGO2.dbo.E01_E02_E03
FROM AIGO2.dbo.base AS MA
LEFT JOIN AIGO2.dbo.opendata_holiday AS J1 ON CAST(MA.Queue_Time AS DATE) = J1.data_dt;


--E04台北市是否停班停課
--E05是否有颱風警報
--E06颱風等級
IF OBJECT_ID('AIGO2.dbo.E04_E05_E06') IS NOT NULL DROP TABLE AIGO2.dbo.E04_E05_E06;
SELECT MA.Serial_Number
      ,MA.Queue_Time
      ,CASE WHEN J1.ttype IS NOT NULL THEN 'Y' ELSE 'N' END AS E041_stopwork
	  ,CASE WHEN J2.ttype IS NOT NULL THEN 'Y' ELSE 'N' END AS E051_typhoon
	  ,ISNULL(J2.tag,'N') AS E061_typhoon_level
INTO AIGO2.dbo.E04_E05_E06
FROM AIGO2.dbo.base AS MA
LEFT JOIN AIGO2.dbo.opendata_typhoon AS J1 ON J1.ttype='stop_work' AND MA.Queue_Time > J1.start_time AND MA.Queue_Time < J1.end_time
LEFT JOIN AIGO2.dbo.opendata_typhoon AS J2 ON J2.ttype='typhoon' AND MA.Queue_Time > J2.start_time AND MA.Queue_Time < J2.end_time;

--E07前一天同時段測站氣壓/氣溫/相對溼度/風速
IF OBJECT_ID('AIGO2.dbo.E07') IS NOT NULL DROP TABLE AIGO2.dbo.E07;
SELECT MA.Serial_Number
      ,J1.StnPres AS E071_StnPres
	  ,J1.Temperature AS E072_Temperature
	  ,J1.RH AS E073_RH
	  ,J1.WS AS E074_WS
INTO AIGO2.dbo.E07
FROM AIGO2.dbo.base AS MA
LEFT JOIN AIGO2.dbo.opendata_climate_hour AS J1 ON DATEADD(DAY,-1,CAST(MA.Queue_Time AS DATE)) = J1.ObsDay AND DATEPART(HOUR,MA.Queue_Time) = J1.ObsTime;


--E08前一天降雨量
IF OBJECT_ID('AIGO2.dbo.E08') IS NOT NULL DROP TABLE AIGO2.dbo.E08;
SELECT MA.Serial_Number
      ,J1.rainfall AS E081_rainfall_1Day
	  ,SUM(J2.rainfall) AS E082_rainfall_7Day_Sum
	  ,AVG(J2.rainfall) AS E083_rainfall_7Day_AVG
	  ,MAX(J2.rainfall) AS E084_rainfall_7Day_MAX
	  ,MIN(J2.rainfall) AS E085_rainfall_7Day_MIN
INTO AIGO2.dbo.E08
FROM AIGO2.dbo.base AS MA
LEFT JOIN AIGO2.dbo.opendata_rainfall AS J1 ON DATEADD(DAY,-1,CAST(MA.Queue_Time AS DATE)) = J1.data_dt
LEFT JOIN AIGO2.dbo.opendata_rainfall AS J2 ON DATEADD(DAY,-1,CAST(MA.Queue_Time AS DATE)) >= J2.data_dt AND DATEADD(DAY,-7,CAST(MA.Queue_Time AS DATE)) <= J2.data_dt
GROUP BY MA.Serial_Number,J1.rainfall;


--E09前一天全球英文鼎泰豐的Google搜尋趨勢
IF OBJECT_ID('AIGO2.dbo.E09') IS NOT NULL DROP TABLE AIGO2.dbo.E09;
WITH V1 AS (
SELECT MA.Serial_Number
      ,J1.trends_grobal_en AS E0901_GoogleTrends_grobalen1Day
	  ,J1.trends_grobal_ch AS E0902_GoogleTrends_grobalch1Day
	  ,J1.trends_taiwan_ch AS E0903_GoogleTrends_taiwanch1Day
	  --一週全球英文
	  ,SUM(J2.trends_grobal_en) AS E0904_GoogleTrends_grobalen7Day_sum
	  ,AVG(J2.trends_grobal_en) AS E0905_GoogleTrends_grobalen7Day_avg
	  ,MAX(J2.trends_grobal_en) AS E0906_GoogleTrends_grobalen7Day_max
	  ,MIN(J2.trends_grobal_en) AS E0907_GoogleTrends_grobalen7Day_min
	  --一週全球中文
	  ,SUM(J2.trends_grobal_ch) AS E0908_GoogleTrends_grobalch7Day_sum
	  ,AVG(J2.trends_grobal_ch) AS E0909_GoogleTrends_grobalch7Day_avg
	  ,MAX(J2.trends_grobal_ch) AS E0910_GoogleTrends_grobalch7Day_max
	  ,MIN(J2.trends_grobal_ch) AS E0911_GoogleTrends_grobalch7Day_min
	  --一週台灣中文
	  ,SUM(J2.trends_taiwan_ch) AS E0912_GoogleTrends_taiwanch7Day_sum
	  ,AVG(J2.trends_taiwan_ch) AS E0913_GoogleTrends_taiwanch7Day_avg
	  ,MAX(J2.trends_taiwan_ch) AS E0914_GoogleTrends_taiwanch7Day_max
	  ,MIN(J2.trends_taiwan_ch) AS E0915_GoogleTrends_taiwanch7Day_min
FROM AIGO2.dbo.base AS MA
LEFT JOIN AIGO2.dbo.opendata_GoogleTrends AS J1 ON DATEADD(DAY,-1,CAST(MA.Queue_Time AS DATE)) = J1.data_dt
LEFT JOIN AIGO2.dbo.opendata_GoogleTrends AS J2 ON DATEADD(DAY,-1,CAST(MA.Queue_Time AS DATE)) >= J2.data_dt AND DATEADD(DAY,-7,CAST(MA.Queue_Time AS DATE)) <= J2.data_dt
GROUP BY MA.Serial_Number,J1.trends_grobal_en,J1.trends_grobal_ch,J1.trends_taiwan_ch
)
,V2 AS (
SELECT MA.Serial_Number
	  --一個月全球英文
	  ,SUM(J1.trends_grobal_en) AS E0916_GoogleTrends_grobalen1M_sum
	  ,AVG(J1.trends_grobal_en) AS E0917_GoogleTrends_grobalen1M_avg
	  ,MAX(J1.trends_grobal_en) AS E0918_GoogleTrends_grobalen1M_max
	  ,MIN(J1.trends_grobal_en) AS E0919_GoogleTrends_grobalen1M_min
	  --一個月全球中文
	  ,SUM(J1.trends_grobal_ch) AS E0920_GoogleTrends_grobalch1M_sum
	  ,AVG(J1.trends_grobal_ch) AS E0921_GoogleTrends_grobalch1M_avg
	  ,MAX(J1.trends_grobal_ch) AS E0922_GoogleTrends_grobalch1M_max
	  ,MIN(J1.trends_grobal_ch) AS E0923_GoogleTrends_grobalch1M_min
	  --一個月台灣中文
	  ,SUM(J1.trends_taiwan_ch) AS E0924_GoogleTrends_taiwanch1M_sum
	  ,AVG(J1.trends_taiwan_ch) AS E0925_GoogleTrends_taiwanch1M_avg
	  ,MAX(J1.trends_taiwan_ch) AS E0926_GoogleTrends_taiwanch1M_max
	  ,MIN(J1.trends_taiwan_ch) AS E0927_GoogleTrends_taiwanch1M_min
FROM AIGO2.dbo.base AS MA
LEFT JOIN AIGO2.dbo.opendata_GoogleTrends AS J1 ON YEAR(DATEADD(MONTH,-1,CAST(MA.Queue_Time AS DATE))) = YEAR(J1.data_dt) AND MONTH(DATEADD(MONTH,-1,CAST(MA.Queue_Time AS DATE))) = MONTH(J1.data_dt)
GROUP BY MA.Serial_Number
)
SELECT MA.Serial_Number
      ,V1.E0901_GoogleTrends_grobalen1Day
	  ,V1.E0902_GoogleTrends_grobalch1Day
	  ,V1.E0903_GoogleTrends_taiwanch1Day
	  ,V1.E0904_GoogleTrends_grobalen7Day_sum
	  ,V1.E0905_GoogleTrends_grobalen7Day_avg
	  ,V1.E0906_GoogleTrends_grobalen7Day_max
	  ,V1.E0907_GoogleTrends_grobalen7Day_min
	  ,V1.E0908_GoogleTrends_grobalch7Day_sum
	  ,V1.E0909_GoogleTrends_grobalch7Day_avg
	  ,V1.E0910_GoogleTrends_grobalch7Day_max
	  ,V1.E0911_GoogleTrends_grobalch7Day_min
	  ,V1.E0912_GoogleTrends_taiwanch7Day_sum
	  ,V1.E0913_GoogleTrends_taiwanch7Day_avg
	  ,V1.E0914_GoogleTrends_taiwanch7Day_max
	  ,V1.E0915_GoogleTrends_taiwanch7Day_min
	  ,V2.E0916_GoogleTrends_grobalen1M_sum
	  ,V2.E0917_GoogleTrends_grobalen1M_avg
	  ,V2.E0918_GoogleTrends_grobalen1M_max
	  ,V2.E0919_GoogleTrends_grobalen1M_min
	  ,V2.E0920_GoogleTrends_grobalch1M_sum
	  ,V2.E0921_GoogleTrends_grobalch1M_avg
	  ,V2.E0922_GoogleTrends_grobalch1M_max
	  ,V2.E0923_GoogleTrends_grobalch1M_min
	  ,V2.E0924_GoogleTrends_taiwanch1M_sum
	  ,V2.E0925_GoogleTrends_taiwanch1M_avg
	  ,V2.E0926_GoogleTrends_taiwanch1M_max
	  ,V2.E0927_GoogleTrends_taiwanch1M_min
INTO AIGO2.dbo.E09
FROM AIGO2.dbo.base AS MA
LEFT JOIN V1 ON MA.Serial_Number=V1.Serial_Number
LEFT JOIN V2 ON MA.Serial_Number=V2.Serial_Number;


--E10前一個月外國旅客人次
IF OBJECT_ID('AIGO2.dbo.E10') IS NOT NULL DROP TABLE AIGO2.dbo.E10;
WITH V1 AS (
SELECT MA.Serial_Number
      ,SUM(J1.Visit) AS E1001_ForeignerTTL
      ,SUM(CASE WHEN J1.Purpose='Business' THEN J1.Visit ELSE 0 END) AS E1002_ForeignerPurpose_Business
	  ,SUM(CASE WHEN J1.Purpose='Conference' THEN J1.Visit ELSE 0 END) AS E1003_ForeignerPurpose_Conference
	  ,SUM(CASE WHEN J1.Purpose='Exhibition' THEN J1.Visit ELSE 0 END) AS E1004_ForeignerPurpose_Exhibition
	  ,SUM(CASE WHEN J1.Purpose='Leisure' THEN J1.Visit ELSE 0 END) AS E1005_ForeignerPurpose_Leisure
	  ,SUM(CASE WHEN J1.Purpose='Medical_Treatment' THEN J1.Visit ELSE 0 END) AS E1006_ForeignerPurpose_MedicalTreatment
	  ,SUM(CASE WHEN J1.Purpose='Others' THEN J1.Visit ELSE 0 END) AS E1007_ForeignerPurpose_Others
	  ,SUM(CASE WHEN J1.Purpose='Study' THEN J1.Visit ELSE 0 END) AS E1008_ForeignerPurpose_Study
	  ,SUM(CASE WHEN J1.Purpose='Visit_Relatives' THEN J1.Visit ELSE 0 END) AS E1009_ForeignerPurpose_VisitRelatives
FROM AIGO2.dbo.base AS MA
LEFT JOIN AIGO2.dbo.opendata_InboundVisitors1 AS J1 ON J1.AGE = 'Total' AND YEAR(DATEADD(MONTH,-1,CAST(MA.Queue_Time AS DATE))) = YEAR(J1.Years) AND MONTH(DATEADD(MONTH,-1,CAST(MA.Queue_Time AS DATE))) = MONTH(J1.Years)
GROUP BY MA.Serial_Number
)
,V2 AS (
SELECT MA.Serial_Number
      ,SUM(CASE WHEN J1.Gender = 'Female' THEN J1.Visit ELSE 0 END) AS E1010_ForeignerGender_Female
	  ,SUM(CASE WHEN J1.Gender = 'Male' THEN J1.Visit ELSE 0 END) AS E1011_ForeignerGender_Male
	  ,SUM(CASE WHEN J1.Residence = 'Asia' THEN J1.Visit ELSE 0 END) AS E1012_ForeignerResidence_Asia
	  ,SUM(CASE WHEN J1.Residence = 'Africa' THEN J1.Visit ELSE 0 END) AS E1013_ForeignerResidence_Africa
	  ,SUM(CASE WHEN J1.Residence = 'Oceania' THEN J1.Visit ELSE 0 END) AS E1014_ForeignerResidence_Oceania
	  ,SUM(CASE WHEN J1.Residence = 'Unknow' THEN J1.Visit ELSE 0 END) AS E1015_ForeignerResidence_Unknow
	  ,SUM(CASE WHEN J1.Residence = 'Americas' THEN J1.Visit ELSE 0 END) AS E1016_ForeignerResidence_Americas
	  ,SUM(CASE WHEN J1.Residence = 'Europe' THEN J1.Visit ELSE 0 END) AS E1017_ForeignerResidence_Europe
FROM AIGO2.dbo.base AS MA
LEFT JOIN AIGO2.dbo.opendata_InboundVisitors2 AS J1 ON YEAR(DATEADD(MONTH,-1,CAST(MA.Queue_Time AS DATE))) = YEAR(J1.Years) AND MONTH(DATEADD(MONTH,-1,CAST(MA.Queue_Time AS DATE))) = MONTH(J1.Years)
GROUP BY MA.Serial_Number
)
,V3 AS (
SELECT MA.Serial_Number
      ,SUM(CASE WHEN J1.AGE = '1-12Years' THEN J1.Visit ELSE 0 END) AS E1018_ForeignerAge_1to12Years
	  ,SUM(CASE WHEN J1.AGE = '13-19Years' THEN J1.Visit ELSE 0 END) AS E1019_ForeignerAge_13to19Years
	  ,SUM(CASE WHEN J1.AGE = '20-29Years' THEN J1.Visit ELSE 0 END) AS E1020_ForeignerAge_20to29Years
	  ,SUM(CASE WHEN J1.AGE = '30-39Years' THEN J1.Visit ELSE 0 END) AS E1021_ForeignerAge_30to39Years
	  ,SUM(CASE WHEN J1.AGE = '40-49Years' THEN J1.Visit ELSE 0 END) AS E1022_ForeignerAge_40to49Years
	  ,SUM(CASE WHEN J1.AGE = '50-59Years' THEN J1.Visit ELSE 0 END) AS E1023_ForeignerAge_50to59Years
	  ,SUM(CASE WHEN J1.AGE = '60Years_and_Over' THEN J1.Visit ELSE 0 END) AS E1024_ForeignerAge_60YearsUp
FROM AIGO2.dbo.base AS MA
LEFT JOIN AIGO2.dbo.opendata_InboundVisitors3 AS J1 ON YEAR(DATEADD(MONTH,-1,CAST(MA.Queue_Time AS DATE))) = YEAR(J1.Years) AND MONTH(DATEADD(MONTH,-1,CAST(MA.Queue_Time AS DATE))) = MONTH(J1.Years)
GROUP BY MA.Serial_Number
)
SELECT MA.Serial_Number
      ,V1.E1001_ForeignerTTL
	  ,V1.E1002_ForeignerPurpose_Business
	  ,V1.E1003_ForeignerPurpose_Conference
	  ,V1.E1004_ForeignerPurpose_Exhibition
	  ,V1.E1005_ForeignerPurpose_Leisure
	  ,V1.E1006_ForeignerPurpose_MedicalTreatment
	  ,V1.E1007_ForeignerPurpose_Others
	  ,V1.E1008_ForeignerPurpose_Study
	  ,V1.E1009_ForeignerPurpose_VisitRelatives
	  ,V2.E1010_ForeignerGender_Female
	  ,V2.E1011_ForeignerGender_Male
	  ,V2.E1012_ForeignerResidence_Asia
	  ,V2.E1013_ForeignerResidence_Africa
	  ,V2.E1014_ForeignerResidence_Oceania
	  ,V2.E1015_ForeignerResidence_Unknow
	  ,V2.E1016_ForeignerResidence_Americas
	  ,V2.E1017_ForeignerResidence_Europe
	  ,V3.E1018_ForeignerAge_1to12Years
	  ,V3.E1019_ForeignerAge_13to19Years
	  ,V3.E1020_ForeignerAge_20to29Years
	  ,V3.E1021_ForeignerAge_30to39Years
	  ,V3.E1022_ForeignerAge_40to49Years
	  ,V3.E1023_ForeignerAge_50to59Years
	  ,V3.E1024_ForeignerAge_60YearsUp
INTO AIGO2.dbo.E10
FROM AIGO2.dbo.base AS MA
LEFT JOIN V1 ON MA.Serial_Number=V1.Serial_Number
LEFT JOIN V2 ON MA.Serial_Number=V2.Serial_Number
LEFT JOIN V3 ON MA.Serial_Number=V3.Serial_Number;


--E11前一月份之台北市物價總指數
IF OBJECT_ID('AIGO2.dbo.E11') IS NOT NULL DROP TABLE AIGO2.dbo.E11;
SELECT MA.Serial_Number
	  ,J1.總指數 AS E111_CPI_TTL
	  ,J1.總指數_不含食物 AS E112_CPI_NoFood
	  ,J1.總指數_不含蔬果 AS E113_CPI_NoVegetables
      ,J1.食物類指數 AS E114_CPI_Food
	  ,J1.總指數漲跌率 AS E115_CPI_TTL_Rate
	  ,J1.食物類漲跌率 AS E116_CPI_Food_Rate
INTO AIGO2.dbo.E11
FROM AIGO2.dbo.base AS MA
LEFT JOIN AIGO2.dbo.opendata_cpi AS J1 ON YEAR(DATEADD(MONTH,-1,CAST(MA.Queue_Time AS DATE))) = SUBSTRING(J1.年月別,1,4) AND MONTH(DATEADD(MONTH,-1,CAST(MA.Queue_Time AS DATE))) = SUBSTRING(J1.年月別,6,2);

/*第四階段開發*/
--B25前一天排隊資料，最後無入席時間/最後取消/最後標示重複取號/最後改外帶的比例
IF OBJECT_ID('AIGO2.dbo.B25') IS NOT NULL DROP TABLE AIGO2.dbo.B25;
WITH V1 AS (
SELECT MA.Serial_Number
      ,(COUNT(CASE WHEN J1.DT_INTIME_ORI IS NULL THEN J1.ID_NUMBER ELSE NULL END)*1.0)/COUNT(1) AS B251_Yesterday_Queue_NoIntime_Percent
	  ,(COUNT(CASE WHEN J1.FG_STATUS = 'C' THEN J1.ID_NUMBER ELSE NULL END)*1.0)/COUNT(1) AS B252_Yesterday_Queue_Cancel_Percent
	  ,(COUNT(CASE WHEN J1.FG_STATUS = 'R' THEN J1.ID_NUMBER ELSE NULL END)*1.0)/COUNT(1) AS B253_Yesterday_Queue_Repeat_Percent
	  ,(COUNT(CASE WHEN J1.FG_STATUS = 'A' THEN J1.ID_NUMBER ELSE NULL END)*1.0)/COUNT(1) AS B254_Yesterday_Queue_Takeaway_Percent
FROM AIGO2.dbo.base AS MA
INNER JOIN AIGO2.dbo.linein AS J1 ON DATEADD(DAY,-1,CAST(MA.Queue_Time AS DATE)) = CAST(J1.DT_GETTIME AS DATE)
GROUP BY MA.Serial_Number
)
,V2 AS (
SELECT MA.Serial_Number
      ,(COUNT(CASE WHEN J1.DT_INTIME_ORI IS NULL THEN J1.ID_NUMBER ELSE NULL END)*1.0)/COUNT(1) AS B255_Lastweek_Queue_NoIntime_Percent
	  ,(COUNT(CASE WHEN J1.FG_STATUS = 'C' THEN J1.ID_NUMBER ELSE NULL END)*1.0)/COUNT(1) AS B256_Lastweek_Queue_Cancel_Percent
	  ,(COUNT(CASE WHEN J1.FG_STATUS = 'R' THEN J1.ID_NUMBER ELSE NULL END)*1.0)/COUNT(1) AS B257_Lastweek_Queue_Repeat_Percent
	  ,(COUNT(CASE WHEN J1.FG_STATUS = 'A' THEN J1.ID_NUMBER ELSE NULL END)*1.0)/COUNT(1) AS B258_Lastweek_Queue_Takeaway_Percent
FROM AIGO2.dbo.base AS MA
INNER JOIN AIGO2.dbo.linein AS J1 ON DATEADD(DAY,-7,CAST(MA.Queue_Time AS DATE)) <= CAST(J1.DT_GETTIME AS DATE) AND DATEADD(DAY,-1,CAST(MA.Queue_Time AS DATE)) >= CAST(J1.DT_GETTIME AS DATE)
GROUP BY MA.Serial_Number
)
SELECT MA.Serial_Number
      ,ISNULL(V1.B251_Yesterday_Queue_NoIntime_Percent,0) AS B251_Yesterday_Queue_NoIntime_Percent
	  ,ISNULL(V1.B252_Yesterday_Queue_Cancel_Percent,0) AS B252_Yesterday_Queue_Cancel_Percent
	  ,ISNULL(V1.B253_Yesterday_Queue_Repeat_Percent,0) AS B253_Yesterday_Queue_Repeat_Percent
	  ,ISNULL(V1.B254_Yesterday_Queue_Takeaway_Percent,0) AS B254_Yesterday_Queue_Takeaway_Percent
	  ,ISNULL(V2.B255_Lastweek_Queue_NoIntime_Percent,0) AS B255_Lastweek_Queue_NoIntime_Percent
	  ,ISNULL(V2.B256_Lastweek_Queue_Cancel_Percent,0) AS B256_Lastweek_Queue_Cancel_Percent
	  ,ISNULL(V2.B257_Lastweek_Queue_Repeat_Percent,0) AS B257_Lastweek_Queue_Repeat_Percent
	  ,ISNULL(V2.B258_Lastweek_Queue_Takeaway_Percent,0) AS B258_Lastweek_Queue_Takeaway_Percent
INTO AIGO2.dbo.B25
FROM AIGO2.dbo.base AS MA
LEFT JOIN V1 ON MA.Serial_Number=V1.Serial_Number
LEFT JOIN V2 ON MA.Serial_Number=V2.Serial_Number;


--B26當天至排隊當下，已排隊但尚無入店時間的比例(整體/排隊號碼1開頭/排隊號碼3開頭/排隊號碼5開頭/排隊號碼7開頭/排隊人數大於等於本次排隊者)
--B27當天至排隊當下，已排隊但尚無入店時間的組數(整體/排隊號碼1開頭/排隊號碼3開頭/排隊號碼5開頭/排隊號碼7開頭/排隊人數大於等於本次排隊者)
IF OBJECT_ID('AIGO2.dbo.B26_B27') IS NOT NULL DROP TABLE AIGO2.dbo.B26_B27;
SELECT MA.Serial_Number
      ,(COUNT(CASE WHEN (J1.DT_INTIME IS NULL OR MA.Queue_Time < J1.DT_INTIME) THEN J1.ID_NUMBER ELSE NULL END)*1.0)/COUNT(1) AS B261_Sameday_Queue_NoIntime_Percent
	  ,CASE WHEN COUNT(CASE WHEN SUBSTRING(CAST(J1.ID_NUMBER AS VARCHAR),1,1)='1' THEN J1.ID_NUMBER ELSE NULL END) > 0
	        THEN (COUNT(CASE WHEN (J1.DT_INTIME IS NULL OR MA.Queue_Time < J1.DT_INTIME) AND SUBSTRING(CAST(J1.ID_NUMBER AS VARCHAR),1,1)='1' THEN J1.ID_NUMBER ELSE NULL END)*1.0)/COUNT(CASE WHEN SUBSTRING(CAST(J1.ID_NUMBER AS VARCHAR),1,1)='1' THEN J1.ID_NUMBER ELSE NULL END)
			ELSE 0 END AS B262_Sameday_Queue_NoIntime_1BeginningPercent
	  ,CASE WHEN COUNT(CASE WHEN SUBSTRING(CAST(J1.ID_NUMBER AS VARCHAR),1,1)='3' THEN J1.ID_NUMBER ELSE NULL END) > 0
	        THEN (COUNT(CASE WHEN (J1.DT_INTIME IS NULL OR MA.Queue_Time < J1.DT_INTIME) AND SUBSTRING(CAST(J1.ID_NUMBER AS VARCHAR),1,1)='3' THEN J1.ID_NUMBER ELSE NULL END)*1.0)/COUNT(CASE WHEN SUBSTRING(CAST(J1.ID_NUMBER AS VARCHAR),1,1)='3' THEN J1.ID_NUMBER ELSE NULL END)
			ELSE 0 END AS B263_Sameday_Queue_NoIntime_3BeginningPercent
	  ,CASE WHEN COUNT(CASE WHEN SUBSTRING(CAST(J1.ID_NUMBER AS VARCHAR),1,1)='5' THEN J1.ID_NUMBER ELSE NULL END) > 0
	        THEN (COUNT(CASE WHEN (J1.DT_INTIME IS NULL OR MA.Queue_Time < J1.DT_INTIME) AND SUBSTRING(CAST(J1.ID_NUMBER AS VARCHAR),1,1)='5' THEN J1.ID_NUMBER ELSE NULL END)*1.0)/COUNT(CASE WHEN SUBSTRING(CAST(J1.ID_NUMBER AS VARCHAR),1,1)='5' THEN J1.ID_NUMBER ELSE NULL END)
			ELSE 0 END AS B264_Sameday_Queue_NoIntime_5BeginningPercent
	  ,CASE WHEN COUNT(CASE WHEN SUBSTRING(CAST(J1.ID_NUMBER AS VARCHAR),1,1)='7' THEN J1.ID_NUMBER ELSE NULL END) > 0
	        THEN (COUNT(CASE WHEN (J1.DT_INTIME IS NULL OR MA.Queue_Time < J1.DT_INTIME) AND SUBSTRING(CAST(J1.ID_NUMBER AS VARCHAR),1,1)='7' THEN J1.ID_NUMBER ELSE NULL END)*1.0)/COUNT(CASE WHEN SUBSTRING(CAST(J1.ID_NUMBER AS VARCHAR),1,1)='7' THEN J1.ID_NUMBER ELSE NULL END)
			ELSE 0 END AS B265_Sameday_Queue_NoIntime_7BeginningPercent
      ,CASE WHEN COUNT(CASE WHEN J1.NQ_PERSON >= (MA.Adult_Count+MA.Kid_Count) THEN J1.ID_NUMBER ELSE NULL END) > 0
	        THEN (COUNT(CASE WHEN (J1.DT_INTIME IS NULL OR MA.Queue_Time < J1.DT_INTIME) AND J1.NQ_PERSON >= (MA.Adult_Count+MA.Kid_Count) THEN J1.ID_NUMBER ELSE NULL END)*1.0)/COUNT(CASE WHEN J1.NQ_PERSON >= (MA.Adult_Count+MA.Kid_Count) THEN J1.ID_NUMBER ELSE NULL END)
			ELSE 0 END AS B266_Sameday_Queue_NoIntime_MorePersonPercent
      ,COUNT(CASE WHEN (J1.DT_INTIME IS NULL OR MA.Queue_Time < J1.DT_INTIME) THEN J1.ID_NUMBER ELSE NULL END) AS B271_Sameday_Queue_NoIntime_Count
	  ,COUNT(CASE WHEN (J1.DT_INTIME IS NULL OR MA.Queue_Time < J1.DT_INTIME) AND SUBSTRING(CAST(J1.ID_NUMBER AS VARCHAR),1,1)='1' THEN J1.ID_NUMBER ELSE NULL END) AS B272_Sameday_Queue_NoIntime_1BeginningCount
	  ,COUNT(CASE WHEN (J1.DT_INTIME IS NULL OR MA.Queue_Time < J1.DT_INTIME) AND SUBSTRING(CAST(J1.ID_NUMBER AS VARCHAR),1,1)='3' THEN J1.ID_NUMBER ELSE NULL END) AS B273_Sameday_Queue_NoIntime_3BeginningCount
	  ,COUNT(CASE WHEN (J1.DT_INTIME IS NULL OR MA.Queue_Time < J1.DT_INTIME) AND SUBSTRING(CAST(J1.ID_NUMBER AS VARCHAR),1,1)='5' THEN J1.ID_NUMBER ELSE NULL END) AS B274_Sameday_Queue_NoIntime_5BeginningCount
	  ,COUNT(CASE WHEN (J1.DT_INTIME IS NULL OR MA.Queue_Time < J1.DT_INTIME) AND SUBSTRING(CAST(J1.ID_NUMBER AS VARCHAR),1,1)='7' THEN J1.ID_NUMBER ELSE NULL END) AS B275_Sameday_Queue_NoIntime_7BeginningCount
	  ,COUNT(CASE WHEN (J1.DT_INTIME IS NULL OR MA.Queue_Time < J1.DT_INTIME) AND J1.NQ_PERSON >= (MA.Adult_Count+MA.Kid_Count) THEN J1.ID_NUMBER ELSE NULL END) AS B276_Sameday_Queue_NoIntime_MorePersonCount
INTO AIGO2.dbo.B26_B27
FROM AIGO2.dbo.base AS MA
INNER JOIN AIGO2.dbo.linein AS J1 ON CAST(MA.Queue_Time AS DATE) = CAST(J1.DT_GETTIME AS DATE) AND MA.Queue_Time > J1.DT_GETTIME
GROUP BY MA.Serial_Number;


--B28等候中排隊號碼1開頭/3開頭/5開頭/7開頭的組數
IF OBJECT_ID('AIGO2.dbo.B28') IS NOT NULL DROP TABLE AIGO2.dbo.B28;
SELECT MA.Serial_Number
      ,COUNT(CASE WHEN SUBSTRING(CAST(J1.ID_NUMBER AS VARCHAR),1,1)='1' THEN J1.ID_NUMBER ELSE NULL END) AS B281_Waiting_1Beginning
	  ,COUNT(CASE WHEN SUBSTRING(CAST(J1.ID_NUMBER AS VARCHAR),1,1)='3' THEN J1.ID_NUMBER ELSE NULL END) AS B282_Waiting_3Beginning
	  ,COUNT(CASE WHEN SUBSTRING(CAST(J1.ID_NUMBER AS VARCHAR),1,1)='5' THEN J1.ID_NUMBER ELSE NULL END) AS B283_Waiting_5Beginning
	  ,COUNT(CASE WHEN SUBSTRING(CAST(J1.ID_NUMBER AS VARCHAR),1,1)='7' THEN J1.ID_NUMBER ELSE NULL END) AS B284_Waiting_7Beginning
	  ,COUNT(CASE WHEN J1.NQ_PERSON >= (MA.Adult_Count+MA.Kid_Count) THEN J1.ID_NUMBER ELSE NULL END) AS B285_Waiting_MorePerson
INTO AIGO2.dbo.B28
FROM AIGO2.dbo.base AS MA
INNER JOIN AIGO2.dbo.Linein AS J1 ON MA.Queue_Time > J1.DT_GETTIME AND MA.Queue_Time < J1.DT_INTIME
GROUP BY MA.Serial_Number;


--C28用餐中的組數已點湯麵/拌麵餛飩/湯品/炒飯/盤菜1/盤菜2/開胃菜/小包/餃類燒賣/大包/甜點粽子/飲料類餐點的個數
--C30用餐中的組數已點湯麵/拌麵餛飩/湯品/炒飯/盤菜1/盤菜2/開胃菜/小包/餃類燒賣/大包/甜點粽子/飲料類餐點但尚未出餐的個數
--C31用餐中的組數已點湯麵/拌麵餛飩/湯品/炒飯/盤菜1/盤菜2/開胃菜/小包/餃類燒賣/大包/甜點粽子/飲料類餐點但尚未出餐的比例
IF OBJECT_ID('AIGO2.dbo.C28_C30_C31') IS NOT NULL DROP TABLE AIGO2.dbo.C28_C30_C31;
WITH V1 AS (
SELECT MA.Serial_Number
      ,SUM(CASE WHEN J3.product_name1 = '湯麵' THEN J2.Amount ELSE 0 END) AS C2801_Achievement_01_SUM
	  ,SUM(CASE WHEN J3.product_name1 = '拌麵餛飩' THEN J2.Amount ELSE 0 END) AS C2802_Achievement_02_SUM
	  ,SUM(CASE WHEN J3.product_name1 = '湯品' THEN J2.Amount ELSE 0 END) AS C2803_Achievement_06_SUM
	  ,SUM(CASE WHEN J3.product_name1 = '炒飯' THEN J2.Amount ELSE 0 END) AS C2804_Achievement_11_SUM
	  ,SUM(CASE WHEN J3.product_name1 = '盤菜1' THEN J2.Amount ELSE 0 END) AS C2805_Achievement_16_SUM
	  ,SUM(CASE WHEN J3.product_name1 = '盤菜2' THEN J2.Amount ELSE 0 END) AS C2806_Achievement_17_SUM
	  ,SUM(CASE WHEN J3.product_name1 = '開胃菜' THEN J2.Amount ELSE 0 END) AS C2807_Achievement_18_SUM
	  ,SUM(CASE WHEN J3.product_name1 = '小包' THEN J2.Amount ELSE 0 END) AS C2808_Achievement_21_SUM
	  ,SUM(CASE WHEN J3.product_name1 = '餃類燒賣' THEN J2.Amount ELSE 0 END) AS C2809_Achievement_22_SUM
	  ,SUM(CASE WHEN J3.product_name1 = '大包' THEN J2.Amount ELSE 0 END) AS C2810_Achievement_26_SUM
	  ,SUM(CASE WHEN J3.product_name1 = '甜點粽子' THEN J2.Amount ELSE 0 END) AS C2811_Achievement_31_SUM
	  ,SUM(CASE WHEN J3.product_name1 = '飲料' THEN J2.Amount ELSE 0 END) AS C2812_Achievement_90_SUM

      ,SUM(CASE WHEN J3.product_name1 = '湯麵' AND MA.Queue_Time < J2.OrderOut_Time THEN J2.Amount ELSE 0 END) AS C3001_Achievement_01_NotOrderOut_SUM
	  ,SUM(CASE WHEN J3.product_name1 = '拌麵餛飩' AND MA.Queue_Time < J2.OrderOut_Time THEN J2.Amount ELSE 0 END) AS C3002_Achievement_02_NotOrderOut_SUM
	  ,SUM(CASE WHEN J3.product_name1 = '湯品' AND MA.Queue_Time < J2.OrderOut_Time THEN J2.Amount ELSE 0 END) AS C3003_Achievement_06_NotOrderOut_SUM
	  ,SUM(CASE WHEN J3.product_name1 = '炒飯' AND MA.Queue_Time < J2.OrderOut_Time THEN J2.Amount ELSE 0 END) AS C3004_Achievement_11_NotOrderOut_SUM
	  ,SUM(CASE WHEN J3.product_name1 = '盤菜1' AND MA.Queue_Time < J2.OrderOut_Time THEN J2.Amount ELSE 0 END) AS C3005_Achievement_16_NotOrderOut_SUM
	  ,SUM(CASE WHEN J3.product_name1 = '盤菜2' AND MA.Queue_Time < J2.OrderOut_Time THEN J2.Amount ELSE 0 END) AS C3006_Achievement_17_NotOrderOut_SUM
	  ,SUM(CASE WHEN J3.product_name1 = '開胃菜' AND MA.Queue_Time < J2.OrderOut_Time THEN J2.Amount ELSE 0 END) AS C3007_Achievement_18_NotOrderOut_SUM
	  ,SUM(CASE WHEN J3.product_name1 = '小包' AND MA.Queue_Time < J2.OrderOut_Time THEN J2.Amount ELSE 0 END) AS C3008_Achievement_21_NotOrderOut_SUM
	  ,SUM(CASE WHEN J3.product_name1 = '餃類燒賣' AND MA.Queue_Time < J2.OrderOut_Time THEN J2.Amount ELSE 0 END) AS C3009_Achievement_22_NotOrderOut_SUM
	  ,SUM(CASE WHEN J3.product_name1 = '大包' AND MA.Queue_Time < J2.OrderOut_Time THEN J2.Amount ELSE 0 END) AS C3010_Achievement_26_NotOrderOut_SUM
	  ,SUM(CASE WHEN J3.product_name1 = '甜點粽子' AND MA.Queue_Time < J2.OrderOut_Time THEN J2.Amount ELSE 0 END) AS C3011_Achievement_31_NotOrderOut_SUM
	  ,SUM(CASE WHEN J3.product_name1 = '飲料' AND MA.Queue_Time < J2.OrderOut_Time THEN J2.Amount ELSE 0 END) AS C3012_Achievement_90_NotOrderOut_SUM
FROM AIGO2.dbo.base AS MA
INNER JOIN AIGO2.dbo.orderinside AS J1 ON MA.Queue_Time > J1.Enter_Time AND MA.Queue_Time < J1.WalkOut_Time
INNER JOIN AIGO2.dbo.order_achievement AS J2 ON J1.Serial_Number=J2.Serial_Number AND MA.Queue_Time > J2.Order_Time
INNER JOIN AIGO2.dbo.product_detail AS J3 ON J2.Product_No=J3.Product_No
GROUP BY MA.Serial_Number
)
SELECT MA.Serial_Number
      ,V1.C2801_Achievement_01_SUM
	  ,V1.C2802_Achievement_02_SUM
	  ,V1.C2803_Achievement_06_SUM
	  ,V1.C2804_Achievement_11_SUM
	  ,V1.C2805_Achievement_16_SUM
	  ,V1.C2806_Achievement_17_SUM
	  ,V1.C2807_Achievement_18_SUM
	  ,V1.C2808_Achievement_21_SUM
	  ,V1.C2809_Achievement_22_SUM
	  ,V1.C2810_Achievement_26_SUM
	  ,V1.C2811_Achievement_31_SUM
	  ,V1.C2812_Achievement_90_SUM
	  ,V1.C3001_Achievement_01_NotOrderOut_SUM
	  ,V1.C3002_Achievement_02_NotOrderOut_SUM
	  ,V1.C3003_Achievement_06_NotOrderOut_SUM
	  ,V1.C3004_Achievement_11_NotOrderOut_SUM
	  ,V1.C3005_Achievement_16_NotOrderOut_SUM
	  ,V1.C3006_Achievement_17_NotOrderOut_SUM
	  ,V1.C3007_Achievement_18_NotOrderOut_SUM
	  ,V1.C3008_Achievement_21_NotOrderOut_SUM
	  ,V1.C3009_Achievement_22_NotOrderOut_SUM
	  ,V1.C3010_Achievement_26_NotOrderOut_SUM
	  ,V1.C3011_Achievement_31_NotOrderOut_SUM
	  ,V1.C3012_Achievement_90_NotOrderOut_SUM
	  ,CASE WHEN C2801_Achievement_01_SUM > 0 THEN (C3001_Achievement_01_NotOrderOut_SUM*1.0)/C2801_Achievement_01_SUM ELSE NULL END AS C3101_Achievement_01_NotOrderOut_Percent
	  ,CASE WHEN C2802_Achievement_02_SUM > 0 THEN (C3002_Achievement_02_NotOrderOut_SUM*1.0)/C2802_Achievement_02_SUM ELSE NULL END AS C3102_Achievement_02_NotOrderOut_Percent
	  ,CASE WHEN C2803_Achievement_06_SUM > 0 THEN (C3003_Achievement_06_NotOrderOut_SUM*1.0)/C2803_Achievement_06_SUM ELSE NULL END AS C3103_Achievement_06_NotOrderOut_Percent
	  ,CASE WHEN C2804_Achievement_11_SUM > 0 THEN (C3004_Achievement_11_NotOrderOut_SUM*1.0)/C2804_Achievement_11_SUM ELSE NULL END AS C3104_Achievement_11_NotOrderOut_Percent
	  ,CASE WHEN C2805_Achievement_16_SUM > 0 THEN (C3005_Achievement_16_NotOrderOut_SUM*1.0)/C2805_Achievement_16_SUM ELSE NULL END AS C3105_Achievement_16_NotOrderOut_Percent
	  ,CASE WHEN C2806_Achievement_17_SUM > 0 THEN (C3006_Achievement_17_NotOrderOut_SUM*1.0)/C2806_Achievement_17_SUM ELSE NULL END AS C3106_Achievement_17_NotOrderOut_Percent
	  ,CASE WHEN C2807_Achievement_18_SUM > 0 THEN (C3007_Achievement_18_NotOrderOut_SUM*1.0)/C2807_Achievement_18_SUM ELSE NULL END AS C3107_Achievement_18_NotOrderOut_Percent
	  ,CASE WHEN C2808_Achievement_21_SUM > 0 THEN (C3008_Achievement_21_NotOrderOut_SUM*1.0)/C2808_Achievement_21_SUM ELSE NULL END AS C3108_Achievement_21_NotOrderOut_Percent
	  ,CASE WHEN C2809_Achievement_22_SUM > 0 THEN (C3009_Achievement_22_NotOrderOut_SUM*1.0)/C2809_Achievement_22_SUM ELSE NULL END AS C3109_Achievement_22_NotOrderOut_Percent
	  ,CASE WHEN C2810_Achievement_26_SUM > 0 THEN (C3010_Achievement_26_NotOrderOut_SUM*1.0)/C2810_Achievement_26_SUM ELSE NULL END AS C3110_Achievement_26_NotOrderOut_Percent
	  ,CASE WHEN C2811_Achievement_31_SUM > 0 THEN (C3011_Achievement_31_NotOrderOut_SUM*1.0)/C2811_Achievement_31_SUM ELSE NULL END AS C3111_Achievement_31_NotOrderOut_Percent
	  ,CASE WHEN C2812_Achievement_90_SUM > 0 THEN (C3012_Achievement_90_NotOrderOut_SUM*1.0)/C2812_Achievement_90_SUM ELSE NULL END AS C3112_Achievement_90_NotOrderOut_Percent
INTO AIGO2.dbo.C28_C30_C31
FROM AIGO2.dbo.base AS MA
LEFT JOIN V1 ON MA.Serial_Number=V1.Serial_Number;



--C29用餐中的組數已點湯麵/拌麵餛飩/湯品/炒飯/盤菜1/盤菜2/開胃菜/小包/餃類燒賣/大包/甜點粽子/飲料類餐點的平均等候時間
IF OBJECT_ID('AIGO2.dbo.C29') IS NOT NULL DROP TABLE AIGO2.dbo.C29;
SELECT MA.Serial_Number
      ,SUM(CASE WHEN J3.product_name1 = '湯麵' AND MA.Queue_Time < J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,MA.Queue_Time) WHEN J3.product_name1 = '湯麵' AND MA.Queue_Time >= J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,J2.OrderOut_Time) ELSE NULL END) AS C2901_Achievement_01_SUMTIME
	  ,SUM(CASE WHEN J3.product_name1 = '拌麵餛飩' AND MA.Queue_Time < J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,MA.Queue_Time) WHEN J3.product_name1 = '拌麵餛飩' AND MA.Queue_Time >= J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,J2.OrderOut_Time) ELSE NULL END) AS C2902_Achievement_02_SUMTIME
	  ,SUM(CASE WHEN J3.product_name1 = '湯品' AND MA.Queue_Time < J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,MA.Queue_Time) WHEN J3.product_name1 = '湯品' AND MA.Queue_Time >= J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,J2.OrderOut_Time) ELSE NULL END) AS C2903_Achievement_06_SUMTIME
	  ,SUM(CASE WHEN J3.product_name1 = '炒飯' AND MA.Queue_Time < J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,MA.Queue_Time) WHEN J3.product_name1 = '炒飯' AND MA.Queue_Time >= J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,J2.OrderOut_Time) ELSE NULL END) AS C2904_Achievement_11_SUMTIME
	  ,SUM(CASE WHEN J3.product_name1 = '盤菜1' AND MA.Queue_Time < J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,MA.Queue_Time) WHEN J3.product_name1 = '盤菜1' AND MA.Queue_Time >= J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,J2.OrderOut_Time) ELSE NULL END) AS C2905_Achievement_16_SUMTIME
	  ,SUM(CASE WHEN J3.product_name1 = '盤菜2' AND MA.Queue_Time < J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,MA.Queue_Time) WHEN J3.product_name1 = '拌麵餛飩' AND MA.Queue_Time >= J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,J2.OrderOut_Time) ELSE NULL END) AS C2906_Achievement_17_SUMTIME
	  ,SUM(CASE WHEN J3.product_name1 = '開胃菜' AND MA.Queue_Time < J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,MA.Queue_Time) WHEN J3.product_name1 = '拌麵餛飩' AND MA.Queue_Time >= J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,J2.OrderOut_Time) ELSE NULL END) AS C2907_Achievement_18_SUMTIME
	  ,SUM(CASE WHEN J3.product_name1 = '小包' AND MA.Queue_Time < J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,MA.Queue_Time) WHEN J3.product_name1 = '拌麵餛飩' AND MA.Queue_Time >= J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,J2.OrderOut_Time) ELSE NULL END) AS C2908_Achievement_21_SUMTIME
	  ,SUM(CASE WHEN J3.product_name1 = '餃類燒賣' AND MA.Queue_Time < J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,MA.Queue_Time) WHEN J3.product_name1 = '餃類燒賣' AND MA.Queue_Time >= J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,J2.OrderOut_Time) ELSE NULL END) AS C2909_Achievement_22_SUMTIME
	  ,SUM(CASE WHEN J3.product_name1 = '大包' AND MA.Queue_Time < J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,MA.Queue_Time) WHEN J3.product_name1 = '大包' AND MA.Queue_Time >= J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,J2.OrderOut_Time) ELSE NULL END) AS C2910_Achievement_26_SUMTIME
	  ,SUM(CASE WHEN J3.product_name1 = '甜點粽子' AND MA.Queue_Time < J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,MA.Queue_Time) WHEN J3.product_name1 = '甜點粽子' AND MA.Queue_Time >= J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,J2.OrderOut_Time) ELSE NULL END) AS C2911_Achievement_31_SUMTIME
	  ,SUM(CASE WHEN J3.product_name1 = '飲料' AND MA.Queue_Time < J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,MA.Queue_Time) WHEN J3.product_name1 = '飲料' AND MA.Queue_Time >= J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,J2.OrderOut_Time) ELSE NULL END) AS C2912_Achievement_90_SUMTIME

      ,AVG(CASE WHEN J3.product_name1 = '湯麵' AND MA.Queue_Time < J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,MA.Queue_Time) WHEN J3.product_name1 = '湯麵' AND MA.Queue_Time >= J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,J2.OrderOut_Time) ELSE NULL END) AS C2913_Achievement_01_AVGTIME
	  ,AVG(CASE WHEN J3.product_name1 = '拌麵餛飩' AND MA.Queue_Time < J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,MA.Queue_Time) WHEN J3.product_name1 = '拌麵餛飩' AND MA.Queue_Time >= J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,J2.OrderOut_Time) ELSE NULL END) AS C2914_Achievement_02_AVGTIME
	  ,AVG(CASE WHEN J3.product_name1 = '湯品' AND MA.Queue_Time < J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,MA.Queue_Time) WHEN J3.product_name1 = '湯品' AND MA.Queue_Time >= J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,J2.OrderOut_Time) ELSE NULL END) AS C2915_Achievement_06_AVGTIME
	  ,AVG(CASE WHEN J3.product_name1 = '炒飯' AND MA.Queue_Time < J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,MA.Queue_Time) WHEN J3.product_name1 = '炒飯' AND MA.Queue_Time >= J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,J2.OrderOut_Time) ELSE NULL END) AS C2916_Achievement_11_AVGTIME
	  ,AVG(CASE WHEN J3.product_name1 = '盤菜1' AND MA.Queue_Time < J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,MA.Queue_Time) WHEN J3.product_name1 = '盤菜1' AND MA.Queue_Time >= J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,J2.OrderOut_Time) ELSE NULL END) AS C2917_Achievement_16_AVGTIME
	  ,AVG(CASE WHEN J3.product_name1 = '盤菜2' AND MA.Queue_Time < J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,MA.Queue_Time) WHEN J3.product_name1 = '拌麵餛飩' AND MA.Queue_Time >= J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,J2.OrderOut_Time) ELSE NULL END) AS C2918_Achievement_17_AVGTIME
	  ,AVG(CASE WHEN J3.product_name1 = '開胃菜' AND MA.Queue_Time < J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,MA.Queue_Time) WHEN J3.product_name1 = '拌麵餛飩' AND MA.Queue_Time >= J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,J2.OrderOut_Time) ELSE NULL END) AS C2919_Achievement_18_AVGTIME
	  ,AVG(CASE WHEN J3.product_name1 = '小包' AND MA.Queue_Time < J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,MA.Queue_Time) WHEN J3.product_name1 = '拌麵餛飩' AND MA.Queue_Time >= J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,J2.OrderOut_Time) ELSE NULL END) AS C2920_Achievement_21_AVGTIME
	  ,AVG(CASE WHEN J3.product_name1 = '餃類燒賣' AND MA.Queue_Time < J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,MA.Queue_Time) WHEN J3.product_name1 = '餃類燒賣' AND MA.Queue_Time >= J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,J2.OrderOut_Time) ELSE NULL END) AS C2921_Achievement_22_AVGTIME
	  ,AVG(CASE WHEN J3.product_name1 = '大包' AND MA.Queue_Time < J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,MA.Queue_Time) WHEN J3.product_name1 = '大包' AND MA.Queue_Time >= J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,J2.OrderOut_Time) ELSE NULL END) AS C2922_Achievement_26_AVGTIME
	  ,AVG(CASE WHEN J3.product_name1 = '甜點粽子' AND MA.Queue_Time < J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,MA.Queue_Time) WHEN J3.product_name1 = '甜點粽子' AND MA.Queue_Time >= J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,J2.OrderOut_Time) ELSE NULL END) AS C2923_Achievement_31_AVGTIME
	  ,AVG(CASE WHEN J3.product_name1 = '飲料' AND MA.Queue_Time < J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,MA.Queue_Time) WHEN J3.product_name1 = '飲料' AND MA.Queue_Time >= J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,J2.OrderOut_Time) ELSE NULL END) AS C2924_Achievement_90_AVGTIME

      ,MAX(CASE WHEN J3.product_name1 = '湯麵' AND MA.Queue_Time < J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,MA.Queue_Time) WHEN J3.product_name1 = '湯麵' AND MA.Queue_Time >= J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,J2.OrderOut_Time) ELSE NULL END) AS C2925_Achievement_01_MAXTIME
	  ,MAX(CASE WHEN J3.product_name1 = '拌麵餛飩' AND MA.Queue_Time < J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,MA.Queue_Time) WHEN J3.product_name1 = '拌麵餛飩' AND MA.Queue_Time >= J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,J2.OrderOut_Time) ELSE NULL END) AS C2926_Achievement_02_MAXTIME
	  ,MAX(CASE WHEN J3.product_name1 = '湯品' AND MA.Queue_Time < J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,MA.Queue_Time) WHEN J3.product_name1 = '湯品' AND MA.Queue_Time >= J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,J2.OrderOut_Time) ELSE NULL END) AS C2927_Achievement_06_MAXTIME
	  ,MAX(CASE WHEN J3.product_name1 = '炒飯' AND MA.Queue_Time < J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,MA.Queue_Time) WHEN J3.product_name1 = '炒飯' AND MA.Queue_Time >= J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,J2.OrderOut_Time) ELSE NULL END) AS C2928_Achievement_11_MAXTIME
	  ,MAX(CASE WHEN J3.product_name1 = '盤菜1' AND MA.Queue_Time < J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,MA.Queue_Time) WHEN J3.product_name1 = '盤菜1' AND MA.Queue_Time >= J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,J2.OrderOut_Time) ELSE NULL END) AS C2929_Achievement_16_MAXTIME
	  ,MAX(CASE WHEN J3.product_name1 = '盤菜2' AND MA.Queue_Time < J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,MA.Queue_Time) WHEN J3.product_name1 = '拌麵餛飩' AND MA.Queue_Time >= J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,J2.OrderOut_Time) ELSE NULL END) AS C2930_Achievement_17_MAXTIME
	  ,MAX(CASE WHEN J3.product_name1 = '開胃菜' AND MA.Queue_Time < J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,MA.Queue_Time) WHEN J3.product_name1 = '拌麵餛飩' AND MA.Queue_Time >= J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,J2.OrderOut_Time) ELSE NULL END) AS C2931_Achievement_18_MAXTIME
	  ,MAX(CASE WHEN J3.product_name1 = '小包' AND MA.Queue_Time < J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,MA.Queue_Time) WHEN J3.product_name1 = '拌麵餛飩' AND MA.Queue_Time >= J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,J2.OrderOut_Time) ELSE NULL END) AS C2932_Achievement_21_MAXTIME
	  ,MAX(CASE WHEN J3.product_name1 = '餃類燒賣' AND MA.Queue_Time < J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,MA.Queue_Time) WHEN J3.product_name1 = '餃類燒賣' AND MA.Queue_Time >= J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,J2.OrderOut_Time) ELSE NULL END) AS C2933_Achievement_22_MAXTIME
	  ,MAX(CASE WHEN J3.product_name1 = '大包' AND MA.Queue_Time < J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,MA.Queue_Time) WHEN J3.product_name1 = '大包' AND MA.Queue_Time >= J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,J2.OrderOut_Time) ELSE NULL END) AS C2934_Achievement_26_MAXTIME
	  ,MAX(CASE WHEN J3.product_name1 = '甜點粽子' AND MA.Queue_Time < J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,MA.Queue_Time) WHEN J3.product_name1 = '甜點粽子' AND MA.Queue_Time >= J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,J2.OrderOut_Time) ELSE NULL END) AS C2935_Achievement_31_MAXTIME
	  ,MAX(CASE WHEN J3.product_name1 = '飲料' AND MA.Queue_Time < J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,MA.Queue_Time) WHEN J3.product_name1 = '飲料' AND MA.Queue_Time >= J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,J2.OrderOut_Time) ELSE NULL END) AS C2936_Achievement_90_MAXTIME

      ,MIN(CASE WHEN J3.product_name1 = '湯麵' AND MA.Queue_Time < J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,MA.Queue_Time) WHEN J3.product_name1 = '湯麵' AND MA.Queue_Time >= J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,J2.OrderOut_Time) ELSE NULL END) AS C2937_Achievement_01_MINTIME
	  ,MIN(CASE WHEN J3.product_name1 = '拌麵餛飩' AND MA.Queue_Time < J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,MA.Queue_Time) WHEN J3.product_name1 = '拌麵餛飩' AND MA.Queue_Time >= J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,J2.OrderOut_Time) ELSE NULL END) AS C2938_Achievement_02_MINTIME
	  ,MIN(CASE WHEN J3.product_name1 = '湯品' AND MA.Queue_Time < J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,MA.Queue_Time) WHEN J3.product_name1 = '湯品' AND MA.Queue_Time >= J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,J2.OrderOut_Time) ELSE NULL END) AS C2939_Achievement_06_MINTIME
	  ,MIN(CASE WHEN J3.product_name1 = '炒飯' AND MA.Queue_Time < J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,MA.Queue_Time) WHEN J3.product_name1 = '炒飯' AND MA.Queue_Time >= J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,J2.OrderOut_Time) ELSE NULL END) AS C2940_Achievement_11_MINTIME
	  ,MIN(CASE WHEN J3.product_name1 = '盤菜1' AND MA.Queue_Time < J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,MA.Queue_Time) WHEN J3.product_name1 = '盤菜1' AND MA.Queue_Time >= J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,J2.OrderOut_Time) ELSE NULL END) AS C2941_Achievement_16_MINTIME
	  ,MIN(CASE WHEN J3.product_name1 = '盤菜2' AND MA.Queue_Time < J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,MA.Queue_Time) WHEN J3.product_name1 = '拌麵餛飩' AND MA.Queue_Time >= J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,J2.OrderOut_Time) ELSE NULL END) AS C2942_Achievement_17_MINTIME
	  ,MIN(CASE WHEN J3.product_name1 = '開胃菜' AND MA.Queue_Time < J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,MA.Queue_Time) WHEN J3.product_name1 = '拌麵餛飩' AND MA.Queue_Time >= J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,J2.OrderOut_Time) ELSE NULL END) AS C2943_Achievement_18_MINTIME
	  ,MIN(CASE WHEN J3.product_name1 = '小包' AND MA.Queue_Time < J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,MA.Queue_Time) WHEN J3.product_name1 = '拌麵餛飩' AND MA.Queue_Time >= J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,J2.OrderOut_Time) ELSE NULL END) AS C2944_Achievement_21_MINTIME
	  ,MIN(CASE WHEN J3.product_name1 = '餃類燒賣' AND MA.Queue_Time < J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,MA.Queue_Time) WHEN J3.product_name1 = '餃類燒賣' AND MA.Queue_Time >= J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,J2.OrderOut_Time) ELSE NULL END) AS C2945_Achievement_22_MINTIME
	  ,MIN(CASE WHEN J3.product_name1 = '大包' AND MA.Queue_Time < J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,MA.Queue_Time) WHEN J3.product_name1 = '大包' AND MA.Queue_Time >= J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,J2.OrderOut_Time) ELSE NULL END) AS C2946_Achievement_26_MINTIME
	  ,MIN(CASE WHEN J3.product_name1 = '甜點粽子' AND MA.Queue_Time < J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,MA.Queue_Time) WHEN J3.product_name1 = '甜點粽子' AND MA.Queue_Time >= J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,J2.OrderOut_Time) ELSE NULL END) AS C2947_Achievement_31_MINTIME
	  ,MIN(CASE WHEN J3.product_name1 = '飲料' AND MA.Queue_Time < J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,MA.Queue_Time) WHEN J3.product_name1 = '飲料' AND MA.Queue_Time >= J2.OrderOut_Time THEN DATEDIFF(MINUTE,J2.Order_Time,J2.OrderOut_Time) ELSE NULL END) AS C2948_Achievement_90_MINTIME
INTO AIGO2.dbo.C29
FROM AIGO2.dbo.base AS MA
INNER JOIN AIGO2.dbo.orderinside AS J1 ON MA.Queue_Time > J1.Enter_Time AND MA.Queue_Time < J1.WalkOut_Time
INNER JOIN AIGO2.dbo.order_achievement AS J2 ON J1.Serial_Number=J2.Serial_Number AND MA.Queue_Time > J2.Order_Time
INNER JOIN AIGO2.dbo.product_detail AS J3 ON J2.Product_No=J3.Product_No
GROUP BY MA.Serial_Number;



/*總表*/
IF OBJECT_ID('AIGO2.dbo.final_feature_table') IS NOT NULL DROP TABLE AIGO2.dbo.final_feature_table;
SELECT MA.Serial_Number
      --第一階段開發
      ,ISNULL(B09.B091_WAITTIME_SUM,0) AS B091_WAITTIME_SUM
	  ,ISNULL(B09.B092_WAITTIME_MAX,0) AS B092_WAITTIME_MAX
	  ,ISNULL(B09.B093_WAITTIME_MIN,0) AS B093_WAITTIME_MIN
	  ,ISNULL(B09.B094_WAITTIME_AVG,0) AS B094_WAITTIME_AVG
	  ,ISNULL(B09.B095_WAITTIME_SUM_CONDITION,0) AS B095_WAITTIME_SUM_CONDITION
	  ,ISNULL(B09.B096_WAITTIME_MAX_CONDITION,0) AS B096_WAITTIME_MAX_CONDITION
	  ,ISNULL(B09.B097_WAITTIME_MIN_CONDITION,0) AS B097_WAITTIME_MIN_CONDITION
	  ,ISNULL(B09.B098_WAITTIME_AVG_CONDITION,0) AS B098_WAITTIME_AVG_CONDITION
	  ,ISNULL(B09.B099_WAITTIME_MEDIAN,0) AS B099_WAITTIME_MEDIAN
	  ,B10.B101_DIFF_ENTERTIME
      ,B11.B111_DIFF_QUEUETIME
	  ,B11.B112_DIFF_QUEUETIME
	  ,B12.B121_DIFF_LEAVETIME
	  ,B13.B131_DIFF_QueueTime
	  ,ISNULL(B14.B141_5min_1minup_wait,'X') AS B141_5min_1minup_wait
	  ,ISNULL(B14.B142_5min_3minup_wait,'X') AS B142_5min_3minup_wait
	  ,ISNULL(B14.B143_10min_1minup_wait,'X') AS B143_10min_1minup_wait
	  ,ISNULL(B14.B144_10min_3minup_wait,'X') AS B144_10min_3minup_wait
	  ,ISNULL(B14.B145_10min_5minup_wait,'X') AS B145_10min_5minup_wait
	  ,ISNULL(B14.B146_15min_1minup_wait,'X') AS B146_15min_1minup_wait
	  ,ISNULL(B14.B147_15min_3minup_wait,'X') AS B147_15min_3minup_wait
	  ,ISNULL(B14.B148_15min_5minup_wait,'X') AS B148_15min_5minup_wait
	  ,ISNULL(B14.B149_15min_10minup_wait,'X') AS B149_15min_10minup_wait
	  ,ISNULL(C01_C02_C03.C011_ING_Serial_Number,0) AS C011_ING_Serial_Number
	  ,ISNULL(C01_C02_C03.C012_ING_TTLCUS_NUMBER,0) AS C012_ING_TTLCUS_NUMBER
	  ,ISNULL(C01_C02_C03.C013_ING_Adult_NUMBER,0) AS C013_ING_Adult_NUMBER
	  ,ISNULL(C01_C02_C03.C014_ING_Kid_NUMBER,0) AS C014_ING_Kid_NUMBER
	  ,ISNULL(C01_C02_C03.C021_ING_Serial_Number,0) AS C021_ING_Serial_Number
	  ,ISNULL(C01_C02_C03.C022_ING_CUS_AVG,0) AS C022_ING_CUS_AVG
	  ,ISNULL(C01_C02_C03.C023_ING_CUS_MAX,0) AS C023_ING_CUS_MAX
	  ,ISNULL(C01_C02_C03.C024_ING_CUS_MIN,0) AS C024_ING_CUS_MIN
	  ,ISNULL(C01_C02_C03.C031_ING_FOREIGNER_PERCENT,0) AS C031_ING_FOREIGNER_PERCENT
	  --第二階段開發
	  ,ISNULL(B18.B181_Outside_Serial_Number,0) AS B181_Outside_Serial_Number
	  ,ISNULL(B19_B20.B191_Outside_AchievementWait_Amount,0) AS B191_Outside_AchievementWait_Amount
	  ,ISNULL(B19_B20.B201_Outside_AchievementWait_DisCount,0) AS B201_Outside_AchievementWait_DisCount
	  ,ISNULL(B21.B211_Outside5min_Serial_Number,0) AS B211_Outside5min_Serial_Number
	  ,ISNULL(B21.B212_Outside10min_Serial_Number,0) AS B212_Outside10min_Serial_Number
	  ,ISNULL(B21.B213_Outside15min_Serial_Number,0) AS B213_Outside15min_Serial_Number
	  ,ISNULL(B22_B23.B221_Outside_Achievementing_5minAmount,0) AS B221_Outside_Achievementing_5minAmount
	  ,ISNULL(B22_B23.B222_Outside_Achievementing_10minAmount,0) AS B222_Outside_Achievementing_10minAmount
	  ,ISNULL(B22_B23.B223_Outside_Achievementing_15minAmount,0) AS B223_Outside_Achievementing_15minAmount
	  ,ISNULL(B22_B23.B231_Outside_Achievementing_5minDisCount,0) AS B231_Outside_Achievementing_5minDisCount
	  ,ISNULL(B22_B23.B232_Outside_Achievementing_10minDisCount,0) AS B232_Outside_Achievementing_10minDisCount
	  ,ISNULL(B22_B23.B233_Outside_Achievementing_15minDisCount,0) AS B233_Outside_Achievementing_15minDisCount
	  ,B24.B2401_Outside_leave5min_sumwaittime
	  ,B24.B2402_Outside_leave10min_sumwaittime
	  ,B24.B2403_Outside_leave15min_sumwaittime
	  ,B24.B2404_Outside_leave5min_avgwaittime
	  ,B24.B2405_Outside_leave10min_avgwaittime
	  ,B24.B2406_Outside_leave15min_avgwaittime
	  ,B24.B2407_Outside_leave5min_maxwaittime
	  ,B24.B2408_Outside_leave10max_maxwaittime
	  ,B24.B2409_Outside_leave15max_maxwaittime
	  ,B24.B2410_Outside_leave5min_minwaittime
	  ,B24.B2411_Outside_leave10min_minwaittime
	  ,B24.B2412_Outside_leave15min_minwaittime
	  --第三階段開發:外部資料
	  ,E01_E02_E03.E011_holiday_usa
	  ,E01_E02_E03.E012_holiday_jp
	  ,E01_E02_E03.E013_holiday_cn
	  ,E01_E02_E03.E014_holiday_tw
	  ,E01_E02_E03.E021_holiday_counts
	  ,E01_E02_E03.E031_highsch_tag
	  ,E01_E02_E03.E032_university_tag
	  ,E04_E05_E06.E041_stopwork
	  ,E04_E05_E06.E051_typhoon
	  ,E04_E05_E06.E061_typhoon_level
	  ,E07.E071_StnPres
	  ,E07.E072_Temperature
	  ,E07.E073_RH
	  ,E07.E074_WS
	  ,E08.E081_rainfall_1Day
	  ,E08.E082_rainfall_7Day_Sum
	  ,E08.E083_rainfall_7Day_AVG
	  ,E08.E084_rainfall_7Day_MAX
	  ,E08.E085_rainfall_7Day_MIN
	  ,E09.E0901_GoogleTrends_grobalen1Day
	  ,E09.E0902_GoogleTrends_grobalch1Day
	  ,E09.E0903_GoogleTrends_taiwanch1Day
	  ,E09.E0904_GoogleTrends_grobalen7Day_sum
	  ,E09.E0905_GoogleTrends_grobalen7Day_avg
	  ,E09.E0906_GoogleTrends_grobalen7Day_max
	  ,E09.E0907_GoogleTrends_grobalen7Day_min
	  ,E09.E0908_GoogleTrends_grobalch7Day_sum
	  ,E09.E0909_GoogleTrends_grobalch7Day_avg
	  ,E09.E0910_GoogleTrends_grobalch7Day_max
	  ,E09.E0911_GoogleTrends_grobalch7Day_min
	  ,E09.E0912_GoogleTrends_taiwanch7Day_sum
	  ,E09.E0913_GoogleTrends_taiwanch7Day_avg
	  ,E09.E0914_GoogleTrends_taiwanch7Day_max
	  ,E09.E0915_GoogleTrends_taiwanch7Day_min
	  ,E09.E0916_GoogleTrends_grobalen1M_sum
	  ,E09.E0917_GoogleTrends_grobalen1M_avg
	  ,E09.E0918_GoogleTrends_grobalen1M_max
	  ,E09.E0919_GoogleTrends_grobalen1M_min
	  ,E09.E0920_GoogleTrends_grobalch1M_sum
	  ,E09.E0921_GoogleTrends_grobalch1M_avg
	  ,E09.E0922_GoogleTrends_grobalch1M_max
	  ,E09.E0923_GoogleTrends_grobalch1M_min
	  ,E09.E0924_GoogleTrends_taiwanch1M_sum
	  ,E09.E0925_GoogleTrends_taiwanch1M_avg
	  ,E09.E0926_GoogleTrends_taiwanch1M_max
	  ,E09.E0927_GoogleTrends_taiwanch1M_min
	  ,E10.E1001_ForeignerTTL
	  ,E10.E1002_ForeignerPurpose_Business
	  ,E10.E1003_ForeignerPurpose_Conference
	  ,E10.E1004_ForeignerPurpose_Exhibition
	  ,E10.E1005_ForeignerPurpose_Leisure
	  ,E10.E1006_ForeignerPurpose_MedicalTreatment
	  ,E10.E1007_ForeignerPurpose_Others
	  ,E10.E1008_ForeignerPurpose_Study
	  ,E10.E1009_ForeignerPurpose_VisitRelatives
	  ,E10.E1010_ForeignerGender_Female
	  ,E10.E1011_ForeignerGender_Male
	  ,E10.E1012_ForeignerResidence_Asia
	  ,E10.E1013_ForeignerResidence_Africa
	  ,E10.E1014_ForeignerResidence_Oceania
	  ,E10.E1015_ForeignerResidence_Unknow
	  ,E10.E1016_ForeignerResidence_Americas
	  ,E10.E1017_ForeignerResidence_Europe
	  ,E10.E1018_ForeignerAge_1to12Years
	  ,E10.E1019_ForeignerAge_13to19Years
	  ,E10.E1020_ForeignerAge_20to29Years
	  ,E10.E1021_ForeignerAge_30to39Years
	  ,E10.E1022_ForeignerAge_40to49Years
	  ,E10.E1023_ForeignerAge_50to59Years
	  ,E10.E1024_ForeignerAge_60YearsUp
	  ,E11.E111_CPI_TTL
	  ,E11.E112_CPI_NoFood
	  ,E11.E113_CPI_NoVegetables
	  ,E11.E114_CPI_Food
	  ,E11.E115_CPI_TTL_Rate
	  ,E11.E116_CPI_Food_Rate
	  --第四階段開發
	  ,ISNULL(B25.B251_Yesterday_Queue_NoIntime_Percent,0) AS B251_Yesterday_Queue_NoIntime_Percent
	  ,ISNULL(B25.B252_Yesterday_Queue_Cancel_Percent,0) AS B252_Yesterday_Queue_Cancel_Percent
	  ,ISNULL(B25.B253_Yesterday_Queue_Repeat_Percent,0) AS B253_Yesterday_Queue_Repeat_Percent
	  ,ISNULL(B25.B254_Yesterday_Queue_Takeaway_Percent,0) AS B254_Yesterday_Queue_Takeaway_Percent
	  ,ISNULL(B25.B255_Lastweek_Queue_NoIntime_Percent,0) AS B255_Lastweek_Queue_NoIntime_Percent
	  ,ISNULL(B25.B256_Lastweek_Queue_Cancel_Percent,0) AS B256_Lastweek_Queue_Cancel_Percent
	  ,ISNULL(B25.B257_Lastweek_Queue_Repeat_Percent,0) AS B257_Lastweek_Queue_Repeat_Percent
	  ,ISNULL(B25.B258_Lastweek_Queue_Takeaway_Percent,0) AS B258_Lastweek_Queue_Takeaway_Percent
	  ,ISNULL(B26_B27.B261_Sameday_Queue_NoIntime_Percent,0) AS B261_Sameday_Queue_NoIntime_Percent
	  ,ISNULL(B26_B27.B262_Sameday_Queue_NoIntime_1BeginningPercent,0) AS B262_Sameday_Queue_NoIntime_1BeginningPercent
	  ,ISNULL(B26_B27.B263_Sameday_Queue_NoIntime_3BeginningPercent,0) AS B263_Sameday_Queue_NoIntime_3BeginningPercent
	  ,ISNULL(B26_B27.B264_Sameday_Queue_NoIntime_5BeginningPercent,0) AS B264_Sameday_Queue_NoIntime_5BeginningPercent
	  ,ISNULL(B26_B27.B265_Sameday_Queue_NoIntime_7BeginningPercent,0) AS B265_Sameday_Queue_NoIntime_7BeginningPercent
	  ,ISNULL(B26_B27.B266_Sameday_Queue_NoIntime_MorePersonPercent,0) AS B266_Sameday_Queue_NoIntime_MorePersonPercent
	  ,ISNULL(B26_B27.B271_Sameday_Queue_NoIntime_Count,0) AS B271_Sameday_Queue_NoIntime_Count
	  ,ISNULL(B26_B27.B272_Sameday_Queue_NoIntime_1BeginningCount,0) AS B272_Sameday_Queue_NoIntime_1BeginningCount
	  ,ISNULL(B26_B27.B273_Sameday_Queue_NoIntime_3BeginningCount,0) AS B273_Sameday_Queue_NoIntime_3BeginningCount
	  ,ISNULL(B26_B27.B274_Sameday_Queue_NoIntime_5BeginningCount,0) AS B274_Sameday_Queue_NoIntime_5BeginningCount
	  ,ISNULL(B26_B27.B275_Sameday_Queue_NoIntime_7BeginningCount,0) AS B275_Sameday_Queue_NoIntime_7BeginningCount
	  ,ISNULL(B26_B27.B276_Sameday_Queue_NoIntime_MorePersonCount,0) AS B276_Sameday_Queue_NoIntime_MorePersonCount
	  ,ISNULL(B28.B281_Waiting_1Beginning,0) AS B281_Waiting_1Beginning
	  ,ISNULL(B28.B282_Waiting_3Beginning,0) AS B282_Waiting_3Beginning
	  ,ISNULL(B28.B283_Waiting_5Beginning,0) AS B283_Waiting_5Beginning
	  ,ISNULL(B28.B284_Waiting_7Beginning,0) AS B284_Waiting_7Beginning
	  ,ISNULL(B28.B285_Waiting_MorePerson,0) AS B285_Waiting_MorePerson
	  ,C28_C30_C31.C2801_Achievement_01_SUM
	  ,C28_C30_C31.C2802_Achievement_02_SUM
	  ,C28_C30_C31.C2803_Achievement_06_SUM
	  ,C28_C30_C31.C2804_Achievement_11_SUM
	  ,C28_C30_C31.C2805_Achievement_16_SUM
	  ,C28_C30_C31.C2806_Achievement_17_SUM
	  ,C28_C30_C31.C2807_Achievement_18_SUM
	  ,C28_C30_C31.C2808_Achievement_21_SUM
	  ,C28_C30_C31.C2809_Achievement_22_SUM
	  ,C28_C30_C31.C2810_Achievement_26_SUM
	  ,C28_C30_C31.C2811_Achievement_31_SUM
	  ,C28_C30_C31.C2812_Achievement_90_SUM
	  ,C29.C2901_Achievement_01_SUMTIME
	  ,C29.C2902_Achievement_02_SUMTIME
	  ,C29.C2903_Achievement_06_SUMTIME
	  ,C29.C2904_Achievement_11_SUMTIME
	  ,C29.C2905_Achievement_16_SUMTIME
	  ,C29.C2906_Achievement_17_SUMTIME
	  ,C29.C2907_Achievement_18_SUMTIME
	  ,C29.C2908_Achievement_21_SUMTIME
	  ,C29.C2909_Achievement_22_SUMTIME
	  ,C29.C2910_Achievement_26_SUMTIME
	  ,C29.C2911_Achievement_31_SUMTIME
	  ,C29.C2912_Achievement_90_SUMTIME
	  ,C29.C2913_Achievement_01_AVGTIME
	  ,C29.C2914_Achievement_02_AVGTIME
	  ,C29.C2915_Achievement_06_AVGTIME
	  ,C29.C2916_Achievement_11_AVGTIME
	  ,C29.C2917_Achievement_16_AVGTIME
	  ,C29.C2918_Achievement_17_AVGTIME
	  ,C29.C2919_Achievement_18_AVGTIME
	  ,C29.C2920_Achievement_21_AVGTIME
	  ,C29.C2921_Achievement_22_AVGTIME
	  ,C29.C2922_Achievement_26_AVGTIME
	  ,C29.C2923_Achievement_31_AVGTIME
	  ,C29.C2924_Achievement_90_AVGTIME
	  ,C29.C2925_Achievement_01_MAXTIME
	  ,C29.C2926_Achievement_02_MAXTIME
	  ,C29.C2927_Achievement_06_MAXTIME
	  ,C29.C2928_Achievement_11_MAXTIME
	  ,C29.C2929_Achievement_16_MAXTIME
	  ,C29.C2930_Achievement_17_MAXTIME
	  ,C29.C2931_Achievement_18_MAXTIME
	  ,C29.C2932_Achievement_21_MAXTIME
	  ,C29.C2933_Achievement_22_MAXTIME
	  ,C29.C2934_Achievement_26_MAXTIME
	  ,C29.C2935_Achievement_31_MAXTIME
	  ,C29.C2936_Achievement_90_MAXTIME
	  ,C29.C2937_Achievement_01_MINTIME
	  ,C29.C2938_Achievement_02_MINTIME
	  ,C29.C2939_Achievement_06_MINTIME
	  ,C29.C2940_Achievement_11_MINTIME
	  ,C29.C2941_Achievement_16_MINTIME
	  ,C29.C2942_Achievement_17_MINTIME
	  ,C29.C2943_Achievement_18_MINTIME
	  ,C29.C2944_Achievement_21_MINTIME
	  ,C29.C2945_Achievement_22_MINTIME
	  ,C29.C2946_Achievement_26_MINTIME
	  ,C29.C2947_Achievement_31_MINTIME
	  ,C29.C2948_Achievement_90_MINTIME
	  ,C28_C30_C31.C3001_Achievement_01_NotOrderOut_SUM
	  ,C28_C30_C31.C3002_Achievement_02_NotOrderOut_SUM
	  ,C28_C30_C31.C3003_Achievement_06_NotOrderOut_SUM
	  ,C28_C30_C31.C3004_Achievement_11_NotOrderOut_SUM
	  ,C28_C30_C31.C3005_Achievement_16_NotOrderOut_SUM
	  ,C28_C30_C31.C3006_Achievement_17_NotOrderOut_SUM
	  ,C28_C30_C31.C3007_Achievement_18_NotOrderOut_SUM
	  ,C28_C30_C31.C3008_Achievement_21_NotOrderOut_SUM
	  ,C28_C30_C31.C3009_Achievement_22_NotOrderOut_SUM
	  ,C28_C30_C31.C3010_Achievement_26_NotOrderOut_SUM
	  ,C28_C30_C31.C3011_Achievement_31_NotOrderOut_SUM
	  ,C28_C30_C31.C3012_Achievement_90_NotOrderOut_SUM
	  ,C28_C30_C31.C3101_Achievement_01_NotOrderOut_Percent
	  ,C28_C30_C31.C3102_Achievement_02_NotOrderOut_Percent
	  ,C28_C30_C31.C3103_Achievement_06_NotOrderOut_Percent
	  ,C28_C30_C31.C3104_Achievement_11_NotOrderOut_Percent
	  ,C28_C30_C31.C3105_Achievement_16_NotOrderOut_Percent
	  ,C28_C30_C31.C3106_Achievement_17_NotOrderOut_Percent
	  ,C28_C30_C31.C3107_Achievement_18_NotOrderOut_Percent
	  ,C28_C30_C31.C3108_Achievement_21_NotOrderOut_Percent
	  ,C28_C30_C31.C3109_Achievement_22_NotOrderOut_Percent
	  ,C28_C30_C31.C3110_Achievement_26_NotOrderOut_Percent
	  ,C28_C30_C31.C3111_Achievement_31_NotOrderOut_Percent
	  ,C28_C30_C31.C3112_Achievement_90_NotOrderOut_Percent
INTO AIGO2.dbo.final_feature_table
FROM AIGO2.dbo.base AS MA
--第一階段開發
LEFT JOIN AIGO2.dbo.B09 ON MA.Serial_Number=B09.Serial_Number
LEFT JOIN AIGO2.dbo.B10 ON MA.Serial_Number=B10.Serial_Number
LEFT JOIN AIGO2.dbo.B11 ON MA.Serial_Number=B11.Serial_Number
LEFT JOIN AIGO2.dbo.B12 ON MA.Serial_Number=B12.Serial_Number
LEFT JOIN AIGO2.dbo.B13 ON MA.Serial_Number=B13.Serial_Number
LEFT JOIN AIGO2.dbo.B14 ON MA.Serial_Number=B14.Serial_Number
LEFT JOIN AIGO2.dbo.C01_C02_C03 ON MA.Serial_Number=C01_C02_C03.Serial_Number
--第二階段開發
LEFT JOIN AIGO2.dbo.B18 ON MA.Serial_Number=B18.Serial_Number
LEFT JOIN AIGO2.dbo.B19_B20 ON MA.Serial_Number=B19_B20.Serial_Number
LEFT JOIN AIGO2.dbo.B21 ON MA.Serial_Number=B21.Serial_Number
LEFT JOIN AIGO2.dbo.B22_B23 ON MA.Serial_Number=B22_B23.Serial_Number
LEFT JOIN AIGO2.dbo.B24 ON MA.Serial_Number=B24.Serial_Number
--第三階段開發
LEFT JOIN AIGO2.dbo.E01_E02_E03 ON MA.Serial_Number=E01_E02_E03.Serial_Number
LEFT JOIN AIGO2.dbo.E04_E05_E06 ON MA.Serial_Number=E04_E05_E06.Serial_Number
LEFT JOIN AIGO2.dbo.E07 ON MA.Serial_Number=E07.Serial_Number
LEFT JOIN AIGO2.dbo.E08 ON MA.Serial_Number=E08.Serial_Number
LEFT JOIN AIGO2.dbo.E09 ON MA.Serial_Number=E09.Serial_Number
LEFT JOIN AIGO2.dbo.E10 ON MA.Serial_Number=E10.Serial_Number
LEFT JOIN AIGO2.dbo.E11 ON MA.Serial_Number=E11.Serial_Number
--第四階段開發
LEFT JOIN AIGO2.dbo.B25 ON MA.Serial_Number=B25.Serial_Number
LEFT JOIN AIGO2.dbo.B26_B27 ON MA.Serial_Number=B26_B27.Serial_Number
LEFT JOIN AIGO2.dbo.B28 ON MA.Serial_Number=B28.Serial_Number
LEFT JOIN AIGO2.dbo.C28_C30_C31 ON MA.Serial_Number=C28_C30_C31.Serial_Number
LEFT JOIN AIGO2.dbo.C29 ON MA.Serial_Number=C29.Serial_Number;







SELECT *
FROM AIGO2.dbo.final_feature_table;

