--主表
IF OBJECT_ID('AIGO.dbo.BASE_TABLE') IS NOT NULL DROP TABLE AIGO.dbo.BASE_TABLE;
SELECT Serial_Number,Queue_Time,Adult_Count,Kid_Count
INTO AIGO.dbo.BASE_TABLE
FROM [AIGO].[dbo].[OrderInside]
WHERE Queue_Time IS NOT NULL;


/*第一階段開發*/
--B09等候中的組數中平均的已等候時間(分鐘)
IF OBJECT_ID('AIGO.dbo.B09') IS NOT NULL DROP TABLE AIGO.dbo.B09;
WITH V1 AS (
SELECT MA.Serial_Number
      ,SUM(CASE WHEN J1.DT_GETTIME IS NULL THEN 0 ELSE DATEDIFF(MINUTE,J1.DT_GETTIME,MA.Queue_Time) END) AS B091_WAITTIME_SUM
	  ,MAX(CASE WHEN J1.DT_GETTIME IS NULL THEN 0 ELSE DATEDIFF(MINUTE,J1.DT_GETTIME,MA.Queue_Time) END) AS B092_WAITTIME_MAX
	  ,MIN(CASE WHEN J1.DT_GETTIME IS NULL THEN 0 ELSE DATEDIFF(MINUTE,J1.DT_GETTIME,MA.Queue_Time) END) AS B093_WAITTIME_MIN
      ,AVG(CASE WHEN J1.DT_GETTIME IS NULL THEN 0 ELSE DATEDIFF(MINUTE,J1.DT_GETTIME,MA.Queue_Time) END) AS B094_WAITTIME_AVG
FROM AIGO.dbo.BASE_TABLE AS MA
INNER JOIN AIGO.dbo.Linein_D AS J1 ON MA.Queue_Time > J1.DT_GETTIME AND MA.Queue_Time < J1.DT_INTIME
GROUP BY MA.Serial_Number
)
,V2 AS (
SELECT MA.Serial_Number
      ,SUM(CASE WHEN J1.DT_GETTIME IS NULL THEN 0 ELSE DATEDIFF(MINUTE,J1.DT_GETTIME,MA.Queue_Time) END) AS B095_WAITTIME_SUM_CONDITION
	  ,MAX(CASE WHEN J1.DT_GETTIME IS NULL THEN 0 ELSE DATEDIFF(MINUTE,J1.DT_GETTIME,MA.Queue_Time) END) AS B096_WAITTIME_MAX_CONDITION
	  ,MIN(CASE WHEN J1.DT_GETTIME IS NULL THEN 0 ELSE DATEDIFF(MINUTE,J1.DT_GETTIME,MA.Queue_Time) END) AS B097_WAITTIME_MIN_CONDITION
      ,AVG(CASE WHEN J1.DT_GETTIME IS NULL THEN 0 ELSE DATEDIFF(MINUTE,J1.DT_GETTIME,MA.Queue_Time) END) AS B098_WAITTIME_AVG_CONDITION
FROM AIGO.dbo.BASE_TABLE AS MA
INNER JOIN AIGO.dbo.Linein_D AS J1 ON MA.Queue_Time > J1.DT_GETTIME AND MA.Queue_Time < J1.DT_INTIME AND J1.NQ_PERSON >= (MA.Adult_Count+MA.Kid_Count)
GROUP BY MA.Serial_Number
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
INTO AIGO.dbo.B09
FROM AIGO.dbo.BASE_TABLE AS MA
LEFT JOIN V1 ON MA.Serial_Number=V1.Serial_Number
LEFT JOIN V2 ON MA.Serial_Number=V2.Serial_Number;


--B10前一組入店用餐時間距本次開始排隊的時間(分鐘)
IF OBJECT_ID('AIGO.dbo.B10') IS NOT NULL DROP TABLE AIGO.dbo.B10;
SELECT MA.Serial_Number,MIN(DATEDIFF(MINUTE,J1.Enter_Time,MA.Queue_Time)) AS B101_DIFF_ENTERTIME
INTO AIGO.dbo.B10
FROM AIGO.dbo.BASE_TABLE AS MA
INNER JOIN AIGO.dbo.OrderInside AS J1 ON MA.Queue_Time > J1.Enter_Time AND CAST(MA.Queue_Time AS DATE) = CAST(J1.Enter_Time AS DATE)
GROUP BY MA.Serial_Number;

--B11前一組開始排隊距離本次開始排隊的時間(分鐘)
IF OBJECT_ID('AIGO.dbo.B11') IS NOT NULL DROP TABLE AIGO.dbo.B11;
SELECT MA.Serial_Number,MIN(DATEDIFF(MINUTE,J1.Queue_Time,MA.Queue_Time)) AS B111_DIFF_QUEUETIME
INTO AIGO.dbo.B11
FROM AIGO.dbo.BASE_TABLE AS MA
INNER JOIN AIGO.dbo.OrderInside AS J1 ON MA.Queue_Time > J1.Queue_Time AND CAST(MA.Queue_Time AS DATE) = CAST(J1.Queue_Time AS DATE)
GROUP BY MA.Serial_Number;


--B12前一組離席時間距本次開始排隊的時間(分鐘)
IF OBJECT_ID('AIGO.dbo.B12') IS NOT NULL DROP TABLE AIGO.dbo.B12;
WITH V1 AS (
SELECT Serial_Number,CASE WHEN WalkOut_Time > Edit_Time THEN WalkOut_Time ELSE Edit_Time END FINAL_TIME
FROM AIGO.dbo.OrderInside
)
SELECT MA.Serial_Number
      ,MIN(DATEDIFF(MINUTE,V1.FINAL_TIME,MA.Queue_Time)) AS B121_DIFF_LEAVETIME
INTO AIGO.dbo.B12
FROM AIGO.dbo.BASE_TABLE AS MA
INNER JOIN V1 ON  MA.Queue_Time > V1.FINAL_TIME AND CAST(MA.Queue_Time AS DATE) = CAST(V1.FINAL_TIME AS DATE) 
GROUP BY MA.Serial_Number;


--B13與前一組入店用餐的組相比，兩者的開始等候的相距時間(分鐘)
IF OBJECT_ID('AIGO.dbo.B13') IS NOT NULL DROP TABLE AIGO.dbo.B13;
WITH V1 AS (
SELECT MA.Serial_Number
      ,MA.Queue_Time
	  ,DENSE_RANK() OVER (PARTITION BY MA.Serial_Number ORDER BY J1.Enter_Time DESC, J1.Serial_Number DESC) AS RID
	  ,J1.Serial_Number AS Serial_Number_Before
FROM AIGO.dbo.BASE_TABLE AS MA
INNER JOIN AIGO.dbo.OrderInside AS J1 ON MA.Queue_Time > J1.Enter_Time AND CAST(MA.Queue_Time AS DATE) = CAST(J1.Enter_Time AS DATE)
)
SELECT MA.Serial_Number,DATEDIFF(MINUTE,J1.Enter_Time,MA.Queue_Time) AS B131_DIFF_QueueTime
INTO AIGO.dbo.B13
FROM (SELECT Serial_Number,Queue_Time,Serial_Number_Before FROM V1 WHERE RID = 1) MA
INNER JOIN AIGO.dbo.OrderInside AS J1 ON MA.Serial_Number_Before = J1.Serial_Number;


--B14近5分鐘是否皆排隊1分鐘以上
IF OBJECT_ID('AIGO.dbo.B14') IS NOT NULL DROP TABLE AIGO.dbo.B14;
WITH V1_1 AS (
SELECT MA.Serial_Number
      ,DATEDIFF(MINUTE,J1.DT_GETTIME,J1.DT_INTIME) AS Queue_minute
	  ,'5min' AS Tag
FROM AIGO.dbo.BASE_TABLE AS MA
INNER JOIN AIGO.dbo.Linein_D AS J1 ON MA.Queue_Time > J1.DT_GETTIME AND DATEADD(MINUTE,-5,MA.Queue_Time) < J1.DT_GETTIME
)
,V1_2 AS (
SELECT MA.Serial_Number
      ,DATEDIFF(MINUTE,J1.DT_GETTIME,J1.DT_INTIME) AS Queue_minute
	  ,'10min' AS Tag
FROM AIGO.dbo.BASE_TABLE AS MA
INNER JOIN AIGO.dbo.Linein_D AS J1 ON MA.Queue_Time > J1.DT_GETTIME AND DATEADD(MINUTE,-10,MA.Queue_Time) < J1.DT_GETTIME
)
,V1_3 AS (
SELECT MA.Serial_Number
      ,DATEDIFF(MINUTE,J1.DT_GETTIME,J1.DT_INTIME) AS Queue_minute
	  ,'15min' AS Tag
FROM AIGO.dbo.BASE_TABLE AS MA
INNER JOIN AIGO.dbo.Linein_D AS J1 ON MA.Queue_Time > J1.DT_GETTIME AND DATEADD(MINUTE,-15,MA.Queue_Time) < J1.DT_GETTIME
)
,V2_1 AS (
SELECT Serial_Number
      ,MIN(CASE WHEN Queue_minute > 1 OR Queue_minute IS NULL THEN 'Y' ELSE 'N' END) AS B1401_5min_1minup_wait
	  ,MIN(CASE WHEN Queue_minute > 3 OR Queue_minute IS NULL THEN 'Y' ELSE 'N' END) AS B1402_5min_3minup_wait
	  ,MIN(CASE WHEN Queue_minute > 5 OR Queue_minute IS NULL THEN 'Y' ELSE 'N' END) AS B1403_5min_5minup_wait
	  ,MIN(CASE WHEN Queue_minute > 10 OR Queue_minute IS NULL THEN 'Y' ELSE 'N' END) AS B1404_5min_10minup_wait
FROM V1_1
GROUP BY Serial_Number
)
,V2_2 AS (
SELECT Serial_Number
      ,MIN(CASE WHEN Queue_minute > 1 OR Queue_minute IS NULL THEN 'Y' ELSE 'N' END) AS B1405_10min_1minup_wait
	  ,MIN(CASE WHEN Queue_minute > 3 OR Queue_minute IS NULL THEN 'Y' ELSE 'N' END) AS B1406_10min_3minup_wait
	  ,MIN(CASE WHEN Queue_minute > 5 OR Queue_minute IS NULL THEN 'Y' ELSE 'N' END) AS B1407_10min_5minup_wait
	  ,MIN(CASE WHEN Queue_minute > 10 OR Queue_minute IS NULL THEN 'Y' ELSE 'N' END) AS B1408_10min_10minup_wait
FROM V1_2
GROUP BY Serial_Number
)
,V2_3 AS (
SELECT Serial_Number
      ,MIN(CASE WHEN Queue_minute > 1 OR Queue_minute IS NULL THEN 'Y' ELSE 'N' END) AS B1409_15min_1minup_wait
	  ,MIN(CASE WHEN Queue_minute > 3 OR Queue_minute IS NULL THEN 'Y' ELSE 'N' END) AS B1410_15min_3minup_wait
	  ,MIN(CASE WHEN Queue_minute > 5 OR Queue_minute IS NULL THEN 'Y' ELSE 'N' END) AS B1411_15min_5minup_wait
	  ,MIN(CASE WHEN Queue_minute > 10 OR Queue_minute IS NULL THEN 'Y' ELSE 'N' END) AS B1412_15min_10minup_wait
FROM V1_3
GROUP BY Serial_Number
)
SELECT MA.Serial_Number
      ,B1401_5min_1minup_wait
	  ,B1402_5min_3minup_wait
	  ,B1403_5min_5minup_wait
	  ,B1404_5min_10minup_wait
	  ,B1405_10min_1minup_wait
	  ,B1406_10min_3minup_wait
	  ,B1407_10min_5minup_wait
	  ,B1408_10min_10minup_wait
	  ,B1409_15min_1minup_wait
	  ,B1410_15min_3minup_wait
	  ,B1411_15min_5minup_wait
	  ,B1412_15min_10minup_wait
INTO AIGO.dbo.B14
FROM AIGO.dbo.BASE_TABLE AS MA
LEFT JOIN V2_1 AS J1 ON MA.Serial_Number=J1.Serial_Number
LEFT JOIN V2_2 AS J2 ON MA.Serial_Number=J2.Serial_Number
LEFT JOIN V2_3 AS J3 ON MA.Serial_Number=J3.Serial_Number;



--C01用餐中的客戶組數
--C02用餐中的平均每組人數
--C03用餐中的組數有外國人的比例
IF OBJECT_ID('AIGO.dbo.C01_C02_C03') IS NOT NULL DROP TABLE AIGO.dbo.C01_C02_C03;
WITH V1 AS (
SELECT Serial_Number
      ,Enter_Time
      ,CASE WHEN WalkOut_Time > Edit_Time THEN WalkOut_Time ELSE Edit_Time END FINAL_TIME
	  ,CASE WHEN Nation_Code = '1' OR Nation_Code IS NULL THEN 'native' ELSE '' END AS Native_Tag
	  ,Adult_Count
	  ,Kid_Count
	  ,Adult_Count + Kid_Count AS ALLCUS
FROM AIGO.dbo.OrderInside
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
INTO AIGO.dbo.C01_C02_C03
FROM AIGO.dbo.BASE_TABLE AS MA
INNER JOIN V1 AS J1 ON MA.Queue_Time > J1.Enter_Time AND MA.Queue_Time < J1.FINAL_TIME
GROUP BY MA.Serial_Number;






/*總表*/
IF OBJECT_ID('AIGO.dbo.final_feature_table') IS NOT NULL DROP TABLE AIGO.dbo.final_feature_table;
SELECT MA.Serial_Number
      ,B09.B091_WAITTIME_SUM
	  ,B09.B092_WAITTIME_MAX
	  ,B09.B093_WAITTIME_MIN
	  ,B09.B094_WAITTIME_AVG
	  ,B09.B095_WAITTIME_SUM_CONDITION
	  ,B09.B096_WAITTIME_MAX_CONDITION
	  ,B09.B097_WAITTIME_MIN_CONDITION
	  ,B09.B098_WAITTIME_AVG_CONDITION
	  ,B10.B101_DIFF_ENTERTIME
	  ,B11.B111_DIFF_QUEUETIME
	  ,B12.B121_DIFF_LEAVETIME
	  ,B13.B131_DIFF_QueueTime
	  ,B14.B1401_5min_1minup_wait
	  ,B14.B1402_5min_3minup_wait
	  ,B14.B1403_5min_5minup_wait
	  ,B14.B1404_5min_10minup_wait
	  ,B14.B1405_10min_1minup_wait
	  ,B14.B1406_10min_3minup_wait
	  ,B14.B1407_10min_5minup_wait
	  ,B14.B1408_10min_10minup_wait
	  ,B14.B1409_15min_1minup_wait
	  ,B14.B1410_15min_3minup_wait
	  ,B14.B1411_15min_5minup_wait
	  ,B14.B1412_15min_10minup_wait
	  ,C01_C02_C03.C011_ING_Serial_Number
	  ,C01_C02_C03.C012_ING_TTLCUS_NUMBER
	  ,C01_C02_C03.C013_ING_Adult_NUMBER
	  ,C01_C02_C03.C014_ING_Kid_NUMBER
	  ,C01_C02_C03.C021_ING_Serial_Number
	  ,C01_C02_C03.C022_ING_CUS_AVG
	  ,C01_C02_C03.C023_ING_CUS_MAX
	  ,C01_C02_C03.C024_ING_CUS_MIN
	  ,C01_C02_C03.C031_ING_FOREIGNER_PERCENT
INTO AIGO.dbo.final_feature_table
FROM AIGO.dbo.BASE_TABLE AS MA
LEFT JOIN AIGO.dbo.B09 ON MA.Serial_Number=B09.Serial_Number
LEFT JOIN AIGO.dbo.B10 ON MA.Serial_Number=B10.Serial_Number
LEFT JOIN AIGO.dbo.B11 ON MA.Serial_Number=B11.Serial_Number
LEFT JOIN AIGO.dbo.B12 ON MA.Serial_Number=B12.Serial_Number
LEFT JOIN AIGO.dbo.B13 ON MA.Serial_Number=B13.Serial_Number
LEFT JOIN AIGO.dbo.B14 ON MA.Serial_Number=B14.Serial_Number
LEFT JOIN AIGO.dbo.C01_C02_C03 ON MA.Serial_Number=C01_C02_C03.Serial_Number;


