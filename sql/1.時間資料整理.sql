/****** SSMS 中 SelectTopNRows 命令的指令碼  ******/
SELECT [ID_NUMBER]
      ,CAST(CONVERT(varchar(100), [DT_GETTIME], 120) AS DATETIME) AS DT_GETTIME
      ,CAST(CONVERT(varchar(100), [DT_INTIME_ORI], 120) AS DATETIME) AS DT_INTIME_ORI
      ,CAST(CONVERT(varchar(100), [DT_INTIME], 120) AS DATETIME) AS DT_INTIME
      ,[NQ_PERSON]
      ,[FG_STATUS]
      ,[FG_FILM]
      ,[TAG]
INTO [AIGO2].[dbo].[linein]
FROM [AIGO2].[dbo].[linein_ORI]


/****** SSMS 中 SelectTopNRows 命令的指令碼  ******/
SELECT [Serial_Number]
      ,[Item_Serial]
      ,[Priority_Serial]
      ,[Order_No]
      ,[Product_No]
      ,[Amount]
      ,[Export_Type]
      ,[Handle_No]
      ,CAST(CONVERT(varchar(100), [Order_Time], 120) AS DATETIME) AS Order_Time
      ,CAST(CONVERT(varchar(100), [OrderOut_Time_ORI], 120) AS DATETIME) AS OrderOut_Time_ORI
      ,CAST(CONVERT(varchar(100), [OrderOut_Time], 120) AS DATETIME) AS OrderOut_Time
      ,CAST(CONVERT(varchar(100), [Expect_Send_Time], 120) AS DATETIME) AS Expect_Send_Time
      ,CAST(CONVERT(varchar(100), [Edit_Time], 120) AS DATETIME) AS Edit_Time
      ,[CookRoom_No]
      ,[CanHurry_Flag]
      ,[CanLock_Flag]
      ,[Estimate_Time]
INTO AIGO2.dbo.order_achievement
FROM [AIGO2].[dbo].[order_achievement_ori]
