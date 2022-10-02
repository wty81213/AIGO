IF OBJECT_ID('AIGO2.dbo.order_achievement') IS NOT NULL DROP TABLE AIGO2.dbo.order_achievement;
SELECT [Serial_Number]
      ,[Item_Serial]
      ,[Priority_Serial]
      ,[Order_No]
      ,[Product_No]
      ,[Amount]
      ,[Export_Type]
      ,[Handle_No]
      ,[Order_Time]
      ,[OrderOut_Time] AS OrderOut_Time_ORI
	  ,CASE WHEN OrderOut_Time < Order_Time THEN Order_Time ELSE OrderOut_Time END AS OrderOut_Time
      ,[Expect_Send_Time]
      ,[Edit_Time]
      ,[CookRoom_No]
      ,[CanHurry_Flag]
      ,[CanLock_Flag]
      ,[Estimate_Time]
INTO AIGO2.dbo.order_achievement
FROM [AIGO2].[dbo].[order_achievement_tp]