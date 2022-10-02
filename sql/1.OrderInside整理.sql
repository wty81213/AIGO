SELECT [Serial_Number]
      ,[Order_Type]
      ,[Nation_Code]
      ,CAST(REPLACE([Adult_Count],'.0','') AS INT) AS Adult_Count
      ,CAST(REPLACE([Kid_Count],'.0','') AS INT) AS Kid_Count
      ,[Team_Flag]
      ,[Unhandy_Flag]
      ,[AddOrder_Times]
      ,[Queue_No]
      ,[Table_No]
      ,[Table_SubNo]
      ,[TakeOut_Flag]
      ,[Queue_Time]
      ,[Enter_Time]
      ,CAST([First_Order_Time] AS datetime) AS First_Order_Time
      ,CAST([First_AddOrder_Time] AS datetime) AS First_AddOrder_Time
      ,CAST([Final_AddOrder_Time] AS datetime) AS Final_AddOrder_Time
      ,CAST([First_Dish_Time] AS datetime) AS First_Dish_Time
      ,CAST([Final_Dish_Time] AS datetime) AS Final_Dish_Time
      ,CAST([WalkOut_Time] AS datetime) AS WalkOut_Time
      ,CAST([Edit_Time] AS datetime) AS Edit_Time
      ,[Nation_People]
      ,[Total_Count]
      ,[Wait_Time]
      ,[Meal_Count]
  INTO AIGO.dbo.orderinside_tp
  FROM [AIGO].[dbo].[orderinside];

  IF OBJECT_ID('AIGO.dbo.orderinside') IS NOT NULL DROP TABLE AIGO.dbo.orderinside;
  SELECT *
  INTO AIGO.dbo.orderinside
  FROM AIGO.dbo.orderinside_tp;

  IF OBJECT_ID('AIGO.dbo.orderinside_TP') IS NOT NULL DROP TABLE AIGO.dbo.orderinside_TP;

