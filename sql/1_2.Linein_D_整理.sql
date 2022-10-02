IF OBJECT_ID('[AIGO].[dbo].[Linein_D]') IS NOT NULL DROP TABLE [AIGO].[dbo].[Linein_D];
WITH V1 AS (
  SELECT [ID_NUMBER]
      ,CAST([DT_GETTIME] AS datetime) AS [DT_GETTIME]
      ,CAST([DT_INTIME] AS datetime) AS [DT_INTIME]
      ,CAST([NQ_PERSON] AS INT) AS [NQ_PERSON]
      ,[FG_STATUS]
      ,[FG_FILM]
  FROM [AIGO].[dbo].[Linein_D_2016]
  UNION
  SELECT [ID_NUMBER]
      ,CAST([DT_GETTIME] AS datetime) AS [DT_GETTIME]
      ,CASE WHEN [DT_INTIME] = 'NULL' THEN NULL ELSE CAST([DT_INTIME] AS datetime) END AS [DT_INTIME]
      ,CAST([NQ_PERSON] AS INT) AS [NQ_PERSON]
      ,[FG_STATUS]
      ,[FG_FILM]
  FROM [AIGO].[dbo].[Linein_D_2017]
  )
  SELECT *
  INTO [AIGO].[dbo].[Linein_D]
  FROM V1;

  