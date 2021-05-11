SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[VZ_msp_get_revenues] (@Date DATE)
AS
BEGIN

     SET NOCOUNT ON;

     -- Понедельник - крайний день диапазона дат
     DECLARE @Monday DATE = (SELECT DATEADD(DAY, 1-DATEPART(WEEKDAY, @Date), @Date))

     IF OBJECT_ID('tempDB..#Lagers', 'U') IS NOT NULL
     DROP TABLE #Lagers

     CREATE TABLE #Lagers (
               lagerid INT
          ,    lagerSapClassifierId INT
          ,    commodityGroupId INT
          ,    commodityGroupName NVARCHAR(50)
     )
   
     DECLARE @query NVARCHAR(MAX)
     SET @query = N'

     SELECT 
               lagerId
          ,	lc.lagerSapClassifierId
          ,	lc.commodityGroupId
          ,    lg.commodityGroupName
     FROM MasterData.sku.lagers l WITH (nolock)
          JOIN MasterData.[dbo].[LinkCommodityGroupLagerSapClassifier] lc (nolock)
               ON l.lagerClassifier = lc.lagerSapClassifierId
          JOIN MasterData.dbo.ListAssortmentCommodityGroups lg (nolock)
               ON lc.commodityGroupId = lg.commodityGroupId
     WHERE l.lagerQuality = 0
          AND lc.disableRow = 0
          AND lg.businessId = 1                 --Сильпо
          AND lg.commodityGroupId in (5550015, 5550017, 5550022, 5550050, 5550053, 5550077, 5550120) ' 

     -- Вторая волна
     IF @Date > '20210412'
     SET @query += 'OR lg.commodityGroupId in (5550002, 5550106, 5550014, 5550005, 5550121, 5550930, 5550008)'

     -- Третья волна
     IF @Date > '20990101'
     SET @query += 'OR lg.commodityGroupId in (5550118, 5550150, 5550151, 5550499)              '

     INSERT INTO #Lagers
     EXEC sp_executesql @query

     SELECT	
               filid
          ,	createddate
          ,    commodityGroupId
          ,    sum_revenues	=	sum(priceout*kolvo)
          ,	sum_kolvo		=	sum(kolvo)
     FROM Cheques.dbo.view_ChequeLines cl with (nolock)
          JOIN #Lagers l
               ON l.LagerId = cl.LagerID
     WHERE created BETWEEN '20210308' AND @Monday 
          AND filid IN (	
               2382, 2129, 2236, 2251, 2085, 2056, 2045, 2025, 1934,
               1995, 2042, 2145, 2183, 2173, 2197, 2009, 2132, 2071, 2051, 2275, 2121,
               2261, 2144, 2192, 2107, 2123, 2105, 2087, 2273, 2247, 2226, 2149, 2232, 2052, 1998, 2256, 2187, 2031, 2122, 2268, 2015,
               2093, 2039, 2043, 2117, 2161, 2029, 2053, 2006, 2127, 2000, 2177, 2010, 2044, 2254, 2086, 2237, 2002, 2290, 2088, 2069,
               2218, 2092, 1997, 2028, 2023, 2146, 2231, 2106, 2216, 2250, 2128, 2154, 1935, 2022, 2266, 2036, 2260, 2116, 2124, 2001,
               2035, 1993, 2159, 2220, 1994, 2046, 2223, 2245, 2017, 2114, 2257, 2018, 2032, 2227, 2230, 2055, 2120, 2057, 2189, 2277,
               2238, 1996, 2047, 2252, 2267, 2246, 2011, 1990, 2240, 2125, 2160, 2004, 2118, 2112, 2013, 2184, 2049, 2038, 2005, 2064,
               2073, 2012, 1999, 2225, 2034, 2221, 2090, 2115, 2233, 2195, 2048, 2014, 2062, 2262, 2040, 2060, 2037, 2191, 2126, 2016,
               2253, 2234, 2176, 2186, 2054, 2156, 2185, 2258, 2259, 2041, 2026, 1992, 2130, 2059, 2133, 2196, 2222, 2242, 2224, 2131,
               2153, 2151, 2027, 2019, 2008, 2113, 2219, 2077, 2078, 2188, 2108, 2134, 1991, 2021, 2143, 2020, 2279, 2070, 2270, 2119,
               2291, 2239, 2063, 2024, 2281, 2269, 2265, 2217, 2091, 2007, 2198, 2244, 2241, 2248, 2050, 2190, 2058, 2214, 2061, 2030,
               2201, 2157, 2170, 2194, 2171, 2158, 2147, 2213, 2148, 2155, 2152, 2199, 2215, 2276, 2212, 2274, 2243, 2150, 2280, 2264,
               2072, 2033, 2229
          )
     GROUP BY FilID, CreatedDate, commodityGroupId





END
GO
