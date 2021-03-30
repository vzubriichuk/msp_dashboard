#!/usr/bin/env python
# coding:utf-8
"""
Author : Vitaliy Zubriichuk
Contact : v@zubr.kiev.ua
Time    : 25.03.2021 16:27
"""
import sqlalchemy as sql
import pyodbc

server = 'S-KV-CENTER-S64'
driver = 'SQL+Server'
db = 'PlanRC'
engine = sql.create_engine('mssql+pyodbc://{}/{}?driver={}'.format(server, db, driver))
connection = engine.raw_connection()
cursor = connection.cursor()


def get_revenues():
    query = '''


        declare @Today date = getdate()
        declare @Monday date = (select dateadd(day, 1-datepart(weekday, @Today), @Today))

        ;with Lagers as (
        select 
                lagerId
            ,	lc.lagerSapClassifierId
            ,	lc.commodityGroupId
            ,	lg.commodityGroupName
        from MasterData.sku.lagers l with (nolock)
            join MasterData.[dbo].[LinkCommodityGroupLagerSapClassifier] lc (nolock)
                on l.lagerClassifier =  lc.lagerSapClassifierId
            join MasterData.dbo.ListAssortmentCommodityGroups lg (nolock)
                on lc.commodityGroupId=lg.commodityGroupId
        where l.lagerQuality = 0
            and lc.disableRow = 0
            and lg.businessId = 1                 --Сильпо
            and lg.commodityGroupId in (5550499, 5550053, 5550017, 5550022, 
            5550150, 5550120, 5550151, 5550118,  5550050, 5550077, 5550015)
        )

        select	
                filid
            ,	createddate
            ,   commodityGroupId
            ,	sum_revenues	=	sum(priceout*kolvo)
            ,	sum_kolvo		=	sum(kolvo)
        from Cheques.dbo.view_ChequeLines cl with (nolock)
            join Lagers l
                on l.LagerId = cl.LagerID
        where created between '20210308' and @Monday 
            and filid in (	
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
        group by FilID, CreatedDate, commodityGroupId

           '''
    return query


def if_exists():
    query = '''

        declare @Today date = getdate()
        declare @Monday date = (select dateadd(day, 1-datepart(weekday, @Today), @Today))

        delete from PlanRC.dbo.VZ_MSP_Dashboard_CommodityGroupFilial
        where modifiedDate = @Monday
        
        delete from PlanRC.dbo.VZ_MSP_Dashboard_CommodityGroup
        where modifiedDate = @Monday
        
        delete from PlanRC.dbo.VZ_MSP_Dashboard_Total
        where modifiedDate = @Monday

    '''
    cursor.execute(query)
    cursor.commit()


