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
        
        DECLARE @Today date = getdate()
        EXEC PlanRC.dbo.VZ_msp_get_revenues @Today
    
    '''
    return query


def if_exists():
    query = '''

        declare @Today date =  getdate()
        declare @Monday date = (select dateadd(day, 1-datepart(weekday, @Today), @Today))

        delete from PlanRC.dbo.VZ_MSP_Dashboard_CommodityGroupFilial
        where modifiedDate = @Monday
        
        delete from PlanRC.dbo.VZ_MSP_Dashboard_CommodityGroup
        where modifiedDate = @Monday
        
        delete from PlanRC.dbo.VZ_MSP_Dashboard_Total_Filial
        where modifiedDate = @Monday
        
        delete from PlanRC.dbo.VZ_MSP_Dashboard_Total
        where modifiedDate = @Monday

    '''
    cursor.execute(query)
    cursor.commit()

def successful_update():
    query = '''
        DECLARE @to nvarchar(max) = 'v.zubriichuk@fozzy.ua',

        @body nvarchar(max) = '
        Задание: Расчет эффекта и статистического решения.
        Статус: Обновление выполнено успешно.\n 
        Автоматическое уведомление.\n'
        
        EXEC msdb.dbo.sp_send_dbmail
        @recipients = @to,
        @subject = '(Автоотчет) Ella Pilot MSP',
        @body = @body
    '''
    cursor.execute(query)
    cursor.commit()


def error_update():
    query = '''
        DECLARE @to nvarchar(max) = 'v.zubriichuk@fozzy.ua',

        @body nvarchar(max) = '
        Задание: Расчет эффекта и статистического решения.
        Статус: Обновление не было выполнено.\n 
        Автоматическое уведомление.\n'

        EXEC msdb.dbo.sp_send_dbmail
        @recipients = @to,
        @subject = '(Автоотчет) Ella Pilot MSP',
        @body = @body
    '''
    cursor.execute(query)
    cursor.commit()