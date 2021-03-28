#!/usr/bin/env python
# coding:utf-8
"""
Author : Vitaliy Zubriichuk
Contact : v@zubr.kiev.ua
Time    : 25.03.2021 16:32
"""
from scipy.stats import mannwhitneyu, wilcoxon
from datetime import date, datetime
import db_connect_sql as conn
import numpy as np
import pandas as pd
import xlrd

# Список филиалов на которых проводился пилот
pg_filials = [1934, 2022, 2031, 2069, 2112, 2120, 2131, 2254, 2382, 2028, 1999]
commodity_groups = [5550499, 5550053, 5550017, 5550022, 5550150, 5550120,
                    5550151, 5550118, 5550050, 5550077, 5550015]

# Выгрузка данных по чекам за период
print('1 of 5. Start load revenues')
sales = pd.read_sql_query(conn.get_revenues(), conn.engine)
# sales = pd.read_csv('sales_5550499_commodity_group.csv')
# print(sales['createddate'].unique())
print('2 of 5. End load revenues')

data_for_all_filials = sales.copy(deep=True)
data_for_all_filials['createddate'] = pd.to_datetime(
    data_for_all_filials['createddate'])

start_pilot = pd.to_datetime('2021-03-08')
pilot = data_for_all_filials[data_for_all_filials['createddate'] >= start_pilot]

end_pilot = pilot.createddate.max()
time_series_pilot = pilot.groupby(['filid', 'createddate']).sum()

filials = np.unique(data_for_all_filials.filid)
candidate_for_cg_filials = filials[~np.isin(filials, pg_filials)]

time_series_pivoted = pd.DataFrame(columns=['dates_pilot', 'time_series_pilot'])

for filid in filials:
    y = time_series_pilot.loc[filid]['sum_revenues']
    y = pd.DataFrame(pd.date_range(start=start_pilot, end=end_pilot),
                     columns=['createddate']).merge(y.reset_index(),
                                                    how='left').set_index(
        'createddate')['sum_revenues'].fillna(0)

    time_series_pivoted.loc[filid, 'time_series_pilot'] = np.array(y)
    time_series_pivoted.loc[filid, 'dates_pilot'] = np.array(y.index)

# Подготавливаем датафреймы
print('3 of 5. Data processing')
result_tg_fil = pd.DataFrame(
    columns=['commodityGroupId', 'Filid', 'EffectValue', 'DecisionValue',
             'modifiedDate'])
result_tg = pd.DataFrame(
    columns=['commodityGroupId', 'EffectValue', 'DecisionValue',
             'modifiedDate'])
result_total = pd.DataFrame(
    columns=['EffectValue', 'DecisionValue', 'modifiedDate'])

# Получение весов
weights = pd.read_excel('weights_shops_msp.xlsx').set_index('index')

# Эффект на уровне ТГ-Филиал
for tg in commodity_groups:
    weights_commodity_group = weights[weights.commodity_group == tg].fillna(
        0).to_dict()

    # Подсчет выручек для синтетических контрольных магазинов
    pg_daily_revenues = []
    cg_daily_revenues = []

    for pg_filial in pg_filials:
        w = weights_commodity_group[pg_filial]
        values = []
        for k, v in w.items():
            values.append(
                time_series_pivoted['time_series_pilot'].to_dict()[k] * v)

        cg_pilot = np.vstack(values).sum(axis=0)
        pg_pilot = time_series_pivoted['time_series_pilot'].to_dict()[pg_filial]

        pg_daily_revenues.append(pg_pilot)
        cg_daily_revenues.append(cg_pilot)

        # Подсчет эффекта и статистического решения на уровне ТГ-Филиал
        effect = (pg_pilot.sum() / cg_pilot.sum()) - 1
        _, p_value = mannwhitneyu(cg_pilot, pg_pilot,
                                  use_continuity=False, alternative='greater')

        df = pd.DataFrame([[tg, pg_filial, effect, p_value, date.today()]],
                          columns=['commodityGroupId', 'Filid', 'EffectValue',
                                   'DecisionValue', 'modifiedDate'])
        result_tg_fil = result_tg_fil.append(df, ignore_index=True,
                                             verify_integrity=False,
                                             sort=None)

# Эффект на уровне ТГ
for tg in commodity_groups:
    weights_commodity_group = weights[weights.commodity_group == tg].fillna(
        0).to_dict()

    # Подсчет выручек для синтетических контрольных магазинов
    pg_daily_revenues = []
    cg_daily_revenues = []

    for pg_filial in pg_filials:
        w = weights_commodity_group[pg_filial]
        values = []
        for k, v in w.items():
            values.append(
                time_series_pivoted['time_series_pilot'].to_dict()[k] * v)

        cg_pilot = np.vstack(values).sum(axis=0)
        pg_pilot = time_series_pivoted['time_series_pilot'].to_dict()[pg_filial]

        pg_daily_revenues.append(pg_pilot)
        cg_daily_revenues.append(cg_pilot)

    pg_daily_revenues = np.array(pg_daily_revenues).flatten()
    cg_daily_revenues = np.array(cg_daily_revenues).flatten()

    # Подсчет эффекта и статистического решения на уровне ТГ
    effect = (pg_daily_revenues.sum() / cg_daily_revenues.sum()) - 1
    _, p_value = mannwhitneyu(cg_daily_revenues, pg_daily_revenues,
                              use_continuity=False, alternative='greater')

    df = pd.DataFrame([[tg, effect, p_value, date.today()]],
                      columns=['commodityGroupId', 'EffectValue',
                               'DecisionValue', 'modifiedDate'])
    result_tg = result_tg.append(df, ignore_index=True, verify_integrity=False,
                                 sort=None)

# Эффект на уровне всех ТГ
pg_total_revenues = []
cg_total_revenues = []

for tg in commodity_groups:
    weights_commodity_group = weights[weights.commodity_group == tg].fillna(
        0).to_dict()

    # Подсчет выручек для синтетических контрольных магазинов
    pg_daily_revenues = []
    cg_daily_revenues = []

    for pg_filial in pg_filials:
        w = weights_commodity_group[pg_filial]
        values = []
        for k, v in w.items():
            values.append(
                time_series_pivoted['time_series_pilot'].to_dict()[k] * v)

        cg_pilot = np.vstack(values).sum(axis=0)
        pg_pilot = time_series_pivoted['time_series_pilot'].to_dict()[pg_filial]

        pg_daily_revenues.append(pg_pilot)
        cg_daily_revenues.append(cg_pilot)

    pg_total_revenues.append(pg_daily_revenues)
    cg_total_revenues.append(cg_daily_revenues)

pg_total_revenues = np.array(pg_total_revenues).flatten()
cg_total_revenues = np.array(cg_total_revenues).flatten()

# Подсчет эффекта и статистического решения на уровне всех ТГ
effect = (pg_total_revenues.sum() / cg_total_revenues.sum()) - 1
_, p_value = mannwhitneyu(cg_total_revenues, pg_total_revenues,
                          use_continuity=False, alternative='greater')

result_total = pd.DataFrame([[effect, p_value, date.today()]],
                            columns=['EffectValue', 'DecisionValue',
                                     'modifiedDate'])

# print(df.iloc[:1])
#
# print(result_tg_fil)
# print(result_tg)
# print(result_total)

result_tg_fil_name = 'VZ_MSP_Dashboard_CommodityGroupFilial'
result_tg_name = 'VZ_MSP_Dashboard_CommodityGroup'
result_total_name = 'VZ_MSP_Dashboard_Total'


print('4 of 5. Load data to server')
result_tg_fil.to_sql(name=result_tg_fil_name, con=conn.engine,
                     if_exists='append', index=False,
                     schema='dbo')
result_tg.to_sql(name=result_tg_name, con=conn.engine, if_exists='append',
                 index=False,
                 schema='dbo')

result_total.to_sql(name=result_total_name, con=conn.engine, if_exists='append',
                    index=False,
                    schema='dbo')
print('5 of 5. End scripts')
