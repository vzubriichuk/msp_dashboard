#!/usr/bin/env python
# coding:utf-8
"""
Author : Vitaliy Zubriichuk
Contact : v@zubr.kiev.ua
Time    : 25.03.2021 16:32
"""
from scipy.stats import mannwhitneyu, wilcoxon
from datetime import date, datetime
from zlib import decompress
from zlib import compress

import db_connect_sql as conn
import numpy as np
import pandas as pd
import time
import xlrd

# Path to folder for open weight_shops_msp.xlsx
path = b"x\x9c%\xca1\n\xc20\x14\x06\xe0\x1by\x07\xa1\xc1.\x85`\xd6\x1fJH\xa3\x06\x9ey\xa5I\x87vk\x8f\xe0\xea%\n.E\xc13\xbc\xdeH\xc5\xf5\xe3\x03N<\x8e\xc3\x8elD\xc1\xae\xbf\xfa\x98\x13J\xa6&\xc43\n\xdf\xda.\xff\xcd\x04j\x19r\x97\xb7<d\xdd\xa6m\x96U\x9e\xb2\xd4\x07u\xac\x94A\xe9m\x83}\xb4\x14rp\x90\xdb\xf7\xcc\xb2\xc8\xeb\xb7\xa0\x87|\xe1\x08Edk\x1d\x88s]\x19\x8d\xce'\xee;\xe7\xd3\x07l:?<"
path_to_folder = decompress(path).decode()

start_script_time = datetime.now()

# Список филиалов на которых проводился пилот
pg_filials = [1934, 2022, 2031, 2069, 2112, 2120, 2131, 2254, 2382, 2028, 1999]

# Выгрузка данных по чекам за период
print('1 of 5. Start load revenues')
# sales = pd.read_csv('sales.csv')
sales = pd.read_sql_query(conn.get_revenues(), conn.engine)
commodity_groups = sales['commodityGroupId'].unique()
date_update = pd.to_datetime(sales['createddate'].max()) + pd.Timedelta(days=1)

print('2 of 5. End load revenues')

print('3 of 5. Data processing')

# Датафреймы
result_tg_fil = pd.DataFrame(
    columns=['commodityGroupId', 'Filid', 'EffectValue', 'DecisionValue',
             'modifiedDate'])
result_tg = pd.DataFrame(
    columns=['commodityGroupId', 'EffectValue', 'DecisionValue',
             'modifiedDate'])
result_total = pd.DataFrame(
    columns=['EffectValue', 'DecisionValue', 'modifiedDate'])

# Получение весов
weights = pd.read_excel(path_to_folder + '\\weights_shops_msp.xlsx').set_index('index')
print(weights)

pg_total_revenues = []
cg_total_revenues = []

# Рекурсия по товарным группам
for tg in commodity_groups:
    data_for_all_filials = sales[sales.commodityGroupId == tg].copy(deep=True)
    data_for_all_filials['createddate'] = pd.to_datetime(
        data_for_all_filials['createddate'])

    start_pilot = pd.to_datetime('2021-03-08')
    pilot = data_for_all_filials[
        data_for_all_filials['createddate'] >= start_pilot]

    end_pilot = pilot.createddate.max()
    time_series_pilot = pilot.groupby(['filid', 'createddate']).sum()

    filials = np.unique(data_for_all_filials.filid)
    candidate_for_cg_filials = filials[~np.isin(filials, pg_filials)]

    time_series_pivoted = pd.DataFrame(
        columns=['dates_pilot', 'time_series_pilot'])

    for filid in filials:
        y = time_series_pilot.loc[filid]['sum_revenues']
        y = pd.DataFrame(pd.date_range(start=start_pilot, end=end_pilot),
                         columns=['createddate']).merge(y.reset_index(),
                                                        how='left').set_index(
            'createddate')['sum_revenues'].fillna(0)

        time_series_pivoted.loc[filid, 'time_series_pilot'] = np.array(y)
        time_series_pivoted.loc[filid, 'dates_pilot'] = np.array(y.index)

    weights_commodity_group = weights[weights.commodity_group == tg].fillna(
        0).to_dict()

    # Расчет эффектов
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

        # Подсчет эффекта и статистического решения на уровне ТГ-Филиал
        effect = (pg_pilot.sum() / cg_pilot.sum()) - 1
        _, p_value = mannwhitneyu(cg_pilot, pg_pilot,
                                  use_continuity=False, alternative='greater')

        df = pd.DataFrame([[tg, pg_filial, effect, p_value, date_update]],
                          columns=['commodityGroupId', 'Filid', 'EffectValue',
                                   'DecisionValue', 'modifiedDate'])
        result_tg_fil = result_tg_fil.append(df, ignore_index=True,
                                             verify_integrity=False,
                                             sort=None)

        # Аккумулируем дневную выручку на уровне ТГ
        pg_daily_revenues.append(pg_pilot)
        cg_daily_revenues.append(cg_pilot)

    # Аккумулируем дневную выручку на уровне всех ТГ
    pg_total_revenues.append(pg_daily_revenues)
    cg_total_revenues.append(cg_daily_revenues)

    pg_daily_revenues = np.array(pg_daily_revenues).flatten()
    cg_daily_revenues = np.array(cg_daily_revenues).flatten()

    # Подсчет эффекта и статистического решения на уровне ТГ
    effect = (pg_daily_revenues.sum() / cg_daily_revenues.sum()) - 1
    _, p_value = mannwhitneyu(cg_daily_revenues, pg_daily_revenues,
                              use_continuity=False, alternative='greater')

    df = pd.DataFrame([[tg, effect, p_value, date_update]],
                      columns=['commodityGroupId', 'EffectValue',
                               'DecisionValue', 'modifiedDate'])
    result_tg = result_tg.append(df, ignore_index=True, verify_integrity=False,
                                 sort=None)

pg_total_revenues = np.array(pg_total_revenues).flatten()
cg_total_revenues = np.array(cg_total_revenues).flatten()

# Подсчет эффекта и статистического решения на уровне всех ТГ
effect = (pg_total_revenues.sum() / cg_total_revenues.sum()) - 1
_, p_value = mannwhitneyu(cg_total_revenues, pg_total_revenues,
                          use_continuity=False, alternative='greater')

result_total = pd.DataFrame([[effect, p_value, date_update]],
                            columns=['EffectValue', 'DecisionValue',
                                     'modifiedDate'])




print('4 of 5. Upload data to server')

# Результирующие таблицы
result_tg_fil_name = 'VZ_MSP_Dashboard_CommodityGroupFilial'
result_tg_name = 'VZ_MSP_Dashboard_CommodityGroup'
result_total_name = 'VZ_MSP_Dashboard_Total'

# Delete if data exists
conn.if_exists()

# Happy end letter
conn.successful_update()

# Upload
result_tg_fil.to_sql(name=result_tg_fil_name, con=conn.engine,
                     if_exists='append', index=False,
                     schema='dbo')
result_tg.to_sql(name=result_tg_name, con=conn.engine, if_exists='append',
                 index=False,
                 schema='dbo')

result_total.to_sql(name=result_total_name, con=conn.engine, if_exists='append',
                    index=False,
                    schema='dbo')
time.sleep(5)
print('5 of 5. End scripts')


end_script_time = datetime.now()
delta = end_script_time - start_script_time

time.sleep(5)
print('\n' 'Total time duration: ' + str(delta.seconds) + ' seconds')
time.sleep(5)

