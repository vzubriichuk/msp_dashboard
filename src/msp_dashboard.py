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
path = b"x\x9c%\xca1\n\xc20\x14\x06\xe0\x1by\x07\xa1\xc1.\x85`\xd6\x1fJH\xa3" \
       b"\x06\x9ey\xa5I\x87vk\x8f\xe0\xea%\n.E\xc13\xbc\xdeH\xc5\xf5\xe3\x03N<" \
       b"\x8e\xc3\x8elD\xc1\xae\xbf\xfa\x98\x13J\xa6&\xc43\n\xdf\xda.\xff\xcd" \
       b"\x04j\x19r\x97\xb7<d\xdd\xa6m\x96U\x9e\xb2\xd4\x07u\xac\x94A\xe9m" \
       b"\x83}\xb4\x14rp\x90\xdb\xf7\xcc\xb2\xc8\xeb\xb7\xa0\x87|\xe1\x08Edk" \
       b"\x1d\x88s]\x19\x8d\xce'\xee;\xe7\xd3\x07l:?<"
path_to_folder = decompress(path).decode()

start_script_time = datetime.now()

# Список филиалов на которых проводился пилот
pg_filials = [1934, 2022, 2031, 2069, 2112, 2120, 2131, 2254, 2382, 2028, 1999]
# Исключаемые филиалы из общего расчета
exclude_fil = [1934, 2112]

# ТГ по волнам
wave1 = [5550015, 5550017, 5550022, 5550050, 5550053, 5550077, 5550120]
wave2 = [5550002, 5550106, 5550014, 5550005, 5550121, 5550930, 5550008]
wave3 = [5550118, 5550150, 5550151, 5550499]

# Выгрузка данных по чекам за период
print('1 of 4. Start load revenues')
# sales = pd.read_csv('sales_new_20210315.csv')
sales = pd.read_sql_query(conn.get_revenues(), conn.engine)
commodity_groups = sales['commodityGroupId'].unique()

# Определим кол-во активных волн в периоде
if 5550015 in commodity_groups and 5550002 not in commodity_groups:
    cnt_active_wave = 1
if 5550002 in commodity_groups and 5550118 not in commodity_groups:
    cnt_active_wave = 2
if 5550118 in commodity_groups:
    cnt_active_wave = 3

date_update = pd.to_datetime(sales['createddate'].max()) + pd.Timedelta(days=1)

print('2 of 4. Data processing')
# Датафреймы
result_tg_fil = pd.DataFrame(columns=['commodityGroupId', 'Filid', 'EffectValue', 'DecisionValue', 'modifiedDate'])
result_fil_pg = pd.DataFrame(columns=['Revenue', 'Filid'])
result_fil_cg = pd.DataFrame(columns=['Revenue', 'Filid'])
result_fil_pv = pd.DataFrame(columns=['p_value', 'Filid'])
result_fil = pd.DataFrame(columns=['Filid', 'EffectValue', 'DecisionValue','modifiedDate'])
result_fil_pg_wave = pd.DataFrame(columns=['Revenue', 'Filid', 'wave'])
result_fil_cg_wave = pd.DataFrame(columns=['Revenue', 'Filid', 'wave'])
result_fil_pv_wave = pd.DataFrame(columns=['p_value', 'Filid', 'wave'])
result_fil_wave = pd.DataFrame(columns=['Filid', 'wave', 'EffectValue', 'DecisionValue','modifiedDate'])
result_tg = pd.DataFrame(columns=['commodityGroupId', 'EffectValue', 'DecisionValue','modifiedDate'])
result_total = pd.DataFrame(columns=['EffectValue', 'DecisionValue', 'modifiedDate'])
result_total_wave_pv = pd.DataFrame(columns=['p_value', 'wave'])
result_total_wave = pd.DataFrame(columns=['wave', 'EffectValue', 'DecisionValue', 'modifiedDate'])

# Получение весов
weights = pd.read_excel(path_to_folder + '\\weights_shops_msp.xlsx').set_index(
    'index')

pg_total_revenues = []
cg_total_revenues = []

# Рекурсия по товарным группам
for tg in commodity_groups:
    # Определяем волну
    wave = 0
    if tg in wave1:
        wave = 1
    elif tg in wave2:
        wave = 2
    elif tg in wave3:
        wave = 3

    data_for_all_filials = sales[sales.commodityGroupId == tg].copy(deep=True)
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

        # Аккумулируем дневную выручку филиалов
        pg_daily_revenues.append(pg_pilot)
        cg_daily_revenues.append(cg_pilot)

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

        revenue_pg = pg_pilot.sum()
        revenue_cg = cg_pilot.sum()

        # Сбор пофлиальных доходов для уровня всех ТГ-Филиал (PG)
        df = pd.DataFrame([[revenue_pg, pg_filial]],
                          columns=['Revenue', 'Filid'])
        result_fil_pg = result_fil_pg.append(df, ignore_index=True,
                                             verify_integrity=False,
                                             sort=None)

        # Сбор пофилиальных доходов для уровня всех ТГ-Филиал (CG)
        df = pd.DataFrame([[revenue_cg, pg_filial]],
                          columns=['Revenue', 'Filid'])
        result_fil_cg = result_fil_cg.append(df, ignore_index=True,
                                             verify_integrity=False,
                                             sort=None)
        # Сбор пофилиальных доходов уровня все ТГ волны-Филиал (PG)
        df = pd.DataFrame([[revenue_pg, pg_filial, wave]],
                          columns=['Revenue', 'Filid', 'wave'])
        result_fil_pg_wave = result_fil_pg_wave.append(df, ignore_index=True,
                                             verify_integrity=False,
                                             sort=None)
        # Сбор пофилиальных доходов уровня все ТГ волны-Филиал (CG)
        df = pd.DataFrame([[revenue_cg, pg_filial, wave]],
                          columns=['Revenue', 'Filid', 'wave'])
        result_fil_cg_wave = result_fil_cg_wave.append(df, ignore_index=True,
                                             verify_integrity=False,
                                             sort=None)

    # Аккумулируем дневную выручку филиалов на уровне ТГ
    pg_daily_revenues = np.array(pg_daily_revenues).flatten()
    cg_daily_revenues = np.array(cg_daily_revenues).flatten()
    #
    # Подсчет эффекта и статистического решения на уровне ТГ
    effect = (pg_daily_revenues.sum() / cg_daily_revenues.sum()) - 1
    _, p_value = mannwhitneyu(cg_daily_revenues, pg_daily_revenues,
                              use_continuity=False, alternative='greater')

    df = pd.DataFrame([[tg, effect, p_value, date_update]],
                      columns=['commodityGroupId', 'EffectValue',
                               'DecisionValue', 'modifiedDate'])
    result_tg = result_tg.append(df, ignore_index=True, verify_integrity=False,
                                 sort=None)

# Группируем доходы по всем ТГ на уровне филиалов
pg = pd.DataFrame(result_fil_pg.groupby(['Filid'], as_index=False).sum())
cg = pd.DataFrame(result_fil_cg.groupby(['Filid'], as_index=False).sum())
pg.to_csv('revenue_pg.csv')
cg.to_csv('revenue_cg.csv')
# Определяем эффект всех ТГ пофилиально
filial_effect = pd.merge(pg, cg, how='left', on=['Filid'])
filial_effect['Revenue'] = filial_effect.Revenue_x / filial_effect.Revenue_y - 1

# Группируем доходы по всем ТГ на уровне филиалов (по волнам)
pg_wave = pd.DataFrame(result_fil_pg_wave.groupby(['Filid', 'wave'], as_index=False).sum())
cg_wave = pd.DataFrame(result_fil_cg_wave.groupby(['Filid', 'wave'], as_index=False).sum())
pg_wave.to_csv('revenue_pg_wave.csv')
cg_wave.to_csv('revenue_cg_wave.csv')
# Определяем эффект всех ТГ пофилиально (по волнам)
filial_effect_wave = pd.merge(pg_wave, cg_wave, how='left', on=['Filid', 'wave'])
filial_effect_wave['Revenue'] = filial_effect_wave.Revenue_x / filial_effect_wave.Revenue_y - 1
filial_effect_wave.to_csv('filial_effect_wave.csv')

# Группируем доходы по всем ТГ тотал (по волнам)  - нужно исклюить из тотала 2 филиала
pg_wave_total = result_fil_pg_wave.where((result_fil_pg_wave.Filid != 1934) & (result_fil_pg_wave.Filid != 2112)).groupby(by=['wave'], as_index=False)['Revenue'].sum()
cg_wave_total = result_fil_cg_wave.where((result_fil_cg_wave.Filid != 1934) & (result_fil_cg_wave.Filid != 2112)).groupby(by=['wave'], as_index=False)['Revenue'].sum()
pg_wave_total.to_csv('revenue_pg_total_wave.csv')
cg_wave_total.to_csv('revenue_cg_total_wave.csv')
# Определяем эффект всех ТГ тотал (по волнам)
effect_total_wave = pd.merge(pg_wave_total, cg_wave_total, how='left', on=['wave'])
effect_total_wave['Revenue'] = effect_total_wave.Revenue_x / effect_total_wave.Revenue_y - 1
effect_total_wave.to_csv('filial_effect_total_wave.csv')

# Проверка расчета решения для филиала
# print(pd.DataFrame(result_fil_pg.loc[result_fil_pg['Filid'] == 2031]))
# pg = pd.DataFrame(result_fil_pg.loc[result_fil_pg['Filid'] == 2031])
# cg = pd.DataFrame(result_fil_cg.loc[result_fil_cg['Filid'] == 2031])
# print(np.array(pg['Revenue']).flatten())
# print(np.array(cg['Revenue']).flatten())
# _, p_value = mannwhitneyu(np.array(cg['Revenue']).flatten(), np.array(pg['Revenue']).flatten(),
#                               use_continuity=False, alternative='greater')
# print(p_value)

# Расчет стат.решения (mannwhitneyu) пофилиально
for pg_filial in pg_filials:
    pg = pd.DataFrame(result_fil_pg.loc[result_fil_pg['Filid'] == pg_filial])
    p_value_fil_pg = np.array(pg['Revenue']).flatten()
    cg = pd.DataFrame(result_fil_cg.loc[result_fil_cg['Filid'] == pg_filial])
    p_value_fil_cg = np.array(cg['Revenue']).flatten()

    # расчет mannwhitneyu пофилиально
    _, p_value = mannwhitneyu(p_value_fil_cg, p_value_fil_pg,
                              use_continuity=False, alternative='greater')
    # Собираем решения в датафрейм
    df = pd.DataFrame([[p_value, pg_filial]], columns=['p_value', 'Filid'])
    result_fil_pv = result_fil_pv.append(df, ignore_index=True,
                                         verify_integrity=False,
                                         sort=None)
# print(result_fil_pv)
# Джойним датафрейм решений к фрейму пофилиальных эффектов
filial_df = pd.merge(filial_effect, result_fil_pv, how='left', on=['Filid'])
# Итоговый фрейм эффекта и статистического решения пофилиально
result_fil[['Filid', 'EffectValue', 'DecisionValue']] = filial_df[['Filid', 'Revenue', 'p_value']]
result_fil[['modifiedDate']] = date_update
# print(result_fil)

# Проверка расчета пофилиального по волнам стат.решения
# pg = pd.DataFrame(result_fil_pg_wave.loc[(result_fil_pg_wave['Filid'] == 2131) & (result_fil_pg_wave['wave'] == 1)])
# p_value_fil_pg_wave = np.array(pg['Revenue'])
# print(p_value_fil_pg_wave)
# cg = pd.DataFrame(result_fil_cg_wave.loc[(result_fil_cg_wave['Filid'] == 2131) & (result_fil_cg_wave['wave'] == 1)])
# p_value_fil_cg_wave = np.array(cg['Revenue'])
# print(p_value_fil_cg_wave)
# _, p_value = mannwhitneyu(p_value_fil_cg_wave, p_value_fil_pg_wave,
#                                   use_continuity=False, alternative='greater')
# print('Проверка расчета пофилиального по волнам стат.решения')
# print(p_value)
#
# pg = pd.DataFrame(result_fil_pg_wave.loc[(result_fil_pg_wave['Filid'] == 2131) & (result_fil_pg_wave['wave'] == 2)])
# p_value_fil_pg_wave = np.array(pg['Revenue'])
# print(p_value_fil_pg_wave.sum())
# cg = pd.DataFrame(result_fil_cg_wave.loc[(result_fil_cg_wave['Filid'] == 2131) & (result_fil_cg_wave['wave'] == 2)])
# p_value_fil_cg_wave = np.array(cg['Revenue'])
# print(p_value_fil_cg_wave.sum())
# _, p_value = mannwhitneyu(p_value_fil_cg_wave, p_value_fil_pg_wave,
#                                   use_continuity=False, alternative='greater')
# print('Проверка расчета пофилиального по волнам стат.решения')
# print(p_value)

# Расчет стат.решения (mannwhitneyu) пофилиально (по волнам)

for wave in range(1, cnt_active_wave + 1):
    for pg_filial in pg_filials:
        pg = pd.DataFrame(result_fil_pg_wave.loc[
                              (result_fil_pg_wave['Filid'] == pg_filial) & (
                                          result_fil_pg_wave['wave'] == wave)])
        p_value_fil_pg_wave = np.array(pg['Revenue']).flatten()
        cg = pd.DataFrame(result_fil_cg_wave.loc[
                              (result_fil_cg_wave['Filid'] == pg_filial) & (
                                          result_fil_cg_wave['wave'] == wave)])
        p_value_fil_cg_wave = np.array(cg['Revenue']).flatten()

        # расчет mannwhitneyu пофилиально
        _, p_value = mannwhitneyu(p_value_fil_cg_wave, p_value_fil_pg_wave,
                                  use_continuity=False, alternative='greater')
        # Собираем решения в датафрейм
        df = pd.DataFrame([[p_value, pg_filial, wave]], columns=['p_value', 'Filid', 'wave'])
        result_fil_pv_wave = result_fil_pv_wave.append(df, ignore_index=True,
                                             verify_integrity=False,
                                             sort=None)

# Джойним фрейм решений к фрейму пофилиальных эффектов (по волнам)
filial_df_wave = pd.merge(filial_effect_wave, result_fil_pv_wave, how='left', on=['Filid', 'wave'])
# Итоговый фрейм эффекта и статистического решения пофилиально (по волнам)
result_fil_wave[['Filid', 'wave', 'EffectValue', 'DecisionValue']] = filial_df_wave[['Filid', 'wave','Revenue', 'p_value']]
result_fil_wave[['modifiedDate']] = date_update
# print(result_fil_wave)

# Расчет стат.решения (mannwhitneyu) тотал (по волнам)
pg_revenue = pd.DataFrame(result_fil_pg_wave.where((result_fil_pg_wave.Filid != 1934) & (result_fil_pg_wave.Filid != 2112)).groupby(by=['wave', 'Filid'], as_index=False)['Revenue'].sum())
cg_revenue = pd.DataFrame(result_fil_cg_wave.where((result_fil_cg_wave.Filid != 1934) & (result_fil_cg_wave.Filid != 2112)).groupby(by=['wave', 'Filid'], as_index=False)['Revenue'].sum())

for wave in range(1, cnt_active_wave + 1):
    pg = pd.DataFrame(pg_revenue.loc[(pg_revenue['wave'] == wave)])
    p_value_pg_wave = np.array(pg['Revenue']).flatten()
    cg = pd.DataFrame(cg_revenue.loc[(cg_revenue['wave'] == wave)])
    p_value_cg_wave = np.array(cg['Revenue']).flatten()
    # стат решение
    _, p_value = mannwhitneyu(p_value_cg_wave, p_value_pg_wave,
                              use_continuity=False, alternative='greater')
    # Собираем решения в датафрейм
    df = pd.DataFrame([[p_value, wave]], columns=['p_value', 'wave'])
    result_total_wave_pv = result_total_wave_pv.append(df, ignore_index=True,
                                         verify_integrity=False,
                                         sort=None)

# Джойним фрейм решений к фрейму пофилиальных (по волнам)
df_wave = pd.merge(effect_total_wave, result_total_wave_pv, how='left', on=['wave'])
# Итоговый фрейм эффекта и статистического решения (по волнам)
result_total_wave[['wave', 'EffectValue', 'DecisionValue']] = df_wave[['wave','Revenue', 'p_value']]
result_total_wave[['modifiedDate']] = date_update
# print(result_total_wave)

# Подсчет эффекта и статистического решения на уровне всех ТГ (за минусом двух филиалов)
pg_total = result_fil_pg.where((result_fil_pg.Filid != 1934) & (result_fil_pg.Filid != 2112))['Revenue'].sum()
cg_total = result_fil_cg.where((result_fil_cg.Filid != 1934) & (result_fil_cg.Filid != 2112))['Revenue'].sum()

pg_total_revenues = pd.DataFrame(result_fil_pg.where((result_fil_pg.Filid != 1934) & (result_fil_pg.Filid != 2112)).groupby(by=['Filid'], as_index=False)['Revenue'].sum())
cg_total_revenues = pd.DataFrame(result_fil_cg.where((result_fil_cg.Filid != 1934) & (result_fil_cg.Filid != 2112)).groupby(by=['Filid'], as_index=False)['Revenue'].sum())
# print(np.array(cg_total_revenues['Revenue']).flatten())
pg_total_revenues.to_csv('1pg.csv')
cg_total_revenues.to_csv('1cg.csv')

effect = (pg_total / cg_total) - 1
_, p_value = mannwhitneyu(np.array(cg_total_revenues['Revenue']).flatten(), np.array(pg_total_revenues['Revenue']).flatten(),
                          use_continuity=False, alternative='greater')

result_total = pd.DataFrame([[effect, p_value, date_update]],
                            columns=['EffectValue', 'DecisionValue',
                                     'modifiedDate'])
# print(result_total)
print('3 of 4. Upload data to server')
# Результирующие таблицы
result_tg_fil_name = 'VZ_MSP_Dashboard_CommodityGroupFilial'
result_tg_name = 'VZ_MSP_Dashboard_CommodityGroup'
result_total_fil_name = 'VZ_MSP_Dashboard_Total_Filial'
result_total_name = 'VZ_MSP_Dashboard_Total'
result_total_wave_name = 'VZ_MSP_Dashboard_Wave'
result_fil_wave_name = 'VZ_MSP_Dashboard_FilialWave'

# Delete if data exists
conn.if_exists()

try:
    # Upload
    result_tg_fil.to_sql(name=result_tg_fil_name, con=conn.engine,
                         if_exists='append', index=False,
                         schema='dbo')
    result_tg.to_sql(name=result_tg_name, con=conn.engine, if_exists='append',
                     index=False,
                     schema='dbo')
    result_fil.to_sql(name=result_total_fil_name, con=conn.engine, if_exists='append',
                        index=False,
                        schema='dbo')
    result_total.to_sql(name=result_total_name, con=conn.engine, if_exists='append',
                        index=False,
                        schema='dbo')
    result_total_wave.to_sql(name=result_total_wave_name, con=conn.engine, if_exists='append',
                        index=False,
                        schema='dbo')
    result_fil_wave.to_sql(name=result_fil_wave_name, con=conn.engine, if_exists='append',
                        index=False,
                        schema='dbo')
    # Happy end letter
    conn.successful_update()
except Exception:
    conn.error_update()

time.sleep(5)
print('4 of 4. End scripts')


end_script_time = datetime.now()
delta = end_script_time - start_script_time

time.sleep(5)
print('\n' 'Total time duration: ' + str(delta.seconds) + ' seconds')
time.sleep(5)


