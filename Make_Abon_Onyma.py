# coding: utf8

import os
import datetime
from concurrent.futures import ThreadPoolExecutor
from resources import Settings
from resources import Functions_Make_Abon_Onyma as Func_MA_Onyma

print("Начало работы: {}".format(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
Func_MA_Onyma.create_abon_onyma()

# Заполнение полей bill, dmid, tmid
account_list = Func_MA_Onyma.get_accounts()
arguments = [account_list[x::Settings.threads_count]  for x in range(0,  Settings.threads_count)]
print('\nПолучение данных из Онимы...')
with ThreadPoolExecutor(max_workers=Settings.threads_count) as executor:
    executor.map(Func_MA_Onyma.run_define_param, arguments)
print("Завершение работы: {}".format(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
