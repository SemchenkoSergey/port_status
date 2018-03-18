# coding: utf8

import os
import datetime
from concurrent.futures import ThreadPoolExecutor
from resources import Settings
from resources import Functions_Make_Abon as Func_MA

print("Начало работы: {}".format(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
Func_MA.create_error_files()
Func_MA.create_abon_dsl()

# Обработка файлов в директории in/argus/
file_list = ['in' + os.sep + 'argus' + os.sep + x for x in os.listdir('in' + os.sep + 'argus')]
Func_MA.argus_abon_dsl(file_list)

# Обработка файлов в директории in/onyma/
file_list = ['in' + os.sep + 'onyma' + os.sep + x for x in os.listdir('in' + os.sep + 'onyma')]
Func_MA.onyma_abon_dsl(file_list)

# Заполнение полей bill, dmid, tmid
account_list = Func_MA.get_accounts()
arguments = [account_list[x::Settings.threads_count]  for x in range(0,  Settings.threads_count)]
del account_list
with ThreadPoolExecutor(max_workers=Settings.threads_count) as executor:
    executor.map(Func_MA.run_define_param, arguments)
print("Завершение работы: {}".format(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
