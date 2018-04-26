# coding: utf8

import os
import datetime
from resources import Settings
from resources import Functions_Make_Abon_Dsl as Func_MA_Dsl

print("Начало работы: {}".format(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
Func_MA_Dsl.create_error_files()
Func_MA_Dsl.create_abon_dsl()

# Обработка файлов в директории in/argus/
file_list = ['in' + os.sep + 'argus' + os.sep + x for x in os.listdir('in' + os.sep + 'argus')]
Func_MA_Dsl.argus_abon_dsl(file_list)

# Обработка файлов в директории in/onyma/
file_list = ['in' + os.sep + 'onyma' + os.sep + x for x in os.listdir('in' + os.sep + 'onyma')]
Func_MA_Dsl.onyma_abon_dsl(file_list)

print("Завершение работы: {}".format(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
