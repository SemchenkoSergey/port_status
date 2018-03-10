# coding: utf8

import os
from resources import Functions_Make_Abon as Func_MA

Func_MA.create_error_files()
Func_MA.create_abon_dsl()

# Обработка файлов в директории in/argus/
file_list = ['in' + os.sep + 'argus' + os.sep + x for x in os.listdir('in' + os.sep + 'argus')]
Func_MA.argus_abon_dsl(file_list)

# Обработка файлов в директории in/onyma/
file_list = ['in' + os.sep + 'onyma' + os.sep + x for x in os.listdir('in' + os.sep + 'onyma')]
Func_MA.onyma_abon_dsl(file_list)

print('Программа завершила работу...')
