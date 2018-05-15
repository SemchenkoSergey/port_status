# coding: utf8

import sys
import datetime
from concurrent.futures import ThreadPoolExecutor
from resources import Settings
from resources import Functions_Make_Abon_Onyma as Func_MA_Onyma
import warnings

warnings.filterwarnings("ignore")


def main():
    print("Начало работы: {}".format(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    Func_MA_Onyma.create_abon_onyma()
    count = 0
    
    # Заполнение полей bill, dmid, tmid
    account_list = Func_MA_Onyma.get_accounts()
    if len(account_list) == 0:
        print('\n!!! Необходимо сформировать таблицу abon_dsl !!!\n')
        sys.exit()
    arguments = [account_list[x::Settings.threads_count]  for x in range(0,  Settings.threads_count)]
    print('\nПолучение данных из Онимы...')
    with ThreadPoolExecutor(max_workers=Settings.threads_count) as executor:
        result = executor.map(Func_MA_Onyma.run_define_param, arguments)
    for i in result:
        count += i
    print('Обработано {} записей.'.format(count))
    print("\nЗавершение работы: {}".format(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    sys.exit()


if __name__ == '__main__':
    main()