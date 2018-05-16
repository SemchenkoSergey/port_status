# coding: utf8

import MySQLdb
import datetime
import time
import sys
from concurrent.futures import ThreadPoolExecutor
from resources import Settings
from resources import Functions_Session_Count as Func_SC


def main():
    # Начало
    run_date = datetime.datetime.now().date()
    print('Проверка сессий начнется завтра после 5 часов утра...\n')
    #run_date = datetime.datetime.now().date() - datetime.timedelta(days=1)
    
    while True:
        current_date = datetime.datetime.now().date()
        if (current_date != run_date) and (datetime.datetime.now().hour >= 5):
            print('Начало работы: {}'.format(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
            count = 0
            connect = MySQLdb.connect(host=Settings.db_host, user=Settings.db_user, password=Settings.db_password, db=Settings.db_name, charset='utf8')
            cursor = connect.cursor()
            Func_SC.check_tables(cursor)
            account_list = Func_SC.get_accounts(cursor)
            if len(account_list) == 0:
                print('\n!!! Необходимо сформировать таблицу abon_dsl !!!\n')
                sys.exit()
            onyma_param_list = Func_SC.get_onyma_params(cursor)
            connect.close()
            arguments = [(account_list[x::Settings.threads_count], onyma_param_list)  for x in range(0,  Settings.threads_count)]
      
            with ThreadPoolExecutor(max_workers=Settings.threads_count) as executor:
                result = executor.map(Func_SC.run, arguments)
            
            for i in result:
                count += i
            print('\nОбработано {} записей.'.format(count))
            print('Завершение работы: {}'.format(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
            run_date = current_date
        else:
            time.sleep(60*10)
            continue