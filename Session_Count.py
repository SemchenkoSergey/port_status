# coding: utf8

import MySQLdb
import datetime
import time
from concurrent.futures import ThreadPoolExecutor
from resources import Settings
from resources import Functions_Session_Count as Func_SC

# Начало

status = {'date' : datetime.datetime.now().date(),  'state' : True}
#status = {'date' : datetime.datetime.now().date(),  'state' : False}
while True:
    if status['date'] == datetime.datetime.now().date() and status['state'] == False:
        print("Начало работы: {}".format(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
        connect = MySQLdb.connect(host=Settings.db_host, user=Settings.db_user, password=Settings.db_password, db=Settings.db_name, charset='utf8')
        cursor = connect.cursor()
        Func_SC.check_tables(cursor)
        account_list = Func_SC.get_accounts(cursor)
        connect.close()
        arguments = [account_list[x::Settings.threads_count]  for x in range(0,  Settings.threads_count)]
        del account_list
        
        with ThreadPoolExecutor(max_workers=Settings.threads_count) as executor:
            executor.map(Func_SC.run, arguments)
            
        print("Завершение работы: {}".format(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
        status['state'] = True
    else:
        if (datetime.datetime.now().date()  - status['date'] > datetime.timedelta(0)) and (datetime.datetime.now().hour >= 6):
            status['date'] = datetime.datetime.now().date()
            status['state'] = False
            continue
        time.sleep(60*30)
        continue
