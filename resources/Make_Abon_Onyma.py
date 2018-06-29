# coding: utf-8

import MySQLdb
import datetime
from concurrent.futures import ThreadPoolExecutor
from resources import Settings
from resources import Onyma


def create_abon_onyma():
    connect = MySQLdb.connect(host=Settings.db_host, user=Settings.db_user, password=Settings.db_password, db=Settings.db_name, charset='utf8')
    cursor = connect.cursor()
    try:
        cursor.execute('DROP TABLE IF EXISTS abon_onyma')
        table = '''
        CREATE TABLE abon_onyma (
        account_name VARCHAR(20) NOT NULL,
        bill VARCHAR(15) NOT NULL,
        dmid VARCHAR(15) NOT NULL,
        tmid VARCHAR(15) NOT NULL,
        CONSTRAINT pk_abon_onyma PRIMARY KEY (account_name)    
        )'''
        cursor.execute(table)
    except:
        pass
    else:
        cursor.execute('commit')
    connect.close()

def get_accounts():
    connect = MySQLdb.connect(host=Settings.db_host, user=Settings.db_user, password=Settings.db_password, db=Settings.db_name, charset='utf8')
    cursor = connect.cursor()
    command = '''
    SELECT account_name
    FROM abon_dsl
    WHERE account_name IS NOT NULL
    '''
    cursor.execute(command)
    result = cursor.fetchall()
    connect.close()
    return result

def run_define_param(account_list):
    count_processed = 0
    connect = MySQLdb.connect(host=Settings.db_host, user=Settings.db_user, password=Settings.db_password, db=Settings.db_name, charset='utf8')
    cursor = connect.cursor()
    onyma = Onyma.get_onyma()
    
    for account in account_list:
        account_name = account[0]
        account_param = Onyma.find_account_param(onyma, account_name)
        if account_param is False:
            continue
        elif account_param == -1:
            onyma = Onyma.get_onyma()
            continue
        else:
            bill, dmid, tmid = account_param
        count_processed += 1
        command = '''
        INSERT INTO abon_onyma
        VALUES ("{}", "{}", "{}", "{}")
        '''.format(account_name, bill, dmid, tmid)
        try:
            cursor.execute(command)
        except:
            pass
        else:
            cursor.execute('commit')
    connect.close()
    del onyma
    return count_processed

def run_define_speed(account_list):
    count_processed = 0
    connect = MySQLdb.connect(host=Settings.db_host, user=Settings.db_user, password=Settings.db_password, db=Settings.db_name, charset='utf8')
    cursor = connect.cursor()
    onyma = Onyma.get_onyma()
    for account in account_list:
        account_name = account[0]
        speed = Onyma.find_account_speed(onyma, account_name)
        if speed is not False:
            command = '''
            UPDATE abon_dsl
            SET tariff = {}
            WHERE account_name = "{}"
            '''.format(speed, account_name)
            try:
                cursor.execute(command)
            except:
                pass
            else:
                cursor.execute('commit')
            count_processed += 1
    connect.close()
    del onyma
    return count_processed
    
def main():
    print("Начало работы: {}".format(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    create_abon_onyma()
    
    ## Заполнение полей bill, dmid, tmid
    account_list = get_accounts()
    if len(account_list) == 0:
        print('\n!!! Необходимо сформировать таблицу abon_dsl !!!\n')
        return
    arguments = [account_list[x::Settings.threads_count]  for x in range(0,  Settings.threads_count)]
    print('\nПолучение данных из Онимы...')
    with ThreadPoolExecutor(max_workers=Settings.threads_count) as executor:
        result = executor.map(run_define_param, arguments)
    count = 0
    for i in result:
        count += i
    print('\nПолучение bill, dmid, tmid')
    print('\nОбработано: {}'.format(count))
    
    # Уточнение тарифов
    with ThreadPoolExecutor(max_workers=Settings.threads_count) as executor:
        result = executor.map(run_define_speed, arguments)
    count = 0
    for i in result:
        count += i
    print('\nУточнение тарифов')
    print('\nОбработано: {}'.format(count))    
    print("\nЗавершение работы: {}".format(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
