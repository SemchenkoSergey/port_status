# coding: utf8

import re
import time
import MySQLdb
import datetime
from resources import Onyma
from resources import Settings
from concurrent.futures import ThreadPoolExecutor


def check_tables(cursor):
    table = '''
        CREATE TABLE IF NOT EXISTS data_sessions (
        account_name VARCHAR(20),
        date DATE,
        count TINYINT UNSIGNED,
        CONSTRAINT pk_data_sessions PRIMARY KEY (account_name, date)    
        )'''
    try:
        cursor.execute(table)
    except:
        pass
    else:
        cursor.execute('commit')

    table = '''
        CREATE TABLE IF NOT EXISTS abon_onyma (
        account_name VARCHAR(20) NOT NULL,
        bill VARCHAR(15) NOT NULL,
        dmid VARCHAR(15) NOT NULL,
        tmid VARCHAR(15) NOT NULL,
        CONSTRAINT pk_abon_onyma PRIMARY KEY (account_name)    
        )'''
    try:
        cursor.execute(table)
    except:
        pass
    else:
        cursor.execute('commit')

def delete_old_records():
    connect = MySQLdb.connect(host=Settings.db_host, user=Settings.db_user, password=Settings.db_password, db=Settings.db_name, charset='utf8')
    cursor = connect.cursor()     
    command = '''
    DELETE
    FROM data_sessions
    WHERE date < DATE_ADD(CURRENT_DATE(), INTERVAL -{} DAY)
    '''.format(Settings.days)
    try:
        cursor.execute(command)
    except:
        pass
    else:
        cursor.execute('commit')
    connect.close()  

def get_accounts(cursor):
    command = '''
    SELECT account_name, tv
    FROM abon_dsl
    WHERE account_name IS NOT NULL
    '''  
    cursor.execute(command)
    return cursor.fetchall()
    

def get_onyma_params(cursor):
    command = '''
    SELECT account_name, bill, dmid, tmid
    FROM abon_onyma
    '''  
    cursor.execute(command)
    onyma_param = cursor.fetchall()
    result = {}
    for param in onyma_param:
        result[param[0]] = {'bill' : param[1], 'dmid' : param[2], 'tmid' : param[3]}
    return result    

def insert_abon_onyma(cursor, bill, dmid, tmid, account_name): 
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
        
def update_abon_onyma(cursor, bill, dmid, tmid, account_name): 
    command = '''
    UPDATE abon_onyma
    SET bill = "{}", dmid = "{}", tmid = "{}"
    WHERE account_name = "{}"
    '''.format(bill, dmid, tmid, account_name)
    try:
        cursor.execute(command)
    except:
        pass
    else:
        cursor.execute('commit')
        
def update_abon_dsl(cursor, account_name):
    command = '''
    UPDATE abon_dsl
    SET tv = "yes"
    WHERE account_name = "{}"
    '''.format(account_name)
    try:
        cursor.execute(command)
    except:
        pass
    else:
        cursor.execute('commit')
        
def insert_data_sessions(cursor, account_name, prev_day, count):
    command = '''
    INSERT INTO data_sessions
    (account_name, date, count)
    VALUES
    ("{}", "{}", {})
    '''.format(account_name, prev_day.strftime('%Y-%m-%d'), count)
    try:
        cursor.execute(command)
    except:
        pass
    else:
        cursor.execute('commit')
        
def run(arguments):
    count_processed = 0
    count_insert = 0
    count_update = 0
    count_tv = 0
    connect = MySQLdb.connect(host=Settings.db_host, user=Settings.db_user, password=Settings.db_password, db=Settings.db_name, charset='utf8')
    cursor = connect.cursor()
    onyma = Onyma.get_onyma()
    account_list = arguments[0]
    onyma_param_list = arguments[1]

    prev_day = datetime.date.today() - datetime.timedelta(days=1)
    for account in account_list:
        account_name = account[0]
        account_tv = account[1]
        if account_name in onyma_param_list:
            bill = onyma_param_list[account_name]['bill']
            dmid = onyma_param_list[account_name]['dmid']
            tmid = onyma_param_list[account_name]['tmid']
        else:
            onyma_param = Onyma.find_account_param(onyma, account_name)
            if onyma_param == -1:
                onyma = Onyma.get_onyma()
                continue
            elif onyma_param is False:
                count_processed += 1
                insert_data_sessions(cursor, account_name, prev_day, 0)
                continue
            else:            
                bill, dmid, tmid = onyma_param
                insert_abon_onyma(cursor, bill, dmid, tmid, account_name)
                count_insert += 1
        count = Onyma.count_sessions(onyma, bill,  dmid,  tmid,  prev_day)
        tv = Onyma.update_tv(onyma, bill, prev_day)
        if (count == -1) or (tv == -1):
            onyma = Onyma.get_onyma()
            continue
        if (tv is True) and (account_tv == 'no'):
            update_abon_dsl(cursor, account_name)
        if count == 0:
            onyma_param = Onyma.find_account_param(onyma, account_name)
            if onyma_param == -1:
                onyma = Onyma.get_onyma()
                continue
            elif onyma_param is False:
                count_processed += 1
                insert_data_sessions(cursor, account_name, prev_day, 0)
                continue            
            else:
                cur_bill, cur_dmid, cur_tmid = onyma_param
                if cur_bill != bill or cur_tmid != tmid or cur_dmid != dmid:
                    update_abon_onyma(cursor, cur_bill, cur_dmid, cur_tmid, account_name)
                    count_update += 1
                count = Onyma.count_sessions(onyma, cur_bill,  cur_dmid,  cur_tmid,  prev_day)
                if count == -1:
                    onyma = Onyma.get_onyma()
                    continue                
        count_processed += 1
        insert_data_sessions(cursor, account_name, prev_day, count)
    connect.close()
    del onyma
    return (count_processed, count_insert, count_update, count_tv)


def main():
    # Начало
    run_date = datetime.datetime.now().date()
    print('Проверка сессий начнется завтра после 5 часов утра...\n')
    #run_date = datetime.datetime.now().date() - datetime.timedelta(days=1)
    
    while True:
        current_date = datetime.datetime.now().date()
        if (current_date != run_date) and (datetime.datetime.now().hour >= 5):
            print('Начало работы: {}'.format(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
            count_processed = 0
            count_insert = 0
            count_update = 0
            count_tv = 0
            connect = MySQLdb.connect(host=Settings.db_host, user=Settings.db_user, password=Settings.db_password, db=Settings.db_name, charset='utf8')
            cursor = connect.cursor()
            check_tables(cursor)
            account_list = get_accounts(cursor)
            if len(account_list) == 0:
                print('\n!!! Необходимо сформировать таблицу abon_dsl !!!\n')
                return
            onyma_param_list = get_onyma_params(cursor)
            connect.close()
            arguments = [(account_list[x::Settings.threads_count], onyma_param_list)  for x in range(0,  Settings.threads_count)]
      
            with ThreadPoolExecutor(max_workers=Settings.threads_count) as executor:
                result = executor.map(run, arguments)
           
            for count in result:
                count_processed += count[0]
                count_insert += count[1]
                count_update += count[2]
                count_tv += count[3]

            print('\nОбработано: {}'.format(count_processed))
            print('Добавлено: {}'.format(count_insert))
            print('Обновлено: {}'.format(count_update))
            print('Обнаружено ТВ: {}\n'.format(count_tv))
            
            delete_old_records()
            print('Завершение работы: {}'.format(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
            run_date = current_date
        else:
            time.sleep(60*10)
            continue
