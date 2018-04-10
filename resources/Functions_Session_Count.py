# coding: utf8

import re
import time
import datetime
import MySQLdb
from resources import Settings
from resources import Functions_Onyma as Onyma
from selenium import webdriver as webdriver

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

def get_accounts(cursor):
    command = '''
    SELECT account_name, bill, dmid, tmid
    FROM abon_dsl
    WHERE account_name IS NOT NULL
    '''  
    cursor.execute(command)
    return cursor.fetchall()

def update_abon_dsl(cursor, bill, dmid, tmid, account_name): 
    command = '''
    UPDATE abon_dsl
    SET bill = "{}", dmid = "{}", tmid = "{}"
    WHERE account_name = "{}"
    '''.format(bill, dmid, tmid, account_name)
    try:
        cursor.execute(command)
    except:
        pass
    else:
        cursor.execute('commit')

def run(account_list):
    connect = MySQLdb.connect(host=Settings.db_host, user=Settings.db_user, password=Settings.db_password, db=Settings.db_name, charset='utf8')
    cursor = connect.cursor()
    browser = Onyma.open_onyma()

    prev_day = datetime.date.today() - datetime.timedelta(days=1)
    for account in account_list:
        account_name = account[0]
        if account[1] is not None and account[2] is not None and account[3] is not None:
            bill = account[1]
            dmid = account[2]
            tmid = account[3]
        else:
            account_param = Onyma.find_account_param(browser, account_name)
            if account_param is False:
                continue
            elif account_param == -1:
                browser.quit()
                browser = Onyma.open_onyma()
                continue
            else:            
                bill, dmid, tmid = account_param
                update_abon_dsl(cursor, bill, dmid, tmid, account_name)
        count = Onyma.count_sessions(bill,  dmid,  tmid,  prev_day,  browser,  cursor)
        if count is False:
            continue
        elif count == -1:
            browser.quit()
            browser = Onyma.open_onyma()
            continue
        elif count == 0:
            account_param = Onyma.find_account_param(browser, account_name)
            if account_param is False:
                continue
            elif account_param == -1:
                browser.quit()
                browser = Onyma.open_onyma()
                continue
            else:
                cur_bill, cur_dmid, cur_tmid = account_param
            if cur_bill != bill or cur_tmid != tmid or cur_dmid != dmid:
                update_abon_dsl(cursor, cur_bill, cur_dmid, cur_tmid, account_name)
                count = Onyma.count_sessions(cur_bill,  cur_dmid,  cur_tmid,  prev_day,  browser,  cursor)          
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
    connect.close()
    browser.quit()
