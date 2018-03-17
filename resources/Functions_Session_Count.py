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
    WHERE (account_name IS NOT NULL) AND (bill IS NOT NULL) AND (dmid IS NOT NULL) AND (tmid IS NOT NULL)
    '''
    cursor.execute(command)
    return cursor.fetchall()

def run(account_list):
    connect = MySQLdb.connect(host=Settings.db_host, user=Settings.db_user, password=Settings.db_password, db=Settings.db_name, charset='utf8')
    cursor = connect.cursor()
    browser = Onyma.open_onyma()

    prev_day = datetime.date.today() - datetime.timedelta(days=1)
    for account in account_list:
        account_name = account[0]
        bill = account[1]
        dmid = account[2]
        tmid = account[3]        
        try:
            count = Onyma.count_sessions(bill,  dmid,  tmid,  prev_day,  browser,  cursor)
            if count == 0:
                cur_bill, cur_tmid, cur_dmid = Onyma.find_account_param(browser, account_name)
                if cur_bill != bill or cur_tmid != tmid or cur_dmid != dmid:
                    command = '''
                    UPDATE abon_dsl
                    SET bill = "{}", tmid = "{}", dmid = "{}"
                    WHERE account_name = "{}"
                    '''.format(cur_bill, cur_tmid, cur_dmid, account_name)
                    try:
                        cursor.execute(command)
                    except:
                        pass
                    else:
                        cursor.execute('commit')
                    count = Onyma.count_sessions(cur_bill,  cur_dmid,  cur_tmid,  prev_day,  browser,  cursor)
        except Exception as ex:
            print(ex)
            browser.quit()
            del browser
            time.sleep(15)
            browser = Onyma.open_onyma()
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
