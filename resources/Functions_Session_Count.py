# coding: utf8

import re
import time
import datetime
import MySQLdb
from resources import Settings
from selenium import webdriver as webdriver

def get_browser():
    while True:
        try:
            #browser = webdriver.Firefox()
            browser = webdriver.Chrome()
        except:
            time.sleep(120)
            continue
        else:
            browser.implicitly_wait(10)
            return browser
    
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

def open_onyma(browser):
    browser.get("https://10.144.196.37/onyma/")
    element = browser.find_element_by_id("LOGIN")
    element.send_keys(Settings.onyma_login)
    element = browser.find_element_by_id("PASSWD")
    element.send_keys(Settings.onyma_password)
    element = browser.find_element_by_id("enter")
    element.click()
        
def check_sessions(card_name,  bill,  dmid,  tmid,  date,  browser,  cursor):
    browser.get("https://10.144.196.37/onyma/main/ddstat.htms?bill={}&dt={}&mon={}&year={}&service=201&dmid={}&tmid={}".format(bill,  date.day,  date.month, date.year,  dmid,  tmid))
    browser.find_element_by_id("onymaPageFooter")
    if re.search(r'<td class="foot">Все</td><td class="pgout" colspan="5">.+?<b>(\d+)</b>',  browser.page_source.__repr__()):
        count = re.search(r'<td class="foot">Все</td><td class="pgout" colspan="5">.+?<b>(\d+)</b>',  browser.page_source.__repr__()).group(1)
    else:
        return False
    command = '''
    INSERT INTO data_sessions
    (account_name, date, count)
    VALUES
    ("{}", "{}", {})
    '''.format(card_name,  date.strftime('%Y-%m-%d'),  count)
    try:
        cursor.execute(command)
    except:
        pass
    else:
        cursor.execute('commit')

def run(account_list):
    connect = MySQLdb.connect(host=Settings.db_host, user=Settings.db_user, password=Settings.db_password, db=Settings.db_name, charset='utf8')
    cursor = connect.cursor()
    browser = get_browser()
    open_onyma(browser)

    prev_day = datetime.date.today() - datetime.timedelta(days=1)
    for account in account_list:
        card_name = account[0]
        bill = account[1]
        dmid = account[2]
        tmid = account[3]        
        try:
            check_sessions(card_name, bill,  dmid,  tmid,  prev_day,  browser,  cursor)
        except:
            browser.quit()
            del browser
            time.sleep(15)
            browser = get_browser()
            open_onyma(browser)
    connect.close()
    browser.quit()
