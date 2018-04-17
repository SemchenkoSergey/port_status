# coding: utf8

import MySQLdb
from resources import Settings
from resources import Functions_Onyma as Onyma


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
    connect = MySQLdb.connect(host=Settings.db_host, user=Settings.db_user, password=Settings.db_password, db=Settings.db_name, charset='utf8')
    cursor = connect.cursor()
    browser = Onyma.open_onyma()
    
    for account in account_list:
        account_param = Onyma.find_account_param(browser, account[0])
        if account_param is False:
            continue
        elif account_param == -1:
            browser.quit()
            browser = Onyma.open_onyma()
            continue
        else:
            bill, dmid, tmid = account_param
        command = '''
        UPDATE abon_dsl
        SET bill = "{}",  dmid = "{}", tmid = "{}"
        WHERE account_name = "{}"
        '''.format(bill, dmid, tmid, account[0])
        try:
            cursor.execute(command)
        except:
            pass
        else:
            cursor.execute('commit')
    connect.close()
    browser.quit()

