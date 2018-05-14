# coding: utf8

import MySQLdb
from resources import Settings
from resources import Functions_Onyma as Onyma


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

