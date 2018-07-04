# coding: utf-8

# SQL-запросы для других модулей

import MySQLdb
from resources import Settings

def create_abon_dsl ():
    connect = MySQLdb.connect(host=Settings.db_host, user=Settings.db_user, password=Settings.db_password, db=Settings.db_name, charset='utf8')
    cursor = connect.cursor()
    try:
        cursor.execute('DROP TABLE IF EXISTS abon_dsl')
        table = '''
        CREATE TABLE abon_dsl (
        phone_number CHAR(10) NOT NULL,
        area VARCHAR(30),
        locality VARCHAR(30),
        street VARCHAR(30),
        house_number VARCHAR(10),
        apartment_number VARCHAR(10),
        hostname VARCHAR(50),
        board TINYINT UNSIGNED,
        port TINYINT UNSIGNED,
        tariff SMALLINT UNSIGNED,
        account_name VARCHAR(20),
        tv ENUM('yes', 'no') DEFAULT 'no',
        timestamp TIMESTAMP,
        CONSTRAINT pk_abon_dsl PRIMARY KEY (phone_number)    
        )'''
        cursor.execute(table)
    except:
        pass
    else:
        cursor.execute('commit')
    connect.close()
    
    
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


def update_abon_dsl_account(cursor, account_name, phone_number):
    command = '''
    UPDATE abon_dsl
    SET account_name = {}
    WHERE phone_number = {}
    '''.format(account_name, phone_number)
    try:
        cursor.execute(command)
    except Exception as ex:
        print(ex)
    else:
        cursor.execute('commit')
        

def insert_table(cursor, table_name, field, values):
    str_values = ''
    for value in values:
        str_values += value + ','
    str_values = str_values[:-1]
    command = '''
    INSERT INTO {}
    ({})
    VALUES
    ({})
    '''.format(table_name, field, str_values)
    try:
        cursor.execute(command)
    except Exception as ex:
        print(ex)
    else:
        cursor.execute('commit')

        
def update_table(cursor, table_name, set_left, set_right, where_left, where_right):
    command ='''
    UPDATE {}
    SET {} = {}
    WHERE {} = {}
    '''.format(table_name, set_left, set_right, where_left, where_right)
    try:
        cursor.execute(command)
    except Exception as ex:
        print(ex)
    else:
        cursor.execute('commit')