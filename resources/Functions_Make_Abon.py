# coding: utf8

import MySQLdb
import re
import csv
import os
import time
import datetime
from resources import Settings
from resources import Functions_Onyma as Onyma
from selenium import webdriver as webdriver

err_file_sql = 'error-abon_dsl-sql.txt'
err_file_argus = 'error-abon_dsl-argus.txt'
err_file_onyma = 'error-abon_dsl-onyma.txt'

def create_error_files():
    current_time = datetime.datetime.now()
    with open('error_files' + os.sep + err_file_argus, 'w') as f:
            f.write(current_time.strftime('%Y-%m-%d %H:%M') + '\n')
    with open('error_files' + os.sep + err_file_onyma, 'w') as f:
            f.write(current_time.strftime('%Y-%m-%d %H:%M') + '\n')
    with open('error_files' + os.sep + err_file_sql, 'w') as f:
            f.write(current_time.strftime('%Y-%m-%d %H:%M') + '\n')
    
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
        protect VARCHAR(10),
        tariff SMALLINT UNSIGNED,
        account_name VARCHAR(20),
        bill VARCHAR(15),
        dmid VARCHAR(15),
        tmid VARCHAR(15),
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
    

def argus_abon_dsl(file_list):
    connect = MySQLdb.connect(host=Settings.db_host, user=Settings.db_user, password=Settings.db_password, db=Settings.db_name, charset='utf8')
    cursor = connect.cursor()        
    
    # Подготовка регулярных выражений
    re_dsl = re.compile(r'([\w(DSL)-]+)\[[\d\.]+\].+?(\d+)[_/](\d+)_? - (\d+)')
    re_protect = re.compile(r'(ЗП\d+)')
    re_address = re.compile(r'(.+), (.+), (.+), (.+), кв. (.+)?')
    
    for file in file_list:
        if file.split('.')[-1] != 'csv':
            continue
        with open(file,  encoding='windows-1251') as f:
            reader = csv.reader(f, delimiter=';')
            for row in reader:
                if len(row) < 5:
                    continue
                cell_dsl = row[4].replace('=', '').replace('"', '')
                cell_address = row[2].replace('=', '').replace('"', '')
                cell_phone = row[0].replace('=', '').replace('"', '')
                # Обработка ячейки с оборудованием
                if (re.search(r'^\d+$', cell_phone) or 'ПП' in cell_phone) and re_dsl.search(cell_dsl):
                    hostname =  '"{}"'.format(re_dsl.search(cell_dsl).group(1))
                    board =  re_dsl.search(cell_dsl).group(3)
                    port =  re_dsl.search(cell_dsl).group(4)
                elif cell_phone != '' and 'DSL' in cell_dsl:
                    with open('error_files' + os.sep + err_file_argus, 'a') as f:
                        f.write(cell_phone + ': ' + cell_dsl + '\n')
                    continue
                else:
                    continue
                protect = '"{}"'.format(re_protect.search(cell_dsl).group(1)) if re_protect.search(cell_dsl) else MySQLdb.NULL
                
                # Обработка ячейки с адресом
                if re_address.search(cell_address):
                    area = '"{}"'.format(re_address.search(cell_address).group(1)) if re_address.search(cell_address).group(1) != None else MySQLdb.NULL
                    locality = '"{}"'.format(re_address.search(cell_address).group(2)) if re_address.search(cell_address).group(2) != None else MySQLdb.NULL
                    street = '"{}"'.format(re_address.search(cell_address).group(3)) if re_address.search(cell_address).group(3) != None else MySQLdb.NULL
                    house_number = '"{}"'.format(re_address.search(cell_address).group(4)) if re_address.search(cell_address).group(4) != None else MySQLdb.NULL
                    apartment_number = '"{}"'.format(re_address.search(cell_address).group(5)) if re_address.search(cell_address).group(5) != None else MySQLdb.NULL
                else:
                    locality = MySQLdb.NULL
                    street = MySQLdb.NULL
                    house_number = MySQLdb.NULL
                    apartment_number = MySQLdb.NULL
                
                phone_number = '"86547{}"'.format(cell_phone)
                command = '''
                INSERT INTO abon_dsl
                (phone_number, area, locality, street, house_number, apartment_number, hostname, board, port, protect)
                VALUES
                ({}, {}, {}, {}, {}, {}, {}, {}, {}, {})
                '''.format(phone_number, area, locality, street, house_number, apartment_number, hostname, board, port, protect)
                try:
                    cursor.execute(command)
                except Exception as ex:
                    with open('error_files' + os.sep + err_file_sql, 'a') as f:
                        f.write(str(ex) + '\n')
                else:
                    cursor.execute('commit')
    connect.close()
    
    
def onyma_abon_dsl(file_list):
    connect = MySQLdb.connect(host=Settings.db_host, user=Settings.db_user, password=Settings.db_password, db=Settings.db_name, charset='utf8')
    cursor = connect.cursor()   
    
    max_speed = ('АРХИВ - [STV]LL.Все включено!.Город.UNLIM.Население (xDSL)', '[РТК] xDSL Нон-стоп max', '[РТК] xDSL тариф "Игровой"')
    # Подготовка регулярных выражений
    re_speed_kb = re.compile(r'(\d+) ?[k|К]')
    re_speed_mb = re.compile(r'(\d+) ?Мбит')
    
    for file in file_list:
        if file.split('.')[-1] != 'csv':
            continue
        with open(file,  encoding='windows-1251') as f:
            reader = csv.reader(f, delimiter=';')
            for row in reader:
                if row[41] != 'deleted' and re.search(r'[xA]DSL', row[37]):
                    phone_number = '"{}"'.format(row[7]) if len(row[7]) == 10 else '"{}"'.format('86547' + row[7])
                    
                    # Определение IPTV
                    if 'IPTV' in row[23]:
                        command = '''
                        UPDATE abon_dsl
                        SET tv = "yes"
                        WHERE phone_number = {}
                        '''.format(phone_number)
                        try:
                            cursor.execute(command)
                        except Exception as ex:
                            with open('error_files' + os.sep + err_file_sql, 'a') as f:
                                f.write(str(ex) + '\n')
                        else:
                            cursor.execute('commit')
                    
                    # Определение учетного имени
                    account_name = row[21]
                    if row[23] == 'SSG-подключение' and account_name != '':
                        command = '''
                        UPDATE abon_dsl
                        SET account_name = "{}"
                        WHERE phone_number = {}
                        '''.format(account_name, phone_number)
                        try:
                            cursor.execute(command)
                        except Exception as ex:
                            with open('error_files' + os.sep + err_file_sql, 'a') as f:
                                f.write(str(ex) + '\n')
                        else:
                            cursor.execute('commit')                        
                        
                    # Определение скорости подключения
                    tariff = row[26]
                    if re_speed_mb.search(tariff):
                        speed = int(re_speed_mb.search(tariff).group(1)) * 1024
                    elif re_speed_kb.search(tariff):
                        speed = int(re_speed_kb.search(tariff).group(1)) 
                    elif tariff in max_speed:
                        speed = 15 * 1024
                    else:
                        with open('error_files' + os.sep + err_file_onyma, 'a') as f:
                                f.write(tariff + '\n')
                        continue
                    command = '''
                    UPDATE abon_dsl
                    SET tariff = {}
                    WHERE phone_number = {}
                    '''.format(speed, phone_number)
                    try:
                        cursor.execute(command)
                    except Exception as ex:
                        with open('error_files' + os.sep + err_file_sql, 'a') as f:
                            f.write(str(ex) + '\n')                          
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

