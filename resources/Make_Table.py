# coding: utf-8

import os
import re
import csv
import datetime
import MySQLdb
from resources import Onyma
from resources import Settings
from concurrent.futures import ThreadPoolExecutor

argus_phone = {}        # 'id из аргуса': 'номер телефона'
phones = {}             # {'телефон': [(account, onyma_id),...]}

def get_area_code(area):
    codes = (('БЛАГОДАРНЕНСКИЙ', 'Благодарный', '86549'),
             ('БУДЕННОВСКИЙ', 'Буденновск', '86559'),
             ('ГЕОРГИЕВСКИЙ', 'Георгиевск', '87951'),
             ('СОВЕТСКИЙ', 'Зеленокумск', '86552'),
             ('ИЗОБИЛЬНЕНСКИЙ', 'Изобильный', '86545'),
             ('ИПАТОВСКИЙ', 'Ипатово', '86542'),
             ('МИНЕРАЛОВОДСКИЙ', 'Минеральные Воды', '87922'),
             ('ШПАКОВСКИЙ', 'Михайловск', '86553'),
             ('НЕФТЕКУМСКИЙ', 'Нефтекумск', '86558'),
             ('НОВОАЛЕКСАНДРОВСКИЙ', 'Новоалександровск', '86544'),
             ('КИРОВСКИЙ', 'Новопавловск', '87938'),
             ('ПЕТРОВСКИЙ', 'Светлоград', '86547'),
             ('АЛЕКСАНДРОВСКИЙ', 'Александровское', '86557'),
             ('АРЗГИРСКИЙ', 'Арзгир', '86560'),
             ('ГРАЧЕВСКИЙ', 'Грачевка', '86540'),
             ('АПАНАСЕНКОВСКИЙ', 'Дивное', '86555'),
             ('ТРУНОВСКИЙ', 'Донское', '86546'),
             ('КОЧУБЕЕВСКИЙ', 'Кочубеевское', '86550'),
             ('КРАСНОГВАРДЕЙСКИЙ', 'Красногвардейское', '86541'),
             ('АНДРОПОВСКИЙ', 'Курсавка', '86556'),
             ('ЛЕВОКУМСКИЙ', 'Левокумское', '86543'),
             ('ТУРКМЕНСКИЙ', 'Летняя Ставка', '86565'),
             ('НОВОСЕЛИЦКИЙ', 'Новоселицкое', '86548'),
             ('СТЕПНОВСКИЙ', 'Степное', '86563'),
             ('ПРЕДГОРНЫЙ', 'Ессентукская', '87961'),
             ('КУРСКИЙ', 'Курская', '87964'),
             ('Ессентуки', 'Ессентуки', '87934'),
             ('Железноводск', 'Железноводск', '87932'),
             ('Кисловодск', 'Кисловодск', '87937'),
             ('Лермонтов', 'Лермонтов', '87935'),
             ('Невинномысск', 'Невинномысск', '86554'),
             ('Пятигорск', 'Пятигорск', '8793'),
             ('Ставрополь', 'Ставрополь', '8652'))
    for code in codes:
        if (code[0].lower() in area.lower()) or (code[1].lower() in area.lower()):
            return code[2]
    return False

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

def update_abon_dsl_account(cursor, account_name, phone_number):
    command = '''
    UPDATE abon_dsl
    SET account_name = "{}"
    WHERE phone_number = {}
    '''.format(account_name, phone_number)
    try:
        cursor.execute(command)
    except Exception as ex:
        print(ex)
    else:
        cursor.execute('commit')    

def find_phone_account(accounts): 
    onyma = Onyma.get_onyma()
    result = []
    for account in accounts:
        print('Поиск {} в Ониме'.format(account[0]))
        argus_id = Onyma.find_argus_id(onyma, account[1])
        if argus_id in argus_phone:
            result.append((account[0], argus_phone[argus_id]))
            print('Учетное имя - {}, телефон - {}'.format(result[-1][0], result[-1][1]))
    return result # [(account_name, phone_number), ...]

def argus_files(file_list):
    connect = MySQLdb.connect(host=Settings.db_host, user=Settings.db_user, password=Settings.db_password, db=Settings.db_name, charset='utf8')
    cursor = connect.cursor()     
    
    # Подготовка регулярных выражений
    re_phone = re.compile(r'\((\d+)\)(.+)') # Код, телефон
    re_address = re.compile(r'(.*),\s?(.*),\s?(.*),\s?(.*),\s?кв\.(.*)') # Район, нас. пункт, улица, дом, кв.
    re_board = re.compile(r'.+0.(\d+)') # Board
    re_onyma = re.compile(r'.+Onyma\s*(\d+)') # Onyma id
    
    # Обработка csv-файлов
    for file in file_list:
        if file.split('.')[-1] != 'csv':
            continue
        print('Обработка файла {}'.format(file))
        with open(file,  encoding='windows-1251') as f:
            reader = csv.reader(f, delimiter=';')
            for row in reader:
                if len(row) < 8:
                    continue
                cell_hostname = row[2].replace('=', '').replace('"', '')
                cell_board = row[4].replace('=', '').replace('"', '')
                cell_port = row[5].replace('=', '').replace('"', '')
                cell_phone = row[8].replace('=', '').replace('"', '')
                cell_address = row[10].replace('=', '').replace('"', '')
                cell_onyma = row[12].replace('=', '').replace('"', '')
                cell_type = row[7].replace('=', '').replace('"', '')
                if cell_type not in ('Телефон', 'Прямой провод') or not re_phone.search(cell_phone) or not re_address.search(cell_address):
                            continue
                
                hostname = '"{}"'.format(cell_hostname)                                     # hostname
                board = re_board.search(cell_board).group(1)                                # board
                port = cell_port                                                            # port
                area_code = re_phone.search(cell_phone).group(1)                            # код телефона
                phone = re_phone.search(cell_phone).group(2)                                # телефон
                phone_number = '"{}{}"'.format(area_code, phone).replace('ПППП', 'ПП')      # полный номер (код+телефон)
                area = '"{}"'.format(re_address.search(cell_address).group(1))              # район
                locality = '"{}"'.format(re_address.search(cell_address).group(2))          # нас. пункт
                street = '"{}"'.format(re_address.search(cell_address).group(3))            # улица
                house_number = '"{}"'.format(re_address.search(cell_address).group(4))      # номер дома
                apartment_number = '"{}"'.format(re_address.search(cell_address).group(5))  # квартира
                try:
                    onyma_equ = re_onyma.search(cell_onyma).group(1)                        # onyma equ
                except:
                    onyma_equ = ''
                    
                
                # Вставка данных в таблицу
                command = '''
                INSERT INTO abon_dsl
                (phone_number, area, locality, street, house_number, apartment_number, hostname, board, port)
                VALUES
                ({}, {}, {}, {}, {}, {}, {}, {}, {})
                '''.format(phone_number, area, locality, street, house_number, apartment_number, hostname, board, port)
                try:
                    cursor.execute(command)
                except Exception as ex:
                    print(ex)
                    continue
                else:
                    cursor.execute('commit')
                #print('{}, {}, {}, {}, {}, {}, {}, {}, {}'.format(phone_number, area, locality, street, house_number, apartment_number, hostname, board, port))
                argus_phone[onyma_equ] = phone_number.replace('"', '')
    connect.close()
 
def onyma_file(file_list):
    connect = MySQLdb.connect(host=Settings.db_host, user=Settings.db_user, password=Settings.db_password, db=Settings.db_name, charset='utf8')
    cursor = connect.cursor()
    
    for file in file_list:
        if file.split('.')[-1] != 'csv':
            continue
        print('Обработка файла {}'.format(file))
        with open(file,  encoding='windows-1251') as f:
            reader = csv.reader(f, delimiter=';')                    
            for row in reader:
                if (row[41] != 'deleted') and (re.search(r'[xA]DSL', row[37])) and (row[23] == 'SSG-подключение'):
                    area_code = get_area_code(row[1])
                    if area_code is False:
                        continue
                    if (len(row[7]) == 10) or (area_code in row[7]):
                        phone_number = '"{}"'.format(row[7])
                    elif (len(row[7]) < 10) and (len(row[7]) > 0):
                        phone_number = '"{}{}"'.format(area_code, row[7]).replace('-', '')
                    else:
                        continue
                    
                    # Определение учетного имени
                    account_name = row[21]
                    onyma_id = row[19]
                    if phone_number not in phones:
                        phones[phone_number] = []
                    phones[phone_number].append((account_name, onyma_id))
                    
            for phone in phones:
                if len(phones[phone]) == 1:
                    update_abon_dsl_account(cursor, phones[phone][0], phone_number)
                else:
                    find_phones = find_phone_account(phones[phone])
                    for find_phone in find_phones:
                        update_abon_dsl_account(cursor, find_phone[0], find_phone[1])
    connect.close()
                    
                    
                    
                    

def run(arguments):
    connect = MySQLdb.connect(host=Settings.db_host, user=Settings.db_user, password=Settings.db_password, db=Settings.db_name, charset='utf8')
    cursor = connect.cursor()         
    onyma = Onyma.get_onyma()
    
    for current_id in arguments:
        argus_id = Onyma.find_argus_id(onyma, current_id)
        if argus_id in argus_phone:
            account_name = onyma_account[current_id]
            
            # Установка номера карты
            command = '''
            UPDATE abon_dsl
            SET account_name = "{}"
            WHERE phone_number = "{}"
            '''.format(account_name, argus_phone[argus_id])
            try:
                cursor.execute(command)
            except:
                pass
            else:
                cursor.execute('commit')
                
            # Определение тарифного плана
            speed = Onyma.find_account_speed(onyma, account_name)
            if (speed is not False) and (speed != -1):
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
            else:
                onyma = Onyma.get_onyma()
            
            # Определение bill, dmid, tmid
            account_param = Onyma.find_account_param(onyma, account_name)
            if (account_param is not False) and (account_param != -1):
                bill, dmid, tmid = account_param
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
            else:
                onyma = Onyma.get_onyma()
                continue
                
            # Определение IPTV
            tv = Onyma.update_tv(onyma, bill, datetime.date.today())
            if tv == -1:
                onyma = Onyma.get_onyma()
            if tv:
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
            
    connect.close()

    
def main():
    print("Начало работы: {}\n".format(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    create_abon_dsl()
    create_abon_onyma()
    
    # Обработка файлов в директории in/argus/
    file_list = ['in' + os.sep + 'argus' + os.sep + x for x in os.listdir('in' + os.sep + 'argus')]
    argus_files(file_list)
    
    # Обработка файлов в директории in/onyma/
    file_list = ['in' + os.sep + 'onyma' + os.sep + x for x in os.listdir('in' + os.sep + 'onyma')]
    onyma_file(file_list)
    
    #print('Получение данных из Онимы...')
    #arguments = [onyma_id[x::Settings.threads_count]  for x in range(0,  Settings.threads_count)]
    #with ThreadPoolExecutor(max_workers=Settings.threads_count) as executor:
        #result = executor.map(run, arguments)
    
    print("\nЗавершение работы: {}".format(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))