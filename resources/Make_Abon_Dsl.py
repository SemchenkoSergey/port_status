# coding: utf-8

import os
import re
import csv
import datetime
import MySQLdb
from resources import Settings

onyma_argus = {}

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

def argus_abon_dsl(file_list):
    connect = MySQLdb.connect(host=Settings.db_host, user=Settings.db_user, password=Settings.db_password, db=Settings.db_name, charset='utf8')
    cursor = connect.cursor()        
    
    # Подготовка регулярных выражений
    re_phone = re.compile(r'\((\d+)\)(.+)') # Код, телефон
    re_address = re.compile(r'(.*),\s?(.*),\s?(.*),\s?(.*),\s?кв\.(.*)') # Район, нас. пункт, улица, дом, кв.
    re_board = re.compile(r'.+0.(\d+).') # Board
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
                
                hostname = '"{}"'.format(cell_hostname)                                     # hostname
                board = re_board.search(cell_board).group(1)                                # board
                port = cell_port                                                            # port
                area_code = re_phone.search(cell_phone).group(1)                            # код телефона
                phone = re_phone.search(cell_phone).group(2)                                # телефон
                phone_number = '"{}{}"'.format(area_code, phone)                            # полный номер (код+телефон)
                area = '"{}"'.format(re_address.search(cell_address).group(1))              # район
                locality = '"{}"'.format(re_address.search(cell_address).group(2))          # нас. пункт
                street = '"{}"'.format(re_address.search(cell_address).group(3))            # улица
                house_number = '"{}"'.format(re_address.search(cell_address).group(4))      # номер дома
                apartment_number = '"{}"'.format(re_address.search(cell_address).group(5))  # квартира
                onyma_id = re_onyma.search(cell_onyma).group(1)                             # onyma id
                
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
                onyma_argus[onyma_id] = phone_number
    connect.close()
 
def onyma_abon_dsl(file_list):
    connect = MySQLdb.connect(host=Settings.db_host, user=Settings.db_user, password=Settings.db_password, db=Settings.db_name, charset='utf8')
    cursor = connect.cursor()
    
    for file in file_list:
        if file.split('.')[-1] != 'csv':
            continue
        print('Обработка файла {}'.format(file))
        with open(file,  encoding='windows-1251') as f:
            reader = csv.reader(f, delimiter=';')
            for row in reader:
                if row[41] != 'deleted' and re.search(r'[xA]DSL', row[37]):
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
    connect.close()


def main():
    print("Начало работы: {}\n".format(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    create_error_files()
    create_abon_dsl()
    
    # Обработка файлов в директории in/argus/
    file_list = ['in' + os.sep + 'argus' + os.sep + x for x in os.listdir('in' + os.sep + 'argus')]
    argus_abon_dsl(file_list)
    
    # Обработка файлов в директории in/onyma/
    file_list = ['in' + os.sep + 'onyma' + os.sep + x for x in os.listdir('in' + os.sep + 'onyma')]
    onyma_abon_dsl(file_list)
    
    print("\nЗавершение работы: {}".format(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
