# coding: utf8

import os
import re
import csv
import datetime
import MySQLdb
from resources import Settings

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
    re_dsl = re.compile(r'([\w(DSL)-]+)\[[\d\.]+\].+?(\d+)[_/](\d+)_? - (\d+)')
    re_protect = re.compile(r'(ЗП\d+)')
    re_address = re.compile(r'(.+, )?(.+), (.+), (.+), кв. (.+)?')
    
    for file in file_list:
        if file.split('.')[-1] != 'csv':
            continue
        print('Обработка файла {}'.format(file))
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
                    if re_address.search(cell_address).group(1) is not None:
                        area = '"{}"'.format(re_address.search(cell_address).group(1)[:-2])
                    else:
                        area = '"{}"'.format(re_address.search(cell_address).group(2))
                    locality = '"{}"'.format(re_address.search(cell_address).group(2))
                    street = '"{}"'.format(re_address.search(cell_address).group(3))
                    house_number = '"{}"'.format(re_address.search(cell_address).group(4))
                    if re_address.search(cell_address).group(5) is not None:
                        apartment_number = '"{}"'.format(re_address.search(cell_address).group(5))
                    else:
                        apartment_number = MySQLdb.NULL
                else:
                    continue                
                area_code = get_area_code(area)
                if area_code is False:
                    continue
                phone_number = '"{}{}"'.format(area_code, cell_phone)
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
                                f.write('{}: {}\n'.format(phone_number, tariff))
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