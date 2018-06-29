# coding: utf-8

import datetime
import time
import MySQLdb
import os
from resources import Settings
from resources import DslamHuawei
from concurrent.futures import ThreadPoolExecutor

DslamHuawei.LOGGING = True

err_file_sql = 'error-port_status-sql.txt'
def create_error_file():
    current_time = datetime.datetime.now()
    with open('error_files' + os.sep + err_file_sql, 'w') as f:
            f.write(current_time.strftime('%Y-%m-%d %H:%M') + '\n')

def check_tables():
    connect = MySQLdb.connect(host=Settings.db_host, user=Settings.db_user, password=Settings.db_password, db=Settings.db_name, charset='utf8')
    cursor = connect.cursor() 
    table = '''
    CREATE TABLE IF NOT EXISTS data_dsl (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT,
    hostname VARCHAR(50) NOT NULL,
    board TINYINT UNSIGNED NOT NULL,
    port TINYINT UNSIGNED NOT NULL,
    up_snr FLOAT(3,1),
    dw_snr FLOAT(3,1),
    up_att FLOAT(3,1),
    dw_att FLOAT(3,1),
    max_up_rate SMALLINT UNSIGNED,
    max_dw_rate SMALLINT UNSIGNED,
    up_rate SMALLINT UNSIGNED,
    dw_rate SMALLINT UNSIGNED,
    datetime DATETIME NOT NULL,
    CONSTRAINT pk_data_dsl PRIMARY KEY (id)
    )'''
    try:
        cursor.execute(table)
    except Exception as ex:
        with open('error_files' + os.sep + err_file_sql, 'a') as f:
            f.write(str(ex) + '\n')            
    else:
        cursor.execute('commit')
    connect.close()

def delete_old_records():
    connect = MySQLdb.connect(host=Settings.db_host, user=Settings.db_user, password=Settings.db_password, db=Settings.db_name, charset='utf8')
    cursor = connect.cursor() 
    table = '''
    DELETE
    FROM data_dsl
    WHERE CAST(datetime AS DATE) < DATE_ADD(CURRENT_DATE(), INTERVAL -{} DAY)
    '''.format(Settings.days)
    try:
        cursor.execute(table)
    except:
        pass
    else:
        cursor.execute('commit')
    connect.close()    
    
def connect_dslam(host):
    #Создание объекта dslam
    ip = host[0]
    model = host[1]
    for i in range(1, 4):
        print('Попытка подключения к {} №{}'.format(ip, i))
        if model == '5600':
            try:
                dslam = DslamHuawei.DslamHuawei5600(ip, Settings.login_5600, Settings.password_5600, 20)
            except:
                print('Не удалось подключиться к {}'.format(ip))
                if i == 3:
                    return None
                else:
                    time.sleep(60)
                    continue
        elif model == '5616':
            try:
                dslam = DslamHuawei.DslamHuawei5616(ip, Settings.login_5616, Settings.password_5616, 20)
            except:
                print('Не удалось подключиться к {}'.format(ip))
                if i == 3:
                    return None
                else:
                    time.sleep(60)
                    continue
        else:
            print('Не знакомый тип DSLAM {}'.format(ip))
            return None
        break
    return dslam

def run(arguments):
    connect = MySQLdb.connect(host=Settings.db_host, user=Settings.db_user, password=Settings.db_password, db=Settings.db_name, charset='utf8')
    cursor = connect.cursor()
    
    current_time = arguments[0]
    host = arguments[1]
    dslam = connect_dslam(host)
    if dslam is None:
        return False
    hostname = dslam.get_info()['hostname']
    ip = dslam.get_info()['ip']
    print('Обработка DSLAM ip {}, hostname {}'.format(ip, hostname))
    for board in dslam.boards:
        paramConnectBoard = dslam.get_line_operation_board(board)
        if paramConnectBoard == False:
            continue
        for port in range(0,dslam.ports):
            param = paramConnectBoard[port]
            if param['up_snr'] == '-':
                command = '''
                INSERT INTO data_dsl
                (hostname, board, port, datetime)
                VALUES
                ("{}", {}, {}, "{}")
                '''.format(hostname, board, port, current_time.strftime('%Y-%m-%d %H:%M:%S'))
                try:
                    cursor.execute(command)
                except Exception as ex:
                    with open('error_files' + os.sep + err_file_sql, 'a') as f:
                        f.write(str(ex) + '\n')                  
                else:
                    cursor.execute('commit')
                    continue
            command = '''
            INSERT INTO data_dsl
            (hostname, board, port, up_snr, dw_snr, up_att, dw_att, max_up_rate, max_dw_rate, up_rate, dw_rate, datetime)
            VALUES
            ("{}", {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, "{}")
            '''.format(hostname, board, port, param['up_snr'], param['dw_snr'], param['up_att'], param['dw_att'], param['max_up_rate'], param['max_dw_rate'], param['up_rate'], param['dw_rate'], current_time.strftime('%Y-%m-%d %H:%M:%S'))
            try:
                cursor.execute(command)
            except Exception as ex:
                with open('error_files' + os.sep + err_file_sql, 'a') as f:
                    f.write(str(ex) + '\n')                  
            else:
                cursor.execute('commit')
    connect.close()
    print('DSLAM ip {}, hostname {} обработан'.format(ip, hostname))
    del dslam
    return True


def main():
    print('Программа запущена...\n')
    # Создание файла для записи ошибок
    create_error_file()
    # Проверка есть ли таблица
    check_tables()
    
    # Интервал запуска
    run_interval = int((24*60*60)/Settings.number_of_launches)
    delete_record_date = datetime.datetime.now().date() - datetime.timedelta(days=1)
    
    # Запуск основного кода
    while True:
        current_time = datetime.datetime.now()
        if 'run_time' in locals():
            if (current_time - run_time).seconds < run_interval:
                time.sleep(300)
                continue
        print('--- Начало обработки ({}) ---\n'.format(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
        arguments = [(current_time, host) for host in Settings.hosts]
        with ThreadPoolExecutor(max_workers=Settings.threads) as executor:
            executor.map(run, arguments)
        run_time = current_time
        
        # Удаление старых записей
        if delete_record_date != run_time.date():
            delete_old_records()
            delete_record_date = run_time.date()
            
        print('--- Обработка завершена ({}) ---\n'.format(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
