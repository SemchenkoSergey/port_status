# coding: utf8

from concurrent.futures import ThreadPoolExecutor
from resources import Functions_Port_Status as Func_PS
from resources import Settings
import datetime
import time


print('Программа запущена...\n')
# Создание файла для записи ошибок
Func_PS.create_error_file()
# Проверка есть ли таблица
Func_PS.check_tables()

# Интервал запуска
run_interval = int((24*60*60)/Settings.number_of_launches)

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
        executor.map(Func_PS.run, arguments)
    run_time = current_time
    Func_PS.delete_old_records()
    print('--- Обработка завершена ({}) ---\n'.format(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
