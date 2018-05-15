#!/usr/bin/env python3
# coding: utf8

import os
import sys
import subprocess


def main():
    while True:
        print('-')
        print('1. Снятие показаний с портов DSLAM')
        print('2. Получение количества абонентских сессий из Онимы')
        print('3. Обработка отчетов из Аргуса и Онимы')
        print('4. Получение данных для работы с Онимой (запустить 1 раз)')
        print()
        print('5. Выход из программы')
        number = input('-\n> ')
        if number == '5':
            sys.exit()
        elif number == '1':
            print()
            subprocess.call(['python3', 'resources{}Port_Status.py'.format(os.sep)])
        elif number == '2':
            print()
            subprocess.call(['python3', 'resources{}Session_Count.py'.format(os.sep)])
        elif number == '3':
            print()
            subprocess.call(['python3', 'resources{}Make_Abon_Dsl.py'.format(os.sep)])
        elif number == '4':
            print()
            subprocess.call(['python3', 'resources{}Make_Abon_Onyma.py'.format(os.sep)])
        else:
            print('\n!!! Нужно ввести число от 1 до 5 !!!\n')    


if __name__ == '__main__':
    main()

