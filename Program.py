#!/usr/bin/env python3
# coding: utf8

import sys
import warnings

warnings.filterwarnings("ignore")


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
            from resources import Port_Status
            print()
            Port_Status.main()
            sys.exit()
        elif number == '2':
            from resources import Session_Count
            print()
            Session_Count.main()
            sys.exit()
        elif number == '3':
            from resources import Make_Abon_Dsl
            print()
            Make_Abon_Dsl.main()
            sys.exit()
        elif number == '4':
            from resources import Make_Abon_Onyma
            print()
            Make_Abon_Onyma.main()
            sys.exit()
        else:
            print('\n!!! Нужно ввести число от 1 до 5 !!!\n')    


if __name__ == '__main__':
    main()