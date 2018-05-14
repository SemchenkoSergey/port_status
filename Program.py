#!/usr/bin/env python3
# coding: utf8

from resources import Port_Status
from resources import Session_Count
from resources import Make_Abon_Dsl
from resources import Make_Abon_Onyma
import sys
import warnings
warnings.filterwarnings("ignore")


def main():
    while True:
        print('1. Запустить Port_Status (снятие показаний с портов DSLAM)')
        print('2. Запустить Session_Count (получение количества абонентских сессий из Онимы)')
        print('3. Запустить Make_Abon_Dsl (обработка отчетов из Аргуса)')
        print('4. Запустить Make_Abon_Onyma (обработка отчета из Онимы)')
        print('5. Выход из программы')
        number = input('---\n> ')
        if number == '5':
            sys.exit()
        elif number == '1':
            Port_Status.main()
        elif number == '2':
            Session_Count.main()
        elif number == '3':
            Make_Abon_Dsl.main()
        elif number == '4':
            Make_Abon_Onyma.main()
        else:
            print('\n!!! Нужно ввести число от 1 до 5 !!!\n')    


if __name__ == '__main__':
    main()

