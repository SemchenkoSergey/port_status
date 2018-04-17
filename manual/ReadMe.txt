Подготовка компьютера с установленной ОС Debian 9 x64:
------------------------------------------------------
Используемый в программе модуль pexpect работает в ОС Linux. Для chromedriver необходима 64 битная версия системы.
Работа возможно только с DSLAM Huawei MA5600 и Huawei MA5616.


Установка программ и модулей python:
------------------------------------
sudo apt install python3
sudo apt install python3-pip
sudo apt install chromium
sudo apt install mariadb-server
sudo apt install default-libmysqlclient-dev 
sudo pip3 install mysqlclient
sudo pip3 install selenium
sudo pip3 install pexpect
Скачать chromedriver: https://sites.google.com/a/chromium.org/chromedriver/downloads и положить его в /usr/local/bin


Настройка базы данных:
----------------------
sudo mysql -p

Создание пользователя 'inet' с паролем 'inet' (для работы программы)
CREATE USER 'inet'@'localhost' IDENTIFIED BY 'inet';

Наделение пользователя 'inet' всеми полномочиями
GRANT ALL PRIVILEGES ON *.* TO 'inet'@'localhost';

Создание пользователя 'operator' с паролем 'operator' (для выполнения запросов)
CREATE USER 'operator'@'%' IDENTIFIED BY 'operator';

Наделение пользователя 'operator' возможностью выполнять запрос SELECT
GRANT SELECT ON *.* TO 'operator'@'%';

Обновление прав доступа
FLUSH PRIVILEGES;

Создание таблицы для работы программы
CREATE DATABASE inet CHARACTER SET utf8;

Выход
exit

Подключение к базе данных 'inet' под учеткой 'operator'
mysql -u operator -poperator inet


Настройки программ:
-------------------
Файл настроек 'resources\Settings.py'
Нужно заполнить логины/пароли (кавычки не убирать)
login_5600 = ''
password_5600 = ''
login_5616 = ''
password_5616 = ''
onyma_login = ''
onyma_password = ''

Так же заполнить список DSLAM ('ip', 'model'), например
hosts = (('172.26.194.12', '5600'),
         ('172.26.194.47', '5600'),
		 ('172.26.194.38', '5616'),
		 ('172.26.194.42', '5616'))


Запуск программ:
----------------
Программы не имеют графического интерфейса. Для их запуска необходимо в терминале перейти в директорию программы и выполнить одну из команд:
python3 Port_Status.py (запуск программы снятия показаний с портов DSLAM)
python3 Session_Count.py (запуск программы подсчета сессий)
python3 Make_Abon.py (запуск программы формирования таблицы с данными об абонентах)

Port_Status.py собирает данные независимо от других программ.
Перед запуском Session_Count.py и выполнением запросов к базе данных необходимо сформировать таблицу с данными об абонентах (дать отработать программе Make_Abon.py).


Отчеты из Аргуса и Онимы:
-------------------------
Отчеты должны быть в формате csv и могут иметь любое имя.
Отчет из Онимы поместить в директорию 'in\onyma'
Отчеты из Аргуса поместить в директорию 'in\argus'


Использование базы данных:
--------------------------
Для подключения к базе данных в терминале выполнить команду
mysql -u operator -poperator inet

Скопировать любой SQL-запрос из файла и вставить в окно терминала. Дождаться вывода.