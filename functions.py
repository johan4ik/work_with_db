import os
import sys
import psycopg2
import csv
from config import host, user, password, db_name, port


def drop_table(cur):
    cur.execute("TRUNCATE transactions;"  # удалить данные из таблицы
                "DELETE FROM transactions;")


def choose_csv():
    status = True
    while status:
        answer = input("Хотите использовать свой csv файл Y/N: ").strip()
        if answer == "Y":
            path = input("введите полный путь к файлу: ").strip()
            status = False
        elif answer == "N":
            path = "csv_files/transactions.csv"
            status = False
        else:
            print("Недопустимое значение")
            os.system('cls')
    return path


def read_csv(path):  # Чтение CSV файла, первичная проверка на соответствие название столбцов таблицы
    with open(path, encoding="UTF-8") as reader:
        lines = [i for i in reader.read().split('\n')]
        if lines[0] != 'customer_id,tr_datetime,mcc_code,tr_type,amount':
            print("[INFO] CSV файл имеет неверный формат")
            sys.exit()
        else:
            columns = lines[0].split(",")
            del lines[0]
            del lines[-1]
            return lines, columns  # Возвращает кортеж из списка столбцов и списка строк


def is_exist_table(cur, table_name):
    schema = table_name.split('.')[0]
    table = table_name.split('.')[1]
    test = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = '" + \
           schema + "' AND table_name = '" + table + "' );"
    cur.execute(test)
    return cur.fetchall()[0]


def fill_table(cur, lines):
    try:
        for line in lines:
            row = line.split(",")
            if len(row) == 5:
                cur.execute(f"INSERT into " +
                            f"transactions (customer_id,tr_datetime,mcc_code_tr_type,amount)"
                            f"values ({int(row[0])},'{row[1]}','{row[2]} ; {row[3]}',{float(row[4])})")
            else:
                print("[INFO] Входная строка имеет неверный формат")
                drop_table(cur)
                sys.exit()

    except Exception as _ex:  # Если во время заполнения таблицы происходит ошибка
        print("[INFO] Error while working with PostgreSQL", _ex)
        drop_table(cur)
        sys.exit()


def create_table(cur, columns):
    cur.execute(f"create table IF NOT EXISTS transactions " +
                f"({columns[0]} integer, {columns[1]} varchar(20),"
                f"{columns[2]}_{columns[3]} varchar(20) , {columns[4]} real)")


def create_table_transactions(cur):
    path = choose_csv()  # путь к таблице
    lines, columns = read_csv(path)  # получаем строки и столбцы
    if not is_exist_table(cur, "public.transactions")[0]:  # Если таблица не существует, создать и заполнить
        print("Создание таблицы...")
        create_table(cur, columns)
        fill_table(cur, lines)
        print("Таблица успешно была создана и заполнена!")
    else:  # Если таблица существует, можно перезаписать данные
        status = True
        while status:
            answer = input("Обнаружена таблица, хотите перезаписать данные? Y/N: ").strip()
            if answer == "Y":  # Очищаем таблицу и заново заполняем
                print("Данные перезаписываются...")
                drop_table(cur)
                fill_table(cur, lines)
                print("данные успешно перезаписаны!")
                status = False
            elif answer == "N":  # Ничего не делаем
                print("---")
                status = False
            else:
                print("Недопустимое значение")
                os.system('cls')


def calculate_variance(cur, choice):  # Нахождение дисперсии
    cur.execute("select variance(a.amount) " +
                "from(select customer_id,count(mcc_code_tr_type),amount "
                "from transactions "
                "group by customer_id,mcc_code_tr_type,amount "
                "having amount<0 and count(mcc_code_tr_type)>=10) as a")

    variance = cur.fetchone()
    if choice == "1" or choice == "2":  # Записать в csv
        with open("csv_files/variance.csv", "w", newline="") as csv_writer:
            writer = csv.writer(csv_writer)
            writer.writerow(["variance"])
            writer.writerow(variance)

    return variance[0]


def authorize(status):  # Авторизация
    if not status:
        connection = psycopg2.connect(  # подключение вручную
            host=input("Введите поле host: ").strip(),
            user=input("Введите поле user: ").strip(),
            password=input("Введите пароль: ").strip(),
            database=input("Введите название базы данных: ").strip(),
            port=input("Введите поле port: ").strip()
        )
    else:  # подключение по конфигу
        connection = psycopg2.connect(
            host=host,
            user=user,
            password=password,
            database=db_name,
            port=port
        )

    connection.autocommit = True
    return connection


def variance_output(cur):  # Куда вывести значение дисперсии
    status = True
    while status:
        choice = input(
            "Вывести значение дисперсии на экран - 0\nЗаписать значение в csv-файл - 1\nоба варианта - 2\n").strip()
        if choice == "0":
            variance = calculate_variance(cur, "0")
            print(f"Дисперсия : {variance}")
            status = False
        elif choice == "1":
            variance = calculate_variance(cur, "1")
            print("Файл успешно был создан!")
            status = False
        elif choice == "2":
            variance = calculate_variance(cur, "2")
            print(f"Дисперсия : {variance}")
            print("Файл успешно был создан!")
            status = False
        else:
            print("Недопустимое значение")
            os.system('cls')


def main_menu():

    status = True
    while status:
        print("Авторизация через (1)файл-конфиг(укажите свои значения в поля в файле config.py) (0)или вручную:")
        choice = input().strip()
        if choice == "0":
            connection = authorize(False)
            status=False

        elif choice == "1":
            connection = authorize(True)
            status = False
        else:
            print("Недопустимое значение")
            os.system('cls')

    cur = connection.cursor()
    return cur, connection
