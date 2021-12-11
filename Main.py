import os
import sys
import psycopg2
from config import host, user, password, db_name, port
import csv


def choice_csv():  # Выбор csv файла, по умолчанию идет transactions.csv
    status = True
    while status:
        print("Хотите использовать свой csv файл Y/N")
        choice = input().strip()
        if choice == "Y":
            path = input("введите полный путь к файлу: ")
            status = False
        elif choice == "N":
            path = "csv_files/transactions.csv"
            status = False
        else:
            print("Недопустимое значение")
            os.system('cls')
    return path


def read_csv(path):  # Чтение csv файла # Проверка на правильность столбцов таблицы
    with open(path, encoding="UTF-8") as data:
        lst = [i for i in data.read().split('\n')]
        if lst[0] == 'customer_id,tr_datetime,mcc_code,tr_type,amount,term_id':
            columns = lst[0].split(",")
            del lst[0]
            del lst[-1]
            return lst, columns
        else:
            print("[INFO] CSV файл имеет неверный формат")
            sys.exit()


def exist_test(cur, table_name):  # Проверка на существование таблицы
    schema = table_name.split('.')[0]
    table = table_name.split('.')[1]
    test = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = '" + \
           schema + "' AND table_name = '" + table + "' );"
    cur.execute(test)
    return cur.fetchall()[0]


def fill_db(cur, lst):  # Заполнение таблицы данными из csv файла
    try:
        for i in lst:
            column = i.split(",")
            if len(column) == 6:  # Если строка определенной длины, то записать
                cur.execute(f"INSERT into " +
                            f"transactions (customer_id,tr_datetime,mcc_code_tr_type,amount)"
                            f"values ({int(column[0])},'{column[1]}','{column[2]} ; {column[3]}',{float(column[4])})")
            else:
                print("[INFO] Error while working with PostgreSQL ")
                sys.exit()

    except Exception as _ex:  # Если выпадает какая то ошибка, данные таблицы стираются
        print("[INFO] Error while working with PostgreSQL", _ex)
        cur.execute("TRUNCATE transactions;"  # удалить таблицу
                    "DELETE FROM transactions;")


def create_table(cur):  # Создание таблицы и перезапись
    path = choice_csv()  # НЕ ПРОВЕРЯЕТ СОЗДАНА ЛИ ТАБЛИЦА
    lst, columns = read_csv(path)
    if not (exist_test(cur, "public.transactions")[0]):  # если таблица не существует - создать и заполнить
        print("создание таблицы Transactions")
        cur.execute(f"create table IF NOT EXISTS transactions " +
                    f"({columns[0]} integer, {columns[1]} varchar(20),"
                    f"{columns[2]}_{columns[3]} varchar(20) , {columns[4]} real)")
        fill_db(cur, lst)
        print("Таблица успешно создана и заполнена!")
        print('-----')
    else:  # выбор, оставить таблицу или заполнить заново
        status = True
        while status:
            answer = input(
                "Обнаружена  таблица,хотите перезаписать ее? Y/N: ").strip()
            if answer == "Y":
                cur.execute("TRUNCATE transactions;"  # удалить таблицу
                            "DELETE FROM transactions;")
                fill_db(cur, lst)
                print("таблица успешно перезаписана\n---")
                status = False
            elif answer == "N":
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


def main():
    try:
        status = True
        while status:
            print("Авторизация через (1)файл-конфиг(укажите свои значения в поля в файле config.py) (0)или вручную:")
            choice = input().strip()
            if choice == "0":
                connection = authorize(False)
                cur = connection.cursor()
                status = False
            elif choice == "1":
                connection = authorize(True)
                cur = connection.cursor()
                status = False
            else:
                print("Недопустимое значение")
                os.system('cls')

        create_table(cur)

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

    except Exception as _ex:
        print("[INFO] Error while working with PostgreSQL", _ex)
    finally:

        if connection:
            connection.close()
            cur.close()
            print("[INFO] PostgreSQL connection closed")


if __name__ == "__main__":
    main()
