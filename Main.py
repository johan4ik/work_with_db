import psycopg2
from config import host, user, password, db_name
import csv

try:
    # connect to exist database
    connection = psycopg2.connect(
        host=host,
        user=user,
        password=password,
        database=db_name
    )
    connection.autocommit = True
    cur = connection.cursor()

    # Автоматическое создание и заполнение таблицы gender_train

    with open("цсвшки/gender_train_cut.csv", encoding="UTF-8") as data:
        lst = [i for i in data.read().split('\n')]
        columns = lst[0].split(",")
        cur.execute(f"CREATE TABLE gender_train ({columns[0]} INTEGER ," +
                    f"{columns[1]} INTEGER)")
        del lst[0]
        for i in lst:
            columns = i.split(",")
            cur.execute(f"INSERT INTO " +
                        f"gender_train (customer_id,gender) "
                        f"VALUES ({int(columns[0])},{int(columns[1])})")

    # Автоматическое создание и заполнение таблицы tr_mcc_codes

    with open("цсвшки/tr_mcc_codes.csv", encoding="UTF-8") as data:
        lst = [i for i in data.read().split('\n')]
        columns = lst[0].split(";")
        cur.execute(f"CREATE TABLE tr_mcc_codes ({columns[0]} INTEGER ," +
                    f"{columns[1]} VARCHAR(300))")
        del lst[0]
        for i in lst:
            columns = i.split(";")
            cur.execute(f"INSERT INTO " +
                        f"tr_mcc_codes (mcc_code,mcc_description) "
                        f"VALUES ({int(columns[0])},'{columns[1]}')")

    # Автоматическое создание и заполнение таблицы tr_types

    with open("цсвшки/tr_types.csv", encoding="UTF-8") as data:
        lst = [i for i in data.read().split('\n')]
        columns = lst[0].split(";")
        cur.execute(f"CREATE TABLE tr_types ({columns[0]} INTEGER ," +
                    f"{columns[1]} VARCHAR(300))")
        del lst[0]
        for i in lst:
            columns = i.split(";")
            cur.execute(f"INSERT INTO " +
                        f"tr_types (tr_type,tr_description) "
                        f"VALUES ({int(columns[0])},'{columns[1]}')")

    # Автоматическое создание и заполнение таблицы transactions

    with open("цсвшки/transactions_cut.csv", encoding="UTF-8") as data:
        lst = [i for i in data.read().split('\n')]
    columns = lst[0].split(",")
    cur.execute(f"create table transactions " +
                f"({columns[0]} integer, {columns[1]} varchar(20),"
                f"{columns[2]}_{columns[3]} varchar(20) , {columns[4]} real)")

    del lst[0]
    for i in lst:
        col = i.split(",")
        cur.execute(f"INSERT into " +
                    f"transactions (customer_id,tr_datetime,mcc_code_tr_type,amount)"
                    f"values ({int(col[0])},'{col[1]}','{col[2]} ; {col[3]}',{float(col[4])})")

    cur.execute("select variance(a.amount) " +
                "from(select customer_id,count(mcc_code_tr_type),amount "
                "from transactions "
                "group by customer_id,mcc_code_tr_type,amount "
                "having amount<0 and count(mcc_code_tr_type)>=10) as a")

    print("Дисперсия по столбцу amount:")
    variance = cur.fetchone()
    with open("цсвшки/variance.csv", "w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["variance"])
        writer.writerow(variance)

    print(*variance)

except Exception as _ex:
    print("[INFO] Error while working with PostgreSQL", _ex)
finally:
    if connection:
        connection.close()
        cur.close()
        print("[INFO] PostgreSQL connection closed")
