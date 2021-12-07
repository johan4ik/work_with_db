import psycopg2
from config import host, user, password, db_name
import csv


def read_csv(path):
    with open(path, encoding="UTF-8") as data:
        lst = [i for i in data.read().split('\n')]
        columns = lst[0].split(",")
        del lst[0]
        del lst[-1]
    return lst, columns


def create_gender_train_table(cur):
    lst, columns = read_csv("csv_files/gender_train_cut.csv")
    cur.execute(f"CREATE TABLE gender_train ({columns[0]} INTEGER ," +
                f"{columns[1]} INTEGER)")
    for i in lst:
        column = i.split(",")
        cur.execute(f"INSERT INTO " +
                    f"gender_train (customer_id,gender) "
                    f"VALUES ({int(column[0])},{int(column[1])})")


def create_tr_mcc_codes_table(cur):
    lst, columns = read_csv("csv_files/tr_mcc_codes.csv")
    cur.execute(f"CREATE TABLE tr_mcc_codes ({columns[0]} INTEGER ," +
                f"{columns[1]} VARCHAR(300))")
    for i in lst:
        column = i.split(";")
        cur.execute(f"INSERT INTO " +
                    f"tr_mcc_codes (mcc_code,mcc_description) "
                    f"VALUES ({int(column[0])},'{column[1]}')")


def create_transactions_table(cur):
    lst, columns = read_csv("csv_files/transactions_cut.csv")
    cur.execute(f"create table transactions " +
                f"({columns[0]} integer, {columns[1]} varchar(20),"
                f"{columns[2]}_{columns[3]} varchar(20) , {columns[4]} real)")
    for i in lst:
        column = i.split(",")
        cur.execute(f"INSERT into " +
                    f"transactions (customer_id,tr_datetime,mcc_code_tr_type,amount)"
                    f"values ({int(column[0])},'{column[1]}','{column[2]} ; {column[3]}',{float(column[4])})")


def create_tr_types_table(cur):
    lst, columns = read_csv("csv_files/tr_types.csv")
    cur.execute(f"CREATE TABLE tr_types ({columns[0]} INTEGER ," +
                f"{columns[1]} VARCHAR(300))")
    for i in lst:
        column = i.split(";")
        cur.execute(f"INSERT INTO " +
                    f"tr_types (tr_type,tr_description) "
                    f"VALUES ({int(column[0])},'{column[1]}')")


def calculate_variance(cur):
    cur.execute("select variance(a.amount) " +
                "from(select customer_id,count(mcc_code_tr_type),amount "
                "from transactions "
                "group by customer_id,mcc_code_tr_type,amount "
                "having amount<0 and count(mcc_code_tr_type)>=10) as a")

    variance = cur.fetchone()
    with open("csv_files/variance.csv", "w", newline="") as csv_writer:
        writer = csv.writer(csv_writer)
        writer.writerow(["variance"])
        writer.writerow(variance)

    return variance[0]


def main():
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

        # create_tr_types_table(cur)
        #
        # create_gender_train_table(cur)
        #
        # create_transactions_table(cur)
        #
        # create_tr_mcc_codes_table(cur)

        variance = calculate_variance(cur)

        print(f"Дисперсия : {variance}")

    except Exception as _ex:
        print("[INFO] Error while working with PostgreSQL", _ex)
    finally:
        if connection:
            connection.close()
            cur.close()
            print("[INFO] PostgreSQL connection closed")


if __name__ == "__main__":
    main()
