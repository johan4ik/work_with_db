import os
from functions import create_table_transactions, authorize, variance_output, main_menu


def main():
    try:

        cur, connection = main_menu()

        create_table_transactions(cur)

        variance_output(cur)

    except Exception as _ex:
        print("[INFO] Error while working with PostgreSQL", _ex)
    finally:

        if connection or connection is None:
            connection.close()
            cur.close()
            print("[INFO] PostgreSQL connection closed")


if __name__ == "__main__":
    main()
