"""Скрипт для заполнения данными таблиц в БД Postgres."""
import csv
import os
import psycopg2

employees_data = os.path.join('north_data', 'employees_data.csv')
customers_data = os.path.join('north_data', 'customers_data.csv')
orders_data = os.path.join('north_data', 'orders_data.csv')

def reader_csv_file(path):
    """
    Функция для чтения данных из csv - файлов
    """
    data_list = []
    with open(path, 'r', encoding='utf-8') as file:
        data = csv.DictReader(file)
        for row in data:
            data_list.append(row)
    return data_list


def main():
    employees = reader_csv_file('north_data/employees_data.csv')
    customers = reader_csv_file('north_data/customers_data.csv')
    orders = reader_csv_file('north_data/orders_data.csv')

    conn = psycopg2.connect(
        user='postgres',
        password="277710",
        host='localhost',
        database='north')
    try:
        with conn:
            # Запись данных в таблицу employees
            with conn.cursor() as cur:
                for emp in employees:
                    cur.execute("INSERT INTO employees VALUES (%s, %s, %s, %s, %s, %s)",
                                (emp["employee_id"], emp["first_name"], emp["last_name"], emp["title"], emp["birth_date"], emp["notes"]))

            # Запись данных в таблицу customers
            with conn.cursor() as cur:
                for customer in customers:
                    cur.execute("INSERT INTO customers VALUES (%s, %s, %s)",
                                (customer["customer_id"], customer["company_name"], customer["contact_name"]))

            # Запись данных в таблицу orders
            with conn.cursor() as cur:
                for order in orders:
                    cur.execute("INSERT INTO orders VALUES (%s, %s, %s, %s, %s)",
                                (order["order_id"], order["customer_id"], order["employee_id"], order["order_date"], order["ship_city"]))

    finally:
        conn.close()



if __name__ == '__main__':
    main()



