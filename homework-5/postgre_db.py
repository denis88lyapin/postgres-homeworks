import psycopg2
import json
from config import config

class PostgresDB:

    def __init__(self, params: dict) -> None:
        self.conn = psycopg2.connect(**params)
        self.conn.autocommit = True
        self.cur = self.conn.cursor()

    def create_database(self, db_name: str) -> None:
        """
        Создание базы данных
        """
        try:
            self.cur.execute(f'DROP DATABASE {db_name}')
            self.cur.execute(f'CREATE DATABASE {db_name}')
            print(f"БД {db_name} успешно создана")
            params.update({'dbname': db_name})  # Обновляем параметры подключения
            self.conn.close()  # Закрываем текущее соединение
            self.conn = psycopg2.connect(**params)  # Подключаемся к созданной базе данных
            self.conn.autocommit = True
            self.cur = self.conn.cursor()
        except(Exception, psycopg2.DatabaseError) as error:
            print(error)

    def execute_sql_script(self, script_file: str) -> None:
        """Выполняет скрипт из файла для заполнения БД данными."""
        try:
            with open(script_file, 'r', encoding='utf-8') as file:
                script = file.read()
            self.cur.execute(script)
            print(f'Скрипт {script_file} успешно выполнен')
        except(Exception, psycopg2.DatabaseError) as error:
            print(error)
        except FileNotFoundError:
            print(f"Скрипт {script_file} не найден")

    def create_suppliers_table(self):
        """Создает таблицу suppliers."""
        try:
            self.cur.execute('''DROP TABLE IF EXISTS suppliers''')
            self.cur.execute(
                """
                CREATE TABLE suppliers (
                    supplier_id SERIAL PRIMARY KEY,
                    company_name varchar(100) NOT NULL,
                    contact_name varchar(30) NOT NULL,
                    contact_title varchar(30) NOT NULL,
                    address varchar(60),
                    citi varchar(60),
                    region varchar(30),
                    postal_code varchar(30),
                    country varchar(30),
                    phone varchar(30),
                    fax varchar(30),
                    homepage varchar(100))
                    """
                    )
            print("Таблица suppliers успешно создана")
        except(Exception, psycopg2.DatabaseError) as error:
            print(error)

    @staticmethod
    def get_suppliers_data(json_file: str) -> list[dict]:
        """Извлекает данные о поставщиках из JSON-файла и возвращает список словарей с соответствующей информацией."""
        with open(json_file, 'r') as file:
            data = json.load(file)
        suppliers = []
        for supplier in data:
            contact = supplier.get('contact').split(', ')
            address = supplier.get('address').split(';')
            supplier_tmp = {
                'company_name': supplier.get('company_name'),
                'contact_name': contact[0].strip(),
                'contact_title': contact[1].strip(),
                'address': address[4].strip(),
                'citi': address[3].strip(),
                'region': address[1].strip(),
                'postal_code': address[2].strip(),
                'country': address[0].strip(),
                'phone': supplier.get('phone'),
                'fax': supplier.get('fax'),
                'homepage': supplier.get('homepage'),
                'products': supplier.get('products')
            }
            suppliers.append(supplier_tmp)
        return suppliers

    def insert_suppliers_data(self, suppliers: list[dict]) -> None:
        """Добавляет данные из suppliers в таблицу suppliers."""
        try:
            for supplier in suppliers:
                self.cur.execute(
                    '''
                    INSERT INTO suppliers (company_name, contact_name, contact_title, address,
                                           citi, region, postal_code, country, phone, fax, homepage)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                         
                    ''',
                    (supplier.get('company_name'), supplier.get('contact_name'), supplier.get('contact_title'),
                     supplier.get('address'),      supplier.get('citi'),         supplier.get('region'),
                     supplier.get('postal_code'),  supplier.get('country'),      supplier.get('phone'),
                     supplier.get('fax'), supplier.get('homepage'))
                )
            print('Данные в таблицу suppliers успешно добавлены')
        except(Exception, psycopg2.DatabaseError) as error:
            print(error)


    def close_connection(self) -> None:
        """Закрывает соединение с базой данных."""
        self.conn.close()


if __name__ == '__main__':
    params = config()
    db = PostgresDB(params)
    db.create_database('my_new_db')
    db.execute_sql_script("fill_db.sql")
    db.create_suppliers_table()
    d = db.get_suppliers_data('suppliers.json')
    db.insert_suppliers_data(d)
    db.close_connection()
    print(d)


