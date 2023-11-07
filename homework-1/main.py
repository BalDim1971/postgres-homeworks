"""Скрипт для заполнения данными таблиц в БД Postgres."""

from pathlib import Path
import csv
import psycopg2
from psycopg2 import Error

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR.joinpath('north_data')

EMPLOYEES_FILE = DATA_DIR.joinpath('employees_data.csv')
CUSTOMERS_FILE = DATA_DIR.joinpath('customers_data.csv')
ORDERS_FILE = DATA_DIR.joinpath('orders_data.csv')
NAME_FILES = (EMPLOYEES_FILE, CUSTOMERS_FILE, ORDERS_FILE)

NAME_BD = 'north'
NAME_TABLES = ('employees', 'customers', 'orders')


def read_csv(name_file: str):
	list_csv = []
	with open(name_file, encoding='utf-8') as r_file:
		reader = csv.DictReader(r_file, delimiter=",")
		for row in reader:
			list_csv.append(row)
	return list_csv


def main():
	'''
	Головная функция.
	
	1. Читаем данные из файлов csv каталога north_data
	2. Создаем подключение к БД
	3. Заносим прочитанные данные в базу данных
	'''
	
	# Списки словарей из csv
	# 'employees', 'customers', 'orders'
	all_list = [[], [], []]
	
	# Формируем списки словарей с данными из csv
	for i in range(3):
		all_list[i] = read_csv(NAME_FILES[i])
	
	# Формируем запросы к БД и записываем данные
	try:
		conn = psycopg2.connect(host='localhost', database=NAME_BD, user='postgres')
		cur = conn.cursor()
		for i in range(3):
			del_statement = f'DELETE FROM {NAME_TABLES[2-i]};'
			cur.execute(del_statement)

		for i in range(3):
			if i == 0:
				for employee in all_list[0]:
					# "employee_id","first_name","last_name","title","birth_date","notes"
					insert_statement = f'INSERT INTO {NAME_TABLES[i]} VALUES (%(employee_id)s, %(first_name)s, %(last_name)s, %(title)s, %(birth_date)s, %(notes)s)'
					cur.execute(insert_statement, employee)
			elif i == 1:
				for customers in all_list[1]:
					# "customer_id", "company_name", "contact_name"
					insert_statement = f'INSERT INTO {NAME_TABLES[i]} VALUES (%(customer_id)s, %(company_name)s, %(contact_name)s)'
					cur.execute(insert_statement, customers)
			else:
				for orders in all_list[2]:
					# "order_id","customer_id","employee_id","order_date","ship_city"
					insert_statement = f'INSERT INTO {NAME_TABLES[i]} VALUES (%(order_id)s, %(customer_id)s, %(employee_id)s, %(order_date)s, %(ship_city)s)'
					cur.execute(insert_statement, orders)
		conn.commit()
	except Exception as error:
		conn.rollback()
		print(f'Не смогли подключиться {error}')
	finally:
		if conn:
			conn.close()


if __name__ == '__main__':
	main()
