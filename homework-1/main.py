"""Скрипт для заполнения данными таблиц в БД Postgres."""

from pathlib import Path
import csv
import psycopg2
from psycopg2 import Error


def main():
	'''
	Головная функция.
	
	1. Читаем данные из файлов csv каталога north_data
	2. Создаем подключение к БД
	3. Заносим прочитанные данные в базу данных
	'''
	
	base_dir = Path(__file__).resolve().parent
	data_dir = base_dir.joinpath('north_data')
	
	employees_data = data_dir.joinpath('employees_data.csv')
	customers_data = data_dir.joinpath('customers_data.csv')
	orders_data = data_dir.joinpath('orders_data.csv')
	name_files = (employees_data, customers_data, orders_data)
	
	# Списки словарей из csv
	employees_list = []
	customers_list = []
	orders_list = []
	
	# Формируем списки словарей с данными из csv
	for i in range(3):
		with open(name_files[i], encoding='utf-8') as r_file:
			reader = csv.DictReader(r_file, delimiter=",")
			for row in reader:
				if i == 0:
					employees_list.append(row)
				elif i == 1:
					customers_list.append(row)
				else:
					orders_list.append(row)
	
	name_tables = ('employees', 'customers', 'orders')
	name_bd = 'north'
	
	# Формируем запросы к БД и записываем данные
	try:
		conn = psycopg2.connect(host='localhost', database=name_bd, user='postgres')
		cur = conn.cursor()
		for i in range(3):
			del_statement = f'DELETE FROM {name_tables[i]};'
			cur.execute(del_statement)
			conn.commit()
			if i == 0:
				for employee in employees_list:
					#"employee_id","first_name","last_name","title","birth_date","notes"
					insert_statement = f'INSERT INTO {name_tables[i]} VALUES (%(employee_id)s, %(first_name)s, %(last_name)s, %(title)s, %(birth_date)s, %(notes)s)'
					cur.execute(insert_statement, employee)
			elif i == 1:
				for customers in customers_list:
					#"customer_id", "company_name", "contact_name"
					insert_statement = f'INSERT INTO {name_tables[i]} VALUES (%(customer_id)s, %(company_name)s, %(contact_name)s)'
					cur.execute(insert_statement, customers)
			else:
				for orders in orders_list:
					#"order_id","customer_id","employee_id","order_date","ship_city"
					insert_statement = f'INSERT INTO {name_tables[i]} VALUES (%(order_id)s, %(customer_id)s, %(employee_id)s, %(order_date)s, %(ship_city)s)'
					cur.execute(insert_statement, orders)
	except (Exception, Error) as error:
		print("Не смогли подключиться", error)
	finally:
		if conn:
			conn.commit()
			cur.close()
			conn.close()

if __name__ == '__main__':
	main()
