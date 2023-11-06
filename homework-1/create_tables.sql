-- SQL-команды для создания таблиц
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS customers;
DROP TABLE IF EXISTS employees;

CREATE TABLE employees
(
    employee_id int PRIMARY KEY,
    first_name varchar(20) NOT NULL,
    last_name varchar(20) NOT NULL
    title varchar(50) NOT NULL,
    birth_date date,
    notes text
);

CREATE TABLE customers
(
    customer_id varchar(5) PRIMARY KEY,
    company_name text,
    contact_name varchar(25)
);

CREATE TABLE orders
(
    order_id int PRIMARY KEY,
    customer_id varchar(5) REFERENCES customers(customer_id) NOT NULL,
    employee_id int REFERENCES employees(employee_id) NOT NULL,
    order_date date,
    ship_city varchar(20)
);

