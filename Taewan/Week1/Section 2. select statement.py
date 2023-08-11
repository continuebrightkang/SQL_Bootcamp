import sys
import pandas as pd
import psycopg2


def connect():
    conn = None

    try:
        print('Connecting…')
        conn = psycopg2.connect("dbname=dvdrental user=postgres password=1234")
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1)
    print("All good, Connection successful!")
    return conn


conn = connect()
df1 = pd.read_sql_query('select * from actor', conn)
df2 = pd.read_sql_query('select * from film', conn)
df3 = pd.read_sql_query('select title,length from film', conn)


# challenge 1
c1 = pd.read_sql_query('SELECT first_name,last_name,email FROM customer', conn)


# it's like pd.unique()
df4 = pd.read_sql_query('SELECT DISTINCT release_year FROM film', conn)
df5 = pd.read_sql_query('SELECT DISTINCT(release_year) FROM film', conn)
df6 = pd.read_sql_query('SELECT DISTINCT(rental_rate) FROM film', conn)


# challenge 2
c2 = pd.read_sql_query('SELECT DISTINCT(rating) FROM film', conn)


df7 = pd.read_sql_query('SELECT COUNT(rental_rate) FROM film', conn)
df8 = pd.read_sql_query('SELECT COUNT(DISTINCT(rating)) FROM film', conn)


df9 = pd.read_sql_query('SELECT * FROM film WHERE rating = \'R\'', conn)
df10 = pd.read_sql_query('SELECT * FROM customer WHERE first_name=\'Jared\'', conn)
df11 = pd.read_sql_query("SELECT * FROM customer WHERE first_name='Jared'", conn)
# both \', ' are okay

df12 = pd.read_sql_query("SELECT * FROM film WHERE rental_rate>4", conn)
df13 = pd.read_sql_query("SELECT * FROM film WHERE rental_rate>4 AND replacement_cost>=19.99 AND rating='R'", conn)
df14 = pd.read_sql_query("SELECT COUNT(*) FROM film WHERE rental_rate>4 AND replacement_cost>=19.99 AND rating='R'", conn)
df15 = pd.read_sql_query("SELECT COUNT(*) FROM film WHERE rating='R' OR rating='PG-13'", conn)
df16 = pd.read_sql_query("SELECT * FROM film WHERE rating!='R'", conn)


# challenge 3
temp = pd.read_sql_query("SELECT * FROM customer", conn)
ch3_1 = pd.read_sql_query("SELECT email FROM customer WHERE first_name='Nancy' AND last_name='Thomas'", conn)
ch3_2 = pd.read_sql_query("SELECT description FROM film WHERE title='Outlaw Hanky'", conn)
temp2 = pd.read_sql_query("SELECT * FROM address", conn)
ch3_3 = pd.read_sql_query("SELECT phone FROM address WHERE address='259 Ipoh Drive'", conn)


df17 = pd.read_sql_query("SELECT * FROM customer ORDER BY first_name", conn)
df18 = pd.read_sql_query("SELECT * FROM customer ORDER BY first_name DESC", conn)
df19 = pd.read_sql_query("SELECT * FROM customer ORDER BY store_id", conn)
df20 = pd.read_sql_query("SELECT store_id,first_name,last_name FROM customer ORDER BY store_id", conn)
df21 = pd.read_sql_query("SELECT store_id,first_name,last_name FROM customer ORDER BY store_id,first_name", conn)
df22 = pd.read_sql_query("SELECT store_id,first_name,last_name FROM customer ORDER BY store_id DESC,first_name", conn)


df23 = pd.read_sql_query("SELECT * FROM payment ORDER BY payment_date DESC LIMIT 5", conn)
df24 = pd.read_sql_query("SELECT * FROM payment WHERE amount!=0.00 ORDER BY payment_date DESC LIMIT 5", conn)
df25 = pd.read_sql_query("SELECT * FROM payment LIMIT 1", conn)


# challenge 4
film = pd.read_sql_query("SELECT * FROM film", conn)
ch4_1 = pd.read_sql_query("SELECT customer_id FROM payment ORDER BY payment_date ASC LIMIT 10", conn)
ch4_2 = pd.read_sql_query("SELECT title,length FROM film ORDER BY length ASC LIMIT 5", conn)
ch4_3 = pd.read_sql_query("SELECT COUNT(*) FROM film WHERE length<=50", conn)


df26 = pd.read_sql_query("SELECT * FROM payment WHERE amount BETWEEN 8 AND 9", conn)
df27 = pd.read_sql_query("SELECT COUNT(*) FROM payment WHERE amount BETWEEN 8 AND 9", conn)
df28 = pd.read_sql_query("SELECT COUNT(*) FROM payment WHERE amount NOT BETWEEN 8 AND 9", conn)
df29 = pd.read_sql_query("SELECT * FROM payment WHERE payment_date BETWEEN '2007-02-01' AND '2007-02-15'", conn)
df30 = pd.read_sql_query("SELECT * FROM payment WHERE payment_date BETWEEN '2007-02-01' AND '2007-02-14'", conn)


df31 = pd.read_sql_query('SELECT DISTINCT(amount) FROM payment ORDER BY amount ', conn)
df32 = pd.read_sql_query('SELECT * FROM payment WHERE amount IN (0.99, 1.98, 1.99)', conn)
df33 = pd.read_sql_query('SELECT COUNT(*) FROM payment WHERE amount IN (0.99, 1.98, 1.99)', conn)
df34 = pd.read_sql_query('SELECT COUNT(*) FROM payment WHERE amount NOT IN (0.99, 1.98, 1.99)', conn)
df35 = pd.read_sql_query("SELECT * FROM customer WHERE first_name IN ('John', 'Jake', 'Julie')", conn)


df36 = pd.read_sql_query("SELECT * FROM customer WHERE first_name LIKE 'J%' AND last_name LIKE 'S%'", conn)
df37 = pd.read_sql_query("SELECT * FROM customer WHERE first_name ILIKE 'j%' AND last_name ILIKE 's%'", conn)
df38 = pd.read_sql_query("SELECT * FROM customer WHERE first_name LIKE '%er%'", conn)
df39 = pd.read_sql_query("SELECT * FROM customer WHERE first_name LIKE '_er%'", conn)
df40 = pd.read_sql_query("SELECT * FROM customer WHERE first_name NOT LIKE '_er%'", conn)
df41 = pd.read_sql_query("SELECT * FROM customer WHERE first_name LIKE 'A%' ORDER BY last_name", conn)
df42 = pd.read_sql_query("SELECT * FROM customer WHERE first_name LIKE 'A%' AND last_name NOT LIKE 'B%' \
    ORDER BY last_name", conn)


# challenge 5
payment = pd.read_sql_query("SELECT * FROM payment", conn)
film = pd.read_sql_query("SELECT * FROM film", conn)
actor = pd.read_sql_query("SELECT * FROM actor", conn)
customer = pd.read_sql_query("SELECT * FROM customer", conn)
address = pd.read_sql_query("SELECT * FROM address", conn)
c5_1 = pd.read_sql_query("SELECT COUNT(amount) FROM payment WHERE amount > 5", conn)
c5_2 = pd.read_sql_query("SELECT COUNT(*) FROM actor WHERE first_name LIKe 'P%'", conn)
c5_3 = pd.read_sql_query("SELECT COUNT(DISTINCT(district)) FROM address", conn)
c5_4 = pd.read_sql_query("SELECT DISTINCT(district) FROM address", conn)
c5_5 = pd.read_sql_query("SELECT COUNT(*) FROM film WHERE rating='R' AND replacement_cost between 5 AND 15", conn)
c5_6 = pd.read_sql_query("SELECT COUNT(*) FROM film WHERE title LIKE '%Truman%'", conn)
