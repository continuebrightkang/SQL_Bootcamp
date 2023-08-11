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

"""
common aggregate functions:
    COUNT()
    SUM()
    AVG()
    MAX()
    MIN()
-> Aggregate functions are used to compute against a "returned column of numeric data" from your SELECT statement.
   Calls happen only in SELECT clause or the HAVING clause of the query.
"""

df1 = pd.read_sql_query('SELECT MIN(replacement_cost) FROM film', conn)
# df2 does not work, because it's not a column, it's a value
df2 = pd.read_sql_query('SELECT MIN(replacement_cost),film_id FROM film', conn)
df3 = pd.read_sql_query(
    'SELECT MIN(replacement_cost), MAX(replacement_cost) FROM film', conn)
df4 = pd.read_sql_query('SELECT AVG(replacement_cost) FROM film', conn)
df5 = pd.read_sql_query(
    'SELECT ROUND(AVG(replacement_cost), 2) FROM film', conn)
df6 = pd.read_sql_query('SELECT SUM(replacement_cost) FROM film', conn)

"""
SELECT category_col, AGG(data_col)
FROM table
WHERE conditions
GROUP BY category_col
"""

payment = pd.read_sql_query('SELECT * FROM payment', conn)
"""
SELECT customer_id from payment GROUP BY customer_id is equal as
SELECT DISTINCT customer_id from payment
"""
df7 = pd.read_sql_query(
    'SELECT customer_id, SUM(amount) FROM payment GROUP BY customer_id ORDER BY customer_id', conn)

df8 = pd.read_sql_query('SELECT customer_id, staff_id, SUM(amount) FROM payment GROUP BY staff_id, customer_id \
    ORDER BY customer_id, staff_id', conn)

df9 = pd.read_sql_query('SELECT DATE(payment_date), SUM(amount) FROM payment GROUP BY DATE(payment_date) \
    ORDER BY SUM(amount) DESC', conn)


# challenge 1
c1_1 = pd.read_sql_query(
    "SELECT staff_id, COUNT(amount) FROM payment \
    GROUP BY staff_id \
    ORDER BY COUNT(amount) \
    DESC", conn)

c1_2 = pd.read_sql_query(
    "SELECT rating, AVG(replacement_cost) FROM film \
    GROUP BY rating", conn)

c1_3 = pd.read_sql_query(
    "SELECT customer_id, SUM(amount) FROM payment \
    GROUP BY customer_id \
    ORDER BY SUM(amount) DESC LIMIT 5", conn)


df10 = pd.read_sql_query(
    "SELECT customer_id, SUM(amount) FROM payment \
    WHERE customer_id NOT IN (184,87,477) \
    GROUP BY customer_id \
    HAVING SUM(amount) > 100", conn
)

# challenge 2
c2_1 = pd.read_sql_query(
    "SELECT customer_id, COUNT(amount) FROM payment \
    GROUP BY customer_id \
    HAVING COUNT(amount) >= 40", conn
)

c2_2 = pd.read_sql_query(
    "SELECT customer_id, SUM(amount) FROM payment \
    WHERE staff_id=2 \
    GROUP BY customer_id \
    HAVING SUM(amount) > 100", conn
)
