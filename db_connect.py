# Importation des packages
import psycopg2
import mysql.connector

# source database
def get_conn_mysql():
    conn = mysql.connector.connect(host="localhost", port=3306, user="root", password="", db="northwind")
    # start a connection
    cur = conn.cursor()
    return cur, conn
# target database
def get_conn_postgresql():
    conn = psycopg2.connect(host="localhost",database="dwh_northwind",user="postgres",password="ALOU2101ab", port="5431")
    # start a connection
    cur = conn.cursor()
    return cur, conn
