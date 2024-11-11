import db_connect
import pandas as pd
import psycopg2
import psycopg2.extras as extras
import warnings
import pandas as pd
import holidays

# Ignorer les avertissements utilisateur
warnings.simplefilter("ignore", UserWarning)

# Connexion aux bases de données MySQL (source) et PostgreSQL (cible)
cur1, conn1 = db_connect.get_conn_mysql()
cur2, conn2 = db_connect.get_conn_postgresql()

# Création et mise à jour des tables dans la base de données cible PostgreSQL
commands = (
    """
    CREATE TABLE IF NOT EXISTS dim_product (
        dim_product_id SERIAL PRIMARY KEY,
        category_id INT,
        category_name VARCHAR(15),
        category_description TEXT,
        product_id INT,
        product_name VARCHAR(40),
        product_supplier_id INT,
        product_category_id INT,
        product_quantity_per_unit VARCHAR(20),
        product_unit_price DECIMAL(10, 2),
        supplier_id INT,
        supplier_company_name VARCHAR(40),
        supplier_contact_name VARCHAR(30),
        supplier_contact_title VARCHAR(30),
        supplier_address VARCHAR(60),
        supplier_city VARCHAR(15),
        supplier_region VARCHAR(15),
        supplier_postal_code VARCHAR(10),
        supplier_country VARCHAR(15)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS dim_customer (
        dim_customer_id SERIAL PRIMARY KEY,
        customer_id INT,
        customer_company_name VARCHAR(40),
        customer_contact_name VARCHAR(30),
        customer_contact_title VARCHAR(30),
        customer_address VARCHAR(60),
        customer_city VARCHAR(15),
        customer_region VARCHAR(15),
        customer_postal_code VARCHAR(10),
        customer_country VARCHAR(15)
    );

    """,
    """
    CREATE TABLE IF NOT EXISTS dim_order (
        dim_order_id SERIAL PRIMARY KEY,
        salesorder_order_id INT,
        salesorder_cust_id INT,
        salesorder_employee_id INT,
        salesorder_order_date TIMESTAMP,
        salesorder_required_date TIMESTAMP,
        salesorder_shipped_date TIMESTAMP,
        salesorder_shipper_id INT,
        salesorder_freight DECIMAL(10, 2),
        salesorder_ship_name VARCHAR(40),
        salesorder_ship_address VARCHAR(60),
        salesorder_ship_city VARCHAR(15),
        salesorder_ship_region VARCHAR(15),
        salesorder_ship_postal_code VARCHAR(10),
        salesorder_ship_country VARCHAR(15),
        orderdetail_id INT,
        orderdetail_order_id INT,
        orderdetail_product_id INT,
        orderdetail_unit_price DECIMAL(10, 2),
        orderdetail_quantity SMALLINT,
        orderdetail_discount DECIMAL(10, 2),
        shipper_id INT,
        shipper_company_name VARCHAR(40)
    );

    """,
    """
    CREATE TABLE IF NOT EXISTS dim_date (
        dim_date_id SERIAL PRIMARY KEY,
        date_dim_date_t DATE,
        date_dim_jour INT,
        date_dim_jour_in_mois INT,
        date_dim_jour_in_week INT,
        date_dim_mois_annee VARCHAR(20),
        date_dim_annee INT,
        date_dim_fete VARCHAR(80),
        date_dim_mois VARCHAR(20)
    );

    """,
    """
    CREATE TABLE IF NOT EXISTS fact_sales (
        id_fact_sales SERIAL PRIMARY KEY,
        nbre_vente INT,
        mt_vente DECIMAL(10, 2),
        nbre_client INT,
        dim_product_id INT,
        dim_order_id INT,
        dim_customer_id INT,
        dim_date_id INT,
        FOREIGN KEY (dim_product_id) REFERENCES dim_product(dim_product_id),
        FOREIGN KEY (dim_order_id) REFERENCES dim_order(dim_order_id),
        FOREIGN KEY (dim_customer_id) REFERENCES dim_customer(dim_customer_id),
        FOREIGN KEY (dim_date_id) REFERENCES dim_date(dim_date_id)
    );
    """,
    """
    TRUNCATE TABLE dim_product, dim_customer, dim_order, dim_date, fact_sales;
    """
)

# Exécution des commandes pour créer/mise à jour des tables dans PostgreSQL
for command in commands:
    cur2.execute(command)
print("--------- Tables mises à jour ----------")

# Commit des changements de schéma
conn2.commit()

# Extraction des données depuis MySQL

# Produits
query1 = """
    SELECT 
        c.categoryId AS category_id, 
        c.categoryName AS category_name, 
        c.description AS category_description, 
        p.productId AS product_id, 
        p.productName AS product_name, 
        p.supplierId AS product_supplier_id, 
        p.categoryId AS product_category_id, 
        p.quantityPerUnit AS product_quantity_per_unit, 
        p.unitPrice AS product_unit_price, 
        s.supplierId AS supplier_id, 
        s.companyName AS supplier_company_name, 
        s.contactName AS supplier_contact_name, 
        s.contactTitle AS supplier_contact_title, 
        s.address AS supplier_address, 
        s.city AS supplier_city, 
        s.region AS supplier_region, 
        s.postalCode AS supplier_postal_code, 
        s.country AS supplier_country
    FROM 
        Product p
    JOIN 
        Supplier s ON p.supplierId = s.supplierId
    JOIN 
        Category c ON p.categoryId = c.categoryId;
"""

# Clients
query2 = """
        SELECT 
        custId AS customer_id, 
        companyName AS customer_company_name, 
        contactName AS customer_contact_name, 
        contactTitle AS customer_contact_title,
        address AS customer_address, 
        city AS customer_city, 
        region AS customer_region, 
        postalCode AS customer_postal_code, 
        country AS customer_country
    FROM 
        Customer;
"""

# Commandes
query3 = """
    SELECT 
        o.orderId AS salesorder_order_id, 
        o.custId AS salesorder_cust_id, 
        o.employeeId AS salesorder_employee_id, 
        o.orderDate AS salesorder_order_date, 
        o.requiredDate AS salesorder_required_date, 
        o.shippedDate AS salesorder_shipped_date, 
        o.shipperId AS salesorder_shipper_id, 
        o.freight AS salesorder_freight, 
        o.shipName AS salesorder_ship_name, 
        o.shipAddress AS salesorder_ship_address, 
        o.shipCity AS salesorder_ship_city, 
        o.shipRegion AS salesorder_ship_region, 
        o.shipPostalCode AS salesorder_ship_postal_code, 
        o.shipCountry AS salesorder_ship_country, 
        od.orderDetailId AS orderdetail_id, 
        od.productId AS orderdetail_product_id, 
        od.unitPrice AS orderdetail_unit_price, 
        od.quantity AS orderdetail_quantity, 
        od.discount AS orderdetail_discount, 
        sh.companyName AS shipper_company_name
    FROM 
        SalesOrder o
    JOIN 
        OrderDetail od ON o.orderId = od.orderId
    JOIN 
        Shipper sh ON o.shipperId = sh.shipperId;
"""
# Fait vente
query4 = """
   SELECT 
        COUNT(o.orderdetail_product_id) AS nbre_vente,
        SUM(o.orderdetail_unit_price * o.orderdetail_quantity) AS mt_vente,
        COUNT(DISTINCT o.salesorder_cust_id) AS nbre_client,
        p.dim_product_id AS dim_product_id,
        o.dim_order_id AS dim_order_id,
        c.dim_customer_id AS dim_customer_id,
        t.dim_date_id AS dim_date_id
    FROM 
        dim_order o
    JOIN 
        dim_product p ON o.orderdetail_product_id = p.product_id
    JOIN 
        dim_customer c ON o.salesorder_cust_id = c.customer_id
    JOIN 
        dim_date t ON t.date_dim_date_t = o.salesorder_order_date
    GROUP BY 
        p.dim_product_id, o.dim_order_id, c.dim_customer_id, t.dim_date_id;
"""

# Génération de la dimension temps avec une plage de dates
date_range = pd.date_range(start="2006-01-01", end="2006-12-31", freq="D")
df_temps = pd.DataFrame({
    'dim_date_id': (date_range - pd.Timestamp("1970-01-01")) // pd.Timedelta('1D'),  # ID unique pour chaque date
    'date_dim_date_t': date_range,
    'date_dim_jour': date_range.day,
    'date_dim_jour_in_mois': date_range.day,
    'date_dim_jour_in_week': date_range.weekday + 1,  # 1 pour Lundi, 7 pour Dimanche
    'date_dim_mois_annee': date_range.strftime('%B %Y'),  # Nom du mois et année
    'date_dim_annee': date_range.year,
    'date_dim_mois': date_range.strftime('%B')  # Nom du mois
})

# Obtenir les jours fériés pour la plage d'années spécifiée (par exemple, pour la France)
holidays_us = holidays.CountryHoliday('US', years=range(2006, 2008))

# Ajouter les colonnes 'fete'
df_temps['date_dim_fete'] = df_temps['dim_date_id'].apply(
    lambda x: holidays_us.get(pd.Timestamp("2006-01-01") + pd.Timedelta(days=x)) if pd.Timestamp("2006-01-01") + pd.Timedelta(days=x) in holidays_us else None
)

# Exécuter les requêtes et charger les données dans des DataFrames
df_product = pd.read_sql(query1, con=conn1)
df_customer = pd.read_sql(query2, con=conn1)
df_order = pd.read_sql(query3, con=conn1)

# Convertir les colonnes en datetime, puis remplacer NaT par None et extraire uniquement la date
df_order['salesorder_order_date'] = pd.to_datetime(df_order['salesorder_order_date'], errors='coerce').dt.date.replace({pd.NaT: None})
df_order['salesorder_required_date'] = pd.to_datetime(df_order['salesorder_required_date'], errors='coerce').dt.date.replace({pd.NaT: None})
df_order['salesorder_shipped_date'] = pd.to_datetime(df_order['salesorder_shipped_date'], errors='coerce').dt.date.replace({pd.NaT: None})


# Fonction pour insérer des données dans PostgreSQL
def execute_values(conn, cur, df, table):
    tuples = [tuple(x) for x in df.to_numpy()]
    cols = ','.join(list(df.columns))
    query = f"INSERT INTO {table} ({cols}) VALUES %s"
    try:
        extras.execute_values(cur, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error:", error)
        conn.rollback()
        return 1
    print("------- Données insérées/mises à jour dans la table ----", table)

# Chargement des DataFrames dans les tables de la base de données PostgreSQL
execute_values(conn2, cur2, df_product, 'dim_product')
execute_values(conn2, cur2, df_customer, 'dim_customer')
execute_values(conn2, cur2, df_order, 'dim_order')
execute_values(conn2, cur2, df_temps, 'dim_date')

##------------------FACT_SALES---------------------
# Chargement de la table de fait
df_fact_sales = pd.read_sql(query4,con=conn2)

# Chargement des données dans la table fact_sales
#execute_values(conn2, cur2, df_fact_sales, 'fact_sales')
if df_fact_sales.empty:
    print("df_fact_sales est vide. Vérifiez la requête et les données sources.")
else:
    execute_values(conn2, cur2, df_fact_sales, 'fact_sales')

# Fermeture des connexions
conn1.close()
conn2.close()
