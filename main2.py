import csv
import sqlite3
import pandas as pd

# Connect to an SQLite database
conn = sqlite3.connect('my_database.db')
cursor = conn.cursor()

# Create the "tours" table
cursor.execute('''
    CREATE TABLE tours (
        POIID INTEGER PRIMARY KEY AUTOINCREMENT,
        Nom_du_POI VARCHAR(100),
        Categories_de_POI VARCHAR(500),
        Latitude REAL,
        Longitude REAL,
        Adresse_postale VARCHAR(200),
        Code_postal_et_commune VARCHAR(100),
        Covid19_mesures_specifiques VARCHAR(500),
        Createur_de_la_donnee VARCHAR(100),
        SIT_diffuseur VARCHAR(100),
        Date_de_mise_a_jour DATE,
        Contacts_du_POI VARCHAR(200),
        Classements_du_POI VARCHAR(200),
        Description TEXT,
        URI_ID_du_POI VARCHAR(200)
    )
''')

# Create the "products" table (same schema as "tours")
cursor.execute('''
    CREATE TABLE products (
        POIID INTEGER PRIMARY KEY AUTOINCREMENT,
        Nom_du_POI VARCHAR(100),
        Categories_de_POI VARCHAR(500),
        Latitude REAL,
        Longitude REAL,
        Adresse_postale VARCHAR(200),
        Code_postal_et_commune VARCHAR(100),
        Covid19_mesures_specifiques VARCHAR(500),
        Createur_de_la_donnee VARCHAR(100),
        SIT_diffuseur VARCHAR(100),
        Date_de_mise_a_jour DATE,
        Contacts_du_POI VARCHAR(200),
        Classements_du_POI VARCHAR(200),
        Description TEXT,
        URI_ID_du_POI VARCHAR(200)
    )
''')

# Create the "places" table (same schema as "tours" and "products")
cursor.execute('''
    CREATE TABLE places (
        POIID INTEGER PRIMARY KEY AUTOINCREMENT,
        Nom_du_POI VARCHAR(100),
        Categories_de_POI VARCHAR(500),
        Latitude REAL,
        Longitude REAL,
        Adresse_postale VARCHAR(200),
        Code_postal_et_commune VARCHAR(100),
        Covid19_mesures_specifiques VARCHAR(500),
        Createur_de_la_donnee VARCHAR(100),
        SIT_diffuseur VARCHAR(100),
        Date_de_mise_a_jour DATE,
        Contacts_du_POI VARCHAR(200),
        Classements_du_POI VARCHAR(200),
        Description TEXT,
        URI_ID_du_POI VARCHAR(200)
    )
''')

# Read data from the provided CSV files and insert into the respective tables
def insert_data(csv_file_path, table_name):
    with open(csv_file_path, 'r') as csv_file:
        csv_reader = csv.DictReader(csv_file, delimiter=',')  # Adjust delimiter
        for row in csv_reader:
            cursor.execute(f'''
                INSERT INTO {table_name} (Nom_du_POI, Categories_de_POI, Latitude, Longitude,
                                          Adresse_postale, Code_postal_et_commune, Covid19_mesures_specifiques,
                                          Createur_de_la_donnee, SIT_diffuseur, Date_de_mise_a_jour,
                                          Contacts_du_POI, Classements_du_POI, Description, URI_ID_du_POI)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (row['Nom_du_POI'], row['Categories_de_POI'], row['Latitude'], row['Longitude'],
                  row['Adresse_postale'], row['Code_postal_et_commune'], row['Covid19_mesures_specifiques'],
                  row['Createur_de_la_donnee'], row['SIT_diffuseur'], row['Date_de_mise_a_jour'],
                  row['Contacts_du_POI'], row['Classements_du_POI'], row['Description'], row['URI_ID_du_POI']))

# Insert data into the "tours" table
insert_data('/Users/macdanesh/PycharmProjects/HolidayIterny/venv/DataCollection/datatourisme-tour-20240214.csv', 'tours')

# Insert data into the "products" table
insert_data('/Users/macdanesh/PycharmProjects/HolidayIterny/venv/DataCollection/datatourisme-product-20240214.csv', 'products')

# Insert data into the "places" table
insert_data('/Users/macdanesh/PycharmProjects/HolidayIterny/venv/DataCollection/datatourisme-place-20240214.csv', 'places')

# Commit changes
conn.commit()

# Query and print the first 20 rows from each table
for table_name in ['tours', 'products', 'places']:
    cursor.execute(f'SELECT * FROM {table_name} LIMIT 20')
    rows = cursor.fetchall()
    print(f"Table: {table_name}")
    for row in rows:
        print(row)

# Close the connection
conn.close()