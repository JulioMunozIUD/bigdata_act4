import requests
import json
import sqlite3
import pandas as pd
import os



def obtener_datos_api(url="",params={}):
    url = "{}/{}/".format(url,params["characters"]) 
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as error:
        print(error)
        return {}
    

parametros = {"characters":"character"}#,"locations":"ticker","episodes":"ticker"}
url = "https://rickandmortyapi.com/api"
data = obtener_datos_api(url=url, params=parametros)

def create_database():
    db_path = "src/db/ingestion.db"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS characters (
                        id INTEGER PRIMARY KEY,
                        name TEXT,
                        status TEXT,
                        species TEXT,
                        type TEXT,
                        gender TEXT,
                        origin TEXT,
                        location TEXT,
                        image TEXT,
                        url TEXT,
                        created TEXT)''')
    conn.commit()
    conn.close()

def insert_data_into_db(data):
    db_path = "src/db/ingestion.db"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    characters = [(char['id'], char['name'], char['status'], char['species'], char['type'], char['gender'],
                   char['origin']['name'], char['location']['name'], char['image'], char['url'], char['created'])
                  for char in data['results']]
    cursor.executemany("""INSERT OR IGNORE INTO characters VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", characters)
    conn.commit()
    conn.close()

def generate_sample_file():
    db_path = "src/db/ingestion.db"
    output_path = "src/xlsx/ingestion.xlsx"
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query("SELECT * FROM characters LIMIT 10", conn)
    df.to_excel(output_path, index=False, engine='openpyxl')  # Specify engine
    conn.close()

def generate_audit_file(data):
    db_path = "src/db/ingestion.db"
    audit_path = "src/static/auditoria/ingestion.txt"
    conn = sqlite3.connect(db_path)
    db_count = pd.read_sql_query("SELECT COUNT(*) AS count FROM characters", conn)['count'][0]
    api_count = len(data['results'])
    conn.close()
    with open(audit_path, "w") as file:
        file.write(f"Registros en la API: {api_count}\n")
        file.write(f"Registros en la BD: {db_count}\n")
        file.write(f"Diferencia: {api_count - db_count}\n")

def main():
    os.makedirs("src/db", exist_ok=True)
    os.makedirs("src/xlsx", exist_ok=True)
    os.makedirs("src/static/auditoria", exist_ok=True)

    parametros = {"characters": "character"}
    url = "https://rickandmortyapi.com/api"
    data = obtener_datos_api(url=url, params=parametros)

    create_database()
    insert_data_into_db(data)
    generate_sample_file()
    generate_audit_file(data)

if __name__ == "__main__":
    main()


