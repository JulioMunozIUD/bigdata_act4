import requests
import json
import sqlite3
import pandas as pd
import os

def obtener_datos_api(endpoint):
    url = f"https://rickandmortyapi.com/api/{endpoint}"
    all_results = []
    try:
        while url:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            results = data.get('results', [])
            all_results.extend(results)
            # Actualiza la url con la siguiente página (o None si ya no hay)
            url = data.get('info', {}).get('next')
        return {'results': all_results, 'info': {'count': len(all_results)}}
    except requests.exceptions.RequestException as error:
        print(f"Error obteniendo {endpoint}: {error}")
        return {'results': [], 'info': {'count': 0}}

def create_database():
    db_path = "src/static/db/ingestion.db"
    # Crear directorios si no existen
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
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
    
    cursor.execute('''CREATE TABLE IF NOT EXISTS locations (
                        id INTEGER PRIMARY KEY,
                        name TEXT,
                        type TEXT,
                        dimension TEXT,
                        residents TEXT,
                        url TEXT,
                        created TEXT)''')
    
    cursor.execute('''CREATE TABLE IF NOT EXISTS episodes (
                        id INTEGER PRIMARY KEY,
                        name TEXT,
                        air_date TEXT,
                        episode TEXT,
                        characters TEXT,
                        url TEXT,
                        created TEXT)''')
    
    conn.commit()
    conn.close()

def insert_data_into_db(data, table):
    db_path = "src/static/db/ingestion.db"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    if table == "characters":
        records = [(char['id'], char['name'], char['status'], char['species'], char['type'], char['gender'],
                    char['origin']['name'], char['location']['name'], char['image'], char['url'], char['created'])
                   for char in data.get('results', [])]
        query = """INSERT OR IGNORE INTO characters VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
    
    elif table == "locations":
        records = [(loc['id'], loc['name'], loc['type'], loc['dimension'], json.dumps(loc['residents']), loc['url'], loc['created'])
                   for loc in data.get('results', [])]
        query = """INSERT OR IGNORE INTO locations VALUES (?, ?, ?, ?, ?, ?, ?)"""
    
    elif table == "episodes":
        records = [(ep['id'], ep['name'], ep['air_date'], ep['episode'], json.dumps(ep['characters']), ep['url'], ep['created'])
                   for ep in data.get('results', [])]
        query = """INSERT OR IGNORE INTO episodes VALUES (?, ?, ?, ?, ?, ?, ?)"""
    
    else:
        conn.close()
        return
    
    cursor.executemany(query, records)
    conn.commit()
    conn.close()

def generate_audit_file(api_data):
    db_path = "src/static/db/ingestion.db"
    audit_path = "src/static/auditoria/ingestion.txt"
    
    # Crear directorios si no existen
    os.makedirs(os.path.dirname(audit_path), exist_ok=True)
    
    conn = sqlite3.connect(db_path)
    
    with open(audit_path, "w") as file:
        for table, api_count in api_data.items():
            db_count = pd.read_sql_query(f"SELECT COUNT(*) AS count FROM {table}", conn)['count'][0]
            difference = api_count - db_count
            file.write(f"Tabla: {table}\n")
            file.write(f"Registros en API: {api_count}\n")
            file.write(f"Registros en BD: {db_count}\n")
            file.write(f"Diferencia: {difference}\n\n")
            print(f"[AUDITORÍA] {table}: API={api_count}, BD={db_count}, Diferencia={difference}")
    
    conn.close()

def export_db_to_excel():
    db_path = "src/static/db/ingestion.db"
    output_path = "src/xlsx/ingestion.xlsx"
    
    # Crear directorios si no existen
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    conn = sqlite3.connect(db_path)
    tables = ["characters", "locations", "episodes"]
    dfs = {table: pd.read_sql_query(f"SELECT * FROM {table}", conn) for table in tables}
    
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        for table, df in dfs.items():
            df.to_excel(writer, sheet_name=table, index=False)
    
    conn.close()
    print("Proceso de ingesta de datos completado.")

def main():
    # Obtener datos de la API
    characters_data = obtener_datos_api("character")
    locations_data = obtener_datos_api("location")
    episodes_data = obtener_datos_api("episode")
    
    # Crear base de datos y tablas
    create_database()
    
    # Insertar datos en la base de datos
    insert_data_into_db(characters_data, "characters")
    insert_data_into_db(locations_data, "locations")
    insert_data_into_db(episodes_data, "episodes")
    
    # Generar archivo de auditoría
    api_data = {
        "characters": characters_data['info']['count'],
        "locations": locations_data['info']['count'],
        "episodes": episodes_data['info']['count']
    }
    generate_audit_file(api_data)
    
    # Exportar a Excel
    export_db_to_excel()

if __name__ == "__main__":
    main()



