import sqlite3
import pandas as pd
import os

def load_data_from_db(db_path, table_name):
    """Carga los datos de una tabla de SQLite en un DataFrame de Pandas."""
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
    conn.close()
    return df

def analyze_data(df, table_name):
    """Realiza análisis exploratorio de la tabla."""
    report = []
    report.append(f"Tabla: {table_name}")
    report.append(f"Total registros: {len(df)}")
    report.append(f"Duplicados antes de limpieza: {df.drop(columns=['id'], errors='ignore').duplicated().sum()}")
    report.append(f"Valores nulos por columna:\n{df.isnull().sum()}")
    report.append(f"Tipos de datos antes de limpieza:\n{df.dtypes}\n")
    return "\n".join(report)

def clean_data(df, table_name):
    """Realiza limpieza de datos en un DataFrame."""
    duplicates_before = df.drop(columns=['id'], errors='ignore').duplicated().sum()
    df = df.drop_duplicates(subset=df.columns.difference(['id'])).reset_index(drop=True)
    duplicates_after = df.drop(columns=['id'], errors='ignore').duplicated().sum()
    removed_duplicates = duplicates_before - duplicates_after
    
    empty_values_count = (df == "").sum().sum()
    null_values_count = df.isnull().sum().sum()
    
    df = df.replace("", "unknown")
    df = df.fillna("unknown")
    
    schema = {
        "characters": {
            "id": int, "name": str, "status": str, "species": str, "type": str,
            "gender": str, "origin": str, "location": str, "image": str, "url": str,
            "created": str
        },
        "locations": {
            "id": int, "name": str, "type": str, "dimension": str, "residents": str,
            "url": str, "created": str
        },
        "episodes": {
            "id": int, "name": str, "air_date": str, "episode": str, "characters": str,
            "url": str, "created": str
        }
    }
    
    corrections = []
    for column, expected_type in schema.get(table_name, {}).items():
        if column in df.columns:
            original_dtype = df[column].dtype
            if expected_type == int:
                df[column] = pd.to_numeric(df[column], errors='coerce').fillna(0).astype(int)
            elif expected_type == str:
                df[column] = df[column].astype(str)
            new_dtype = df[column].dtype
            if original_dtype != new_dtype:
                corrections.append(f"Columna: {column}, Tipo antes: {original_dtype}, Tipo después: {new_dtype}")
    
    return df, corrections, empty_values_count, null_values_count, removed_duplicates

def save_cleaned_data(df, table_name, output_dir, output_path, first_table):
    """Guarda el DataFrame limpio en un archivo Excel sin sobrescribir las tablas existentes."""
    os.makedirs(output_dir, exist_ok=True)
    
    mode = 'w' if first_table else 'a'
    with pd.ExcelWriter(output_path, engine='openpyxl', mode=mode, if_sheet_exists='replace' if not first_table else None) as writer:
        df.to_excel(writer, sheet_name=table_name, index=False)

def generate_cleaning_report(report_data, output_path):
    """Guarda el reporte de limpieza en un archivo .txt."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w") as f:
        f.write(report_data)

def main():
    db_path = "src/db/ingestion.db"
    report_path = "src/static/auditoria/cleaning_report.txt"
    output_dir = "src/xlsx"
    output_path = os.path.join(output_dir, "cleaned_data.xlsx")
    
    tables = ["characters", "locations", "episodes"]
    full_report = []
    
    if os.path.exists(output_path):
        os.remove(output_path)
    
    first_table = True
    for table in tables:
        df = load_data_from_db(db_path, table)
        report = analyze_data(df, table)
        full_report.append(report)
        df_cleaned, corrections, empty_values_count, null_values_count, removed_duplicates = clean_data(df, table)
        save_cleaned_data(df_cleaned, table, output_dir, output_path, first_table)
        first_table = False
        full_report.append(f"Registros después de limpieza: {len(df_cleaned)}")
        full_report.append(f"Duplicados eliminados en {table}: {removed_duplicates}")
        
        if corrections:
            full_report.append("Correcciones de tipos de datos:")
            full_report.extend(corrections)
            full_report.append("")
       
        full_report.append(f"Tipos de datos después de limpieza:\n{df_cleaned.dtypes}")
        full_report.append(f"Total de columnas modificadas en {table}: {len(corrections)}")
        full_report.append(f"Valores vacíos detectados y reemplazados en {table}: {empty_values_count}")
        full_report.append(f"Valores nulos detectados y reemplazados en {table}: {null_values_count}\n")
        
    generate_cleaning_report("\n".join(full_report), report_path)
    print(f"[AUDITORÍA] Reporte de limpieza generado en: {report_path}")
    print("Limpieza de datos completada. Archivos generados correctamente.")

if __name__ == "__main__":
    main()


