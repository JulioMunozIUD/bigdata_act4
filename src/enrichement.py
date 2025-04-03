import pandas as pd
import os

# Definir rutas de archivos
BASE_PATH = os.path.join("src", "db")
STATIC_PATH = os.path.join("src", "static/auditoria")
OUTPUT_PATH = os.path.join("src", "xlsx")

CLEANED_FILE = os.path.join(BASE_PATH, "cleaned_data.xlsx")
SCRIPTS_FILE = os.path.join(BASE_PATH, "RickAndMortyScripts.xlsx")
ENRICHED_FILE = os.path.join(OUTPUT_PATH, "enriched_data.xlsx")
REPORT_FILE = os.path.join(STATIC_PATH, "enriched_report.txt")

# Asegurar que las carpetas existen
os.makedirs(STATIC_PATH, exist_ok=True)
os.makedirs(OUTPUT_PATH, exist_ok=True)

# 1. Cargar el dataset limpio
def load_cleaned_dataset():
    sheets_cleaned = pd.read_excel(CLEANED_FILE, sheet_name=None)
    for sheet_name, df in sheets_cleaned.items():
        df.columns = df.columns.str.lower().str.strip()
        sheets_cleaned[sheet_name] = df
    return sheets_cleaned

# 2. Cargar el dataset de scripts
def load_scripts_dataset():
    df_scripts = pd.read_excel(SCRIPTS_FILE)
    df_scripts.columns = df_scripts.columns.str.lower().str.strip()
    return df_scripts

# 3. Integrar datasets
def integrate_datasets(cleaned_sheets, scripts_df):
    enriched_sheets = {sheet: df.copy() for sheet, df in cleaned_sheets.items()}
    if "episodes" in cleaned_sheets:
        df_episodes = cleaned_sheets["episodes"].copy()
        df_episodes['name'] = df_episodes['name'].str.lower().str.strip()
        scripts_df['name'] = scripts_df['name'].str.lower().str.strip()

        df_episodes_enriched = df_episodes.merge(scripts_df, on="name", how="left", suffixes=("", "_script"))
        enriched_sheets["episodes"] = df_episodes_enriched
    return enriched_sheets

# 4. Generar reporte de auditoría
def generate_report(enriched_sheets, cleaned_sheets, scripts_df):
    with pd.ExcelWriter(ENRICHED_FILE) as writer:
        for sheet_name, df in enriched_sheets.items():
            df.to_excel(writer, sheet_name=sheet_name, index=False)

    with open(REPORT_FILE, "w", encoding="utf-8") as report:
        report.write("Reporte de Enriquecimiento de Datos\n")
        report.write("==================================\n\n")

        total_cleaned_cells = sum(df.shape[0] * df.shape[1] for df in cleaned_sheets.values())
        total_enriched_cells = sum(df.shape[0] * df.shape[1] for df in enriched_sheets.values())

        # Número de filas y columnas por hoja
        for sheet_name, df in cleaned_sheets.items():
            report.write(f"Hoja: {sheet_name}\n")
            report.write(f"  Dataset limpio - Filas: {df.shape[0]}, Columnas: {df.shape[1]}, Celdas: {df.shape[0] * df.shape[1]}\n")
        report.write(f"Registros en dataset de scripts: {len(scripts_df)}\n\n")

        for sheet_name, df in enriched_sheets.items():
            report.write(f"Hoja: {sheet_name}\n")
            report.write(f"  Dataset enriquecido - Filas: {df.shape[0]}, Columnas: {df.shape[1]}, Celdas: {df.shape[0] * df.shape[1]}\n")
        report.write("\n")

        # Auditoría específica para 'episodes'
        if "episodes" in cleaned_sheets:
            coincidencias = cleaned_sheets['episodes']['name'].str.lower().str.strip().isin(
                scripts_df['name'].str.lower().str.strip()
            )
            report.write(f"Registros coincidentes en 'episodes': {coincidencias.sum()}\n")
            report.write(f"Registros no encontrados en la fuente de scripts: {len(cleaned_sheets['episodes']) - coincidencias.sum()}\n\n")

        # Comparar columnas agregadas
        report.write("Columnas adicionales agregadas por hoja:\n")
        for sheet_name in enriched_sheets.keys():
            cleaned_cols = set(cleaned_sheets.get(sheet_name, pd.DataFrame()).columns)
            enriched_cols = set(enriched_sheets[sheet_name].columns)
            added_cols = enriched_cols - cleaned_cols
            if added_cols:
                report.write(f"- {sheet_name}: {', '.join(sorted(added_cols))}\n")
            else:
                report.write(f"- {sheet_name}: No se agregaron columnas\n")

        # Resumen General
        report.write("\nResumen General:\n")
        report.write(f"Total de celdas en dataset limpio: {total_cleaned_cells}\n")
        report.write(f"Total de celdas en dataset enriquecido: {total_enriched_cells}\n")
        report.write(f"Diferencia en celdas: {total_enriched_cells - total_cleaned_cells}\n")

if __name__ == "__main__":
    cleaned_sheets = load_cleaned_dataset()
    scripts_df = load_scripts_dataset()
    
    cleaned_sheets_original = {sheet: df.copy() for sheet, df in cleaned_sheets.items()}
    
    enriched_sheets = integrate_datasets(cleaned_sheets, scripts_df)
    
    generate_report(enriched_sheets, cleaned_sheets_original, scripts_df)
    print("Proceso de enriquecimiento completado.")













