"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel

# Importa las librerías necesarias
import pandas as pd
import zipfile
import io
import os


def clean_campaign_data():
    """
    Limpia datos de una campaña de marketing de un banco.
    Lee directamente varios CSV comprimidos en ZIP dentro de 'files/input/',
    consolida toda la información en un único DataFrame y, a partir de ahí,
    genera tres archivos CSV sin comprimir en 'files/output/':
      - client.csv
      - campaign.csv
      - economics.csv

    Reglas de limpieza aplicadas:
      client.csv
        - job: reemplazar "." por "" y "-" por "_"
        - education: reemplazar "." por "_" y "unknown" por pd.NA
        - credit_default: "yes" -> 1; cualquier otro valor -> 0
        - mortgage: "yes" -> 1; cualquier otro valor -> 0

      campaign.csv
        - previous_outcome: "success" -> 1; cualquier otro valor -> 0
        - campaign_outcome: "yes" -> 1; cualquier otro valor -> 0
        - last_contact_date: combinar 'day' y 'month' con año 2022 en formato YYYY-MM-DD
       
        economics.csv:
        - client_id
        - const_price_idx
        - eurobor_three_months
     
     """

    # Crea la carpeta de salida si no existe
    os.makedirs("files/output", exist_ok=True)
    
    # Acumula aquí los DataFrames leídos de cada archivo comprimido
    all_dfs = []
    
    # Lee y procesa todos los CSV de forma directa desde cada ZIP
    for i in range(10):
        with zipfile.ZipFile(f'files/input/bank-marketing-campaing-{i}.csv.zip', 'r') as z:
            # Toma el primer archivo CSV dentro del ZIP
            with z.open(z.namelist()[0]) as f:
                # Lee el CSV a un DataFrame (decodifica el binario)
                df = pd.read_csv(io.TextIOWrapper(f, encoding='utf-8'))
                all_dfs.append(df)
    
    # Une todos los DataFrames en uno solo (filas apiladas)
    full_df = pd.concat(all_dfs, ignore_index=True)
    
    # ---------- client.csv ----------
    # Selecciona y copia las columnas para la tabla de clientes
    client_df = full_df[['client_id', 'age', 'job', 'marital', 'education', 'credit_default', 'mortgage']].copy()
    
    # job: reemplaza "." por nada y "-" por "_"
    client_df['job'] = client_df['job'].str.replace('.', '', regex=False)
    client_df['job'] = client_df['job'].str.replace('-', '_', regex=False)
    
    # education: "." -> "_" y "unknown" -> pd.NA (valor faltante)
    client_df['education'] = client_df['education'].str.replace('.', '_', regex=False)
    client_df['education'] = client_df['education'].replace('unknown', pd.NA)
    
    # credit_default: "yes" -> 1; otro -> 0
    client_df['credit_default'] = (client_df['credit_default'] == 'yes').astype(int)
    
    # mortgage: "yes" -> 1; otro -> 0
    client_df['mortgage'] = (client_df['mortgage'] == 'yes').astype(int)
    
    # Guarda client.csv
    client_df.to_csv("files/output/client.csv", index=False)
    
    # ---------- campaign.csv ----------
    # Selecciona columnas de campaña (incluye month/day para crear la fecha final)
    campaign_df = full_df[['client_id', 'number_contacts', 'contact_duration', 'previous_campaign_contacts', 
                           'previous_outcome', 'campaign_outcome', 'month', 'day']].copy()
    
    # previous_outcome: "success" -> 1; otro -> 0
    campaign_df['previous_outcome'] = (campaign_df['previous_outcome'] == 'success').astype(int)
    
    # campaign_outcome: "yes" -> 1; otro -> 0
    campaign_df['campaign_outcome'] = (campaign_df['campaign_outcome'] == 'yes').astype(int)
    
    # Crea last_contact_date en formato YYYY-MM-DD a partir de month (texto) y day (num)
    month_mapping = {
        'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
        'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12
    }
    campaign_df['month_num'] = campaign_df['month'].map(month_mapping)
    campaign_df['last_contact_date'] = pd.to_datetime({
        'year': 2022,
        'month': campaign_df['month_num'],
        'day': campaign_df['day']
    }).dt.strftime('%Y-%m-%d')
    
    # Deja solo las columnas finales requeridas para campaign.csv
    campaign_final = campaign_df[['client_id', 'number_contacts', 'contact_duration', 
                                  'previous_campaign_contacts', 'previous_outcome', 
                                  'campaign_outcome', 'last_contact_date']]
    
    # Guarda campaign.csv
    campaign_final.to_csv("files/output/campaign.csv", index=False)
    
    # ---------- economics.csv ----------
    # Selecciona columnas económicas tal como están en el dataset
    economics_df = full_df[['client_id', 'cons_price_idx', 'euribor_three_months']].copy()
    
    # Guarda economics.csv
    economics_df.to_csv("files/output/economics.csv", index=False)
    
    return

if __name__ == "__main__":
    clean_campaign_data()