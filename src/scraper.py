import requests
import pandas as pd
import os


def download_omie_price(date: str) -> pd.DataFrame:
    
    url = f"https://www.omie.es/es/file-download?parents%5B0%5D=marginalpdbc&filename=marginalpdbc_{date}.1"
    
    response = requests.get(url)
    
    if response.status_code != 200:
        raise Exception(f"Error descargando datos: {response.status_code}")
    
    #Obtener ruta base del proyecto (una carpeta arriba de src)
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    
    raw_dir = os.path.join(base_dir, "data", "raw")
    os.makedirs(raw_dir, exist_ok=True)
    
    file_name = os.path.join(raw_dir, f"omie_{date}.csv")
    
    with open(file_name, "wb") as f:
        f.write(response.content)
    
    df = pd.read_csv(file_name, sep=';', encoding='latin1', skiprows=1, header=None)
    
    return df

######################## Scrapping range ###########################################

from datetime import datetime, timedelta

def download_omie_range(start_date: str, end_date: str):
    """
    Descarga datos entre dos fechas (YYYYMMDD)
    """
    
    current = datetime.strptime(start_date, "%Y%m%d")
    end = datetime.strptime(end_date, "%Y%m%d")
    
    dfs = []
    
    while current <= end:
        date_str = current.strftime("%Y%m%d")
        
        try:
            df = download_omie_price(date_str)
            dfs.append(df)
            print(f"OK: {date_str}")
        except Exception as e:
            print(f"ERROR: {date_str} -> {e}")
        
        current += timedelta(days=1)
    
    return dfs

####################### Scrapping Range v2 #######################################

import time

def download_omie_range_v2(start_date: str, end_date: str) -> pd.DataFrame:
    """
    Descarga datos entre dos fechas (YYYYMMDD) y devuelve un único DataFrame limpio.
    """
    
    current = datetime.strptime(start_date, "%Y%m%d")
    end = datetime.strptime(end_date, "%Y%m%d")
    
    dfs = []
    errors_log = []
    
    while current <= end:
        date_str = current.strftime("%Y%m%d")
        
        try:
            df = download_omie_price(date_str)
            print(f"{date_str}: {df.shape[1]} columnas")
            print(df.head(2))
            
            # Asignar nombres de columnas
            df.columns = ["year", "month", "day", "hour", "price_es", "price_pt","extra"]
            #Hay una columna extra vacia, la quito
            df = df.drop(columns='extra') 
            # Eliminar última fila (asterisco)
            df = df[df.iloc[:, 0] != '*']

            # Convertir a numérico (errors="coerce") convierte valores raros en NaN
            # asi no peta y continua
            df["price_es"] = pd.to_numeric(df["price_es"], errors="coerce")
            df["price_pt"] = pd.to_numeric(df["price_pt"], errors="coerce")
            df["hour"] = pd.to_numeric(df["hour"], errors="coerce")
            
            dfs.append(df)
            print(f"OK: {date_str} ({len(df)} horas)")

        # Guarda los errores del 'try' como e, y los pone para saber donde fallo.    
        except Exception as e:
            print(f"ERROR: {date_str} -> {e}")
            errors_log.append({"date": date_str, "error": str(e)}) 
        time.sleep(0.5)
        current += timedelta(days=1)
    # Si no hay dfs => Error gordo, no hay datos
    if not dfs:
        raise Exception("No se descargaron datos")
    
    # Concatenar todo en un único DataFrame
    df_final = pd.concat(dfs, ignore_index=True)
    
    # Añadir columna datetime
    df_final["datetime"] = pd.to_datetime(df_final[["year", "month", "day", "hour"]]
                           .rename(columns={"hour": "hour"})
                           .assign(hour=df_final["hour"] - 1))
    # Directorios
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    raw_dir = os.path.join(base_dir, "data", "raw")

    # Errors log
    if errors_log:
        df_errors = pd.DataFrame(errors_log)
        df_errors.to_csv(os.path.join(raw_dir, "errors_log.csv"), index=False)

    # Archivo final
    file_name = f"omie_{start_date}_{end_date}.csv"
    df_final.to_csv(os.path.join(raw_dir, file_name), index=False)
    return df_final


######################### Datos de open-meteo ####################################


def download_openmeteo(lat, lon, start_date, end_date, location_name):
    """
    Descarga datos meteorológicos horarios de Open-Meteo.
    Variables: temperatura, irradiación solar, velocidad de viento
    """
    url = "https://archive-api.open-meteo.com/v1/archive"
    
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": "temperature_2m,shortwave_radiation,windspeed_10m",
        "timezone": "Europe/Madrid"
    }
    
    response = requests.get(url, params=params)
    
    if response.status_code != 200:
        raise Exception(f"Error {response.status_code} para {location_name}")
    
    data = response.json()
    
    df = pd.DataFrame({
        "datetime": data["hourly"]["time"],
        "temperature": data["hourly"]["temperature_2m"],
        "solar_radiation": data["hourly"]["shortwave_radiation"],
        "wind_speed": data["hourly"]["windspeed_10m"],
        "location": location_name
    })
    
    df["datetime"] = pd.to_datetime(df["datetime"])
    
    return df