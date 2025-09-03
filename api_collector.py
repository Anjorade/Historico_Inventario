import os
import requests
import pandas as pd
import time
from datetime import datetime
from urllib.parse import quote

# Configuración segura desde secretos
TOKEN = os.getenv("API_TOKEN")
BASE_URL = os.getenv("API_BASE_URL")
HEADERS = {"token": TOKEN}

# Configuración modificada según requerimientos
MAX_RETRIES = 0  # Sin reintentos
REQUEST_DELAY = 20  # 20 segundos entre páginas
PAGE_SIZE = 10000  # 10,000 registros por página
TOTAL_ROWS = 700000  # Total estimado de registros

# ENDPOINTS - Solo mantener consulta1
ENDPOINTS = {
    "Consulta_1": "System.InventoryItemsSnap.List.View1"
}

# Configuración de la consulta - Solo Consulta_1
QUERY_CONFIG = [
    {
        "name": "Consulta_1",
        "params": {
            "orderby": "civi_snapshot_date desc",
            "take": str(PAGE_SIZE),  # Usar PAGE_SIZE dinámico
            "skip": 0,  # Inicializar skip en 0
        }
    }
]

def build_url(endpoint, params):
    param_parts = []
    for key, value in params.items():
        # Codificar valores para URL
        encoded_value = quote(str(value))
        param_parts.append(f"{key}={encoded_value}")
    url = f"{BASE_URL}{endpoint}?{'&'.join(param_parts)}"
    return url

def fetch_data_page(url, name, page_number):
    print(f"📄 Consultando página {page_number} para {name}")
    try:
        response = requests.get(url, headers=HEADERS, timeout=60)
        response.raise_for_status()
        data = response.json()
        
        if not data:
            print(f"⚠️  Página {page_number} de {name} no devolvió datos.")
            return None, False
            
        if isinstance(data, dict) and data.get("error"):
            print(f"⚠️  Error en página {page_number} de {name}: {data.get('error')}")
            return None, False
            
        df = pd.json_normalize(data)
        df["load_timestamp"] = datetime.now().isoformat()
        df["page_number"] = page_number
        
        # Verificar si hay más páginas (si obtenemos menos registros de los solicitados)
        has_more_pages = len(df) == PAGE_SIZE
        return df, has_more_pages
        
    except requests.exceptions.RequestException as e:
        print(f"⚠️  Error en página {page_number} de {name}: {e}")
        return None, False
    except ValueError as e:
        print(f"⚠️  Error al decodificar JSON en página {page_number} de {name}: {e}")
        return None, False

def save_data_csv(df, name):
    os.makedirs("data", exist_ok=True)
    # Formato: Histórico_YYYY-MM-DD_HH-MM-SS.csv
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename = f"Historico_{timestamp}.csv"
    path = f"data/{filename}"
    df.to_csv(path, index=False, encoding='utf-8')
    print(f"💾 Guardado: {path} - {len(df)} registros")
    return path

def main():
    print("🚀 Iniciando consulta paginada para Histórico de Inventarios")
    start_time = time.time()

    all_data = pd.DataFrame()
    query = QUERY_CONFIG[0]  # Solo la primera consulta
    name = query["name"]
    page_number = 1
    
    has_more_pages = True
    
    while has_more_pages:
        # Construir URL con paginación
        query_params = query["params"].copy()
        query_params["skip"] = (page_number - 1) * PAGE_SIZE
        
        url = build_url(ENDPOINTS[name], query_params)
        
        # Consultar página
        df_page, has_more_pages = fetch_data_page(url, name, page_number)
        
        if df_page is not None:
            # Concatenar datos
            all_data = pd.concat([all_data, df_page], ignore_index=True)
            print(f"📊 Página {page_number}: {len(df_page)} registros - Total acumulado: {len(all_data)}")
            
            # Espera de 20 segundos entre páginas
            if has_more_pages:
                print(f"⏳ Esperando {REQUEST_DELAY} segundos antes de la siguiente página...")
                time.sleep(REQUEST_DELAY)
        else:
            print(f"❌ Error en página {page_number}, deteniendo la consulta.")
            break
        
        page_number += 1
        
        # Límite de seguridad (máximo 70 páginas para 700,000 registros)
        if page_number > 70:
            print("⚠️  Límite máximo de páginas alcanzado (70 páginas)")
            break

    # Guardar todos los datos en CSV
    if not all_data.empty:
        file_path = save_data_csv(all_data, name)
        print(f"✅ Proceso completado. Total de registros obtenidos: {len(all_data)}")
        print(f"📁 Archivo guardado como: {file_path}")
    else:
        print("❌ No se obtuvieron datos.")

    duration = time.time() - start_time
    print(f"⏱️  Tiempo total del proceso: {duration:.2f} segundos")

if __name__ == "__main__":
    main()
