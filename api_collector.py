import os
import requests
import pandas as pd
import time
from datetime import datetime
from urllib.parse import quote
import json
from flatten_json import flatten

# Configuración segura desde secretos
TOKEN = os.getenv("API_TOKEN")
BASE_URL = os.getenv("API_BASE_URL")
HEADERS = {"token": TOKEN}

# Configuración modificada según requerimientos
MAX_RETRIES = 0  # Sin reintentos
REQUEST_DELAY = 20  # 20 segundos entre páginas
PAGE_SIZE = 10000  # 10,000 registros por página

# ENDPOINTS
ENDPOINTS = {
    "Consulta_1": "System.InventoryItemsSnap.List.View1"
}

# Configuración de la consulta
QUERY_CONFIG = [
    {
        "name": "Consulta_1",
        "params": {
            "orderby": "civi_snapshot_date desc",
            "take": str(PAGE_SIZE),
            "skip": 0
        }
    }
]

def install_flatten_json():
    """Instalar la librería flatten-json si no está disponible"""
    try:
        import flatten_json
    except ImportError:
        print("📦 Instalando librería flatten-json...")
        import subprocess
        subprocess.check_call(["pip", "install", "flatten-json"])
        import flatten_json
    return flatten_json

def flatten_json_data(data):
    """Aplanar JSON anidado a una estructura plana"""
    flattened_data = []
    
    if isinstance(data, list):
        for item in data:
            if isinstance(item, dict):
                flattened_item = flatten(item, separator='_')
                flattened_data.append(flattened_item)
            else:
                flattened_data.append({'value': item})
    elif isinstance(data, dict):
        flattened_data.append(flatten(data, separator='_'))
    else:
        flattened_data.append({'value': data})
    
    return flattened_data

def build_url(endpoint, params):
    param_parts = []
    for key, value in params.items():
        encoded_value = quote(str(value))
        param_parts.append(f"{key}={encoded_value}")
    url = f"{BASE_URL}{endpoint}?{'&'.join(param_parts)}"
    return url

def fetch_data_page(url, name, page_number):
    print(f"📄 Consultando página {page_number} para {name}")
    try:
        response = requests.get(url, headers=HEADERS, timeout=60)
        response.raise_for_status()
        
        # Debug: ver estructura de la respuesta
        data = response.json()
        print(f"📋 Tipo de respuesta: {type(data)}")
        
        if isinstance(data, dict):
            print(f"🔍 Keys en la respuesta: {list(data.keys())}")
            # Guardar muestra de la respuesta para análisis
            with open(f'debug_response_page_{page_number}.json', 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
        
        # Aplanar el JSON
        flattened_data = flatten_json_data(data)
        
        if not flattened_data:
            print(f"⚠️  Página {page_number} no devolvió datos válidos")
            return None, False
        
        # Crear DataFrame
        df = pd.DataFrame(flattened_data)
        
        # Añadir metadatos
        df["load_timestamp"] = datetime.now().isoformat()
        df["page_number"] = page_number
        
        print(f"📊 Página {page_number}: {len(df)} registros, {len(df.columns)} columnas")
        print(f"🏷️  Columnas: {list(df.columns)[:10]}{'...' if len(df.columns) > 10 else ''}")
        
        # Verificar si hay más páginas
        has_more_pages = len(df) == PAGE_SIZE
        
        return df, has_more_pages
        
    except Exception as e:
        print(f"⚠️  Error en página {page_number}: {str(e)}")
        return None, False

def save_data_csv(df, name):
    os.makedirs("data", exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename = f"Historico_{timestamp}.csv"
    path = f"data/{filename}"
    
    # Guardar como CSV
    df.to_csv(path, index=False, encoding='utf-8', sep=',')
    
    # Verificar que el archivo se creó correctamente
    if os.path.exists(path):
        file_size = os.path.getsize(path) / 1024 / 1024  # MB
        print(f"💾 CSV guardado: {path}")
        print(f"📊 Tamaño: {file_size:.2f} MB")
        print(f"📈 Registros: {len(df)}")
        print(f"🏷️  Columnas: {len(df.columns)}")
        
        # Mostrar preview
        print("\n📋 PREVIEW DEL CSV:")
        print(df.head(3).to_string())
        
        return path
    else:
        print("❌ Error: No se pudo crear el archivo CSV")
        return None

def main():
    # Instalar dependencias necesarias
    install_flatten_json()
    
    print("🚀 Iniciando consulta paginada para Histórico de Inventarios")
    print("📝 Los datos se guardarán en formato CSV organizado por filas y columnas")
    
    start_time = time.time()
    all_data = pd.DataFrame()
    query = QUERY_CONFIG[0]
    name = query["name"]
    page_number = 1
    has_more_pages = True
    
    while has_more_pages:
        query_params = query["params"].copy()
        query_params["skip"] = (page_number - 1) * PAGE_SIZE
        
        url = build_url(ENDPOINTS[name], query_params)
        print(f"\n🔗 Página {page_number}: {url}")
        
        df_page, has_more_pages = fetch_data_page(url, name, page_number)
        
        if df_page is not None and not df_page.empty:
            # Concatenar datos manteniendo todas las columnas
            all_data = pd.concat([all_data, df_page], ignore_index=True, sort=False)
            print(f"📦 Total acumulado: {len(all_data)} registros")
            
            if has_more_pages:
                print(f"⏳ Esperando {REQUEST_DELAY} segundos...")
                time.sleep(REQUEST_DELAY)
        else:
            print(f"❌ No hay más datos o error en página {page_number}")
            break
        
        page_number += 1
        
        # Límite de seguridad
        if page_number > 10:  # Reducido para pruebas
            print("⚠️  Límite de páginas alcanzado (10 páginas)")
            break

    # Guardar datos finales
    if not all_data.empty:
        csv_path = save_data_csv(all_data, name)
        if csv_path:
            print(f"\n✅ PROCESO COMPLETADO EXITOSAMENTE")
            print(f"📁 Archivo: {csv_path}")
            print(f"📊 Total registros: {len(all_data)}")
            print(f"🏷️  Total columnas: {len(all_data.columns)}")
        else:
            print("❌ Error al guardar el archivo CSV")
    else:
        print("❌ No se obtuvieron datos para guardar")

    duration = time.time() - start_time
    print(f"\n⏱️  Tiempo total: {duration:.2f} segundos")

if __name__ == "__main__":
    main()
