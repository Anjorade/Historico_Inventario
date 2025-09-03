import os
import requests
import pandas as pd
import time
from datetime import datetime
from urllib.parse import quote
import json

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

def build_url(endpoint, params):
    param_parts = []
    for key, value in params.items():
        encoded_value = quote(str(value))
        param_parts.append(f"{key}={encoded_value}")
    url = f"{BASE_URL}{endpoint}?{'&'.join(param_parts)}"
    return url

def extract_message_data(data):
    """Extrae el array 'message' de la respuesta JSON"""
    try:
        # Verificar si la respuesta tiene la estructura esperada
        if isinstance(data, dict) and 'message' in data:
            message_data = data['message']
            
            if isinstance(message_data, list):
                print(f"✅ Array 'message' encontrado con {len(message_data)} elementos")
                return message_data
            else:
                print(f"⚠️  'message' no es un array, es: {type(message_data)}")
                return None
        else:
            print(f"⚠️  No se encontró clave 'message' en la respuesta")
            print(f"🔍 Keys disponibles: {list(data.keys()) if isinstance(data, dict) else 'N/A'}")
            return None
            
    except Exception as e:
        print(f"❌ Error extrayendo datos de 'message': {e}")
        return None

def fetch_data_page(url, name, page_number):
    print(f"📄 Consultando página {page_number} para {name}")
    try:
        response = requests.get(url, headers=HEADERS, timeout=60)
        response.raise_for_status()
        
        data = response.json()
        print(f"📋 Tipo de respuesta: {type(data)}")
        
        # Guardar respuesta completa para debug
        debug_filename = f"debug_page_{page_number}.json"
        with open(debug_filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        print(f"📁 Respuesta guardada en: {debug_filename}")
        
        # Extraer el array 'message'
        message_array = extract_message_data(data)
        
        if not message_array:
            print(f"❌ No se pudo extraer el array 'message' de la página {page_number}")
            return None, False
        
        # Crear DataFrame directamente desde el array message
        df = pd.DataFrame(message_array)
        
        if df.empty:
            print(f"⚠️  DataFrame vacío después de procesar 'message'")
            return None, False
        
        # Añadir metadatos
        df["load_timestamp"] = datetime.now().isoformat()
        df["page_number"] = page_number
        
        print(f"📊 Página {page_number}: {len(df)} registros, {len(df.columns)} columnas")
        print(f"🏷️  Primeras 5 columnas: {list(df.columns)[:5]}")
        
        # Mostrar preview de los datos
        print(f"👀 Preview de los datos:")
        print(df.head(2).to_string(index=False))
        
        # Verificar si hay más páginas
        has_more_pages = len(df) == PAGE_SIZE
        
        return df, has_more_pages
        
    except Exception as e:
        print(f"❌ Error en página {page_number}: {str(e)}")
        return None, False

def save_data_csv(df, name):
    os.makedirs("data", exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename = f"Historico_{timestamp}.csv"
    path = f"data/{filename}"
    
    try:
        # Guardar como CSV
        df.to_csv(path, index=False, encoding='utf-8')
        
        # Verificar que el archivo se creó correctamente
        if os.path.exists(path):
            file_size = os.path.getsize(path) / 1024 / 1024  # MB
            print(f"✅ CSV guardado exitosamente: {path}")
            print(f"📊 Tamaño: {file_size:.2f} MB")
            print(f"📈 Registros: {len(df)}")
            print(f"🏷️  Columnas: {len(df.columns)}")
            
            # Mostrar estructura del CSV
            print("\n📋 ESTRUCTURA DEL CSV:")
            print(f"Columnas: {list(df.columns)}")
            print(f"\nPrimeras 2 filas:")
            print(df.head(2).to_string(index=False))
            
            return path
        else:
            print("❌ Error: No se pudo crear el archivo CSV")
            return None
            
    except Exception as e:
        print(f"❌ Error guardando CSV: {e}")
        return None

def main():
    print("🚀 Iniciando consulta paginada para Histórico de Inventarios")
    print("🎯 Extrayendo específicamente el array 'message' para CSV")
    
    start_time = time.time()
    all_data = pd.DataFrame()
    query = QUERY_CONFIG[0]
    name = query["name"]
    page_number = 1
    has_more_pages = True
    
    while has_more_pages and page_number <= 10:  # Límite de 10 páginas para prueba
        query_params = query["params"].copy()
        query_params["skip"] = (page_number - 1) * PAGE_SIZE
        
        url = build_url(ENDPOINTS[name], query_params)
        print(f"\n🔗 Página {page_number}: {url}")
        
        df_page, has_more_pages = fetch_data_page(url, name, page_number)
        
        if df_page is not None and not df_page.empty:
            # Concatenar datos
            all_data = pd.concat([all_data, df_page], ignore_index=True)
            print(f"📦 Total acumulado: {len(all_data)} registros")
            
            if has_more_pages:
                print(f"⏳ Esperando {REQUEST_DELAY} segundos...")
                time.sleep(REQUEST_DELAY)
        else:
            print(f"❌ No hay más datos o error en página {page_number}")
            break
        
        page_number += 1

    # Guardar datos finales
    if not all_data.empty:
        print(f"\n💾 Guardando datos finales...")
        print(f"📊 Total de registros a guardar: {len(all_data)}")
        print(f"🏷️  Total de columnas: {len(all_data.columns)}")
        
        csv_path = save_data_csv(all_data, name)
        
        if csv_path:
            print(f"\n🎉 PROCESO COMPLETADO EXITOSAMENTE")
            print(f"📁 Archivo CSV: {csv_path}")
            print(f"📊 Total registros: {len(all_data)}")
        else:
            print("❌ Error al guardar el archivo CSV final")
    else:
        print("❌ No se obtuvieron datos para guardar")

    duration = time.time() - start_time
    print(f"\n⏱️  Tiempo total: {duration:.2f} segundos")

if __name__ == "__main__":
    main()
