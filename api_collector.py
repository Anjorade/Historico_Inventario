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
MAX_PAGES = 100  # Límite máximo de páginas para 1,000,000 registros

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

def debug_response_structure(data, page_number):
    """Analiza en profundidad la estructura de la respuesta"""
    try:
        debug_filename = f"debug_structure_page_{page_number}.txt"
        with open(debug_filename, 'w', encoding='utf-8') as f:
            f.write("=== ESTRUCTURA COMPLETA DE LA RESPUESTA ===\n")
            f.write(f"Tipo: {type(data)}\n")
            f.write(f"Página: {page_number}\n")
            f.write(f"Timestamp: {datetime.now().isoformat()}\n\n")
            
            if isinstance(data, dict):
                f.write("🔑 KEYS DEL OBJETO PRINCIPAL:\n")
                f.write("=" * 50 + "\n")
                for key, value in data.items():
                    f.write(f"  {key}: {type(value)}")
                    if isinstance(value, (list, dict)):
                        if isinstance(value, list):
                            f.write(f" (elementos: {len(value)})")
                        else:
                            f.write(f" (keys: {len(value)})")
                    f.write("\n")
                
                # Profundizar en la estructura de 'message' si existe
                if 'message' in data and isinstance(data['message'], list):
                    f.write("\n📋 ESTRUCTURA DEL ARRAY 'message':\n")
                    f.write("=" * 50 + "\n")
                    if len(data['message']) > 0:
                        first_item = data['message'][0]
                        f.write(f"Primer elemento tipo: {type(first_item)}\n")
                        if isinstance(first_item, dict):
                            f.write(f"Keys del primer elemento: {list(first_item.keys())}\n")
                            f.write(f"Número de keys: {len(first_item.keys())}\n")
                        
                        f.write(f"\nTotal elementos en 'message': {len(data['message'])}\n")
                    else:
                        f.write("Array 'message' está vacío\n")
                
                # Mostrar también otras keys importantes
                for key in ['total', 'count', 'records', 'items']:
                    if key in data:
                        f.write(f"\n📊 {key.upper()}: {data[key]} (tipo: {type(data[key])})\n")
            
            elif isinstance(data, list):
                f.write("📋 LA RESPUESTA ES DIRECTAMENTE UN ARRAY:\n")
                f.write(f"Total elementos: {len(data)}\n")
                if len(data) > 0:
                    f.write(f"Tipo de elementos: {type(data[0])}\n")
                    if isinstance(data[0], dict):
                        f.write(f"Keys del primer elemento: {list(data[0].keys())}\n")
            
            f.write("\n=== MUESTRA DEL CONTENIDO ( primeros 2000 caracteres) ===\n")
            f.write("=" * 60 + "\n")
            sample_content = json.dumps(data, indent=2, ensure_ascii=False)
            f.write(sample_content[:2000])
            if len(sample_content) > 2000:
                f.write("\n... [CONTINÚA]")
        
        print(f"📁 Debug de estructura guardado en: {debug_filename}")
        return True
        
    except Exception as e:
        print(f"❌ Error en debug_response_structure: {e}")
        return False

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
            if isinstance(data, dict):
                print(f"🔍 Keys disponibles: {list(data.keys())}")
            return None
            
    except Exception as e:
        print(f"❌ Error extrayendo datos de 'message': {e}")
        return None

def fetch_data_page(url, name, page_number, expected_records=PAGE_SIZE):
    print(f"📄 Consultando página {page_number} para {name}")
    print(f"🔗 URL: {url}")
    
    try:
        response = requests.get(url, headers=HEADERS, timeout=120)
        response.raise_for_status()
        
        # Obtener la respuesta JSON
        data = response.json()
        print(f"📋 Tipo de respuesta: {type(data)}")
        
        # 🔍 LLAMAR A LA FUNCIÓN DE DEBUG AQUÍ 🔍
        print("🔍 Ejecutando análisis de estructura de la respuesta...")
        debug_response_structure(data, page_number)
        
        # Extraer el array 'message'
        message_array = extract_message_data(data)
        
        if not message_array:
            print(f"❌ No se pudo extraer el array 'message' de la página {page_number}")
            # Guardar respuesta completa para debug
            debug_filename = f"debug_error_page_{page_number}.json"
            with open(debug_filename, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            print(f"📁 Respuesta error guardada en: {debug_filename}")
            return None, False
        
        # Crear DataFrame directamente desde el array message
        df = pd.DataFrame(message_array)
        
        if df.empty:
            print(f"⚠️  DataFrame vacío después de procesar 'message'")
            return None, False
        
        # Añadir metadatos
        df["load_timestamp"] = datetime.now().isoformat()
        df["page_number"] = page_number
        
        print(f"📊 Página {page_number}: {len(df)} registros obtenidos (esperados: {expected_records})")
        print(f"🏷️  Columnas: {len(df.columns)}")
        
        # Verificar si hay más páginas
        has_more_pages = len(df) >= expected_records
        
        if not has_more_pages:
            print(f"📄 Última página detectada: se obtuvieron {len(df)} registros de {expected_records} esperados")
        
        return df, has_more_pages
        
    except requests.exceptions.Timeout:
        print(f"⏰ Timeout en página {page_number}")
        return None, True
        
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
        
        if os.path.exists(path):
            file_size = os.path.getsize(path) / 1024 / 1024
            print(f"✅ CSV guardado exitosamente: {path}")
            print(f"📊 Tamaño: {file_size:.2f} MB")
            print(f"📈 Registros: {len(df)}")
            print(f"🏷️  Columnas: {len(df.columns)}")
            
            return path
        else:
            print("❌ Error: No se pudo crear el archivo CSV")
            return None
            
    except Exception as e:
        print(f"❌ Error guardando CSV: {e}")
        return None

def main():
    print("🚀 Iniciando consulta paginada para Histórico de Inventarios")
    print(f"🎯 Objetivo: 700,000 registros con paginación de {PAGE_SIZE} por página")
    print("🔍 Modo debug activado - Se generarán archivos de análisis\n")
    
    start_time = time.time()
    all_data = pd.DataFrame()
    query = QUERY_CONFIG[0]
    name = query["name"]
    page_number = 1
    has_more_pages = True
    total_expected = 700000
    
    while has_more_pages and page_number <= MAX_PAGES:
        # Actualizar parámetros de paginación
        query_params = query["params"].copy()
        query_params["skip"] = (page_number - 1) * PAGE_SIZE
        query_params["take"] = PAGE_SIZE
        
        url = build_url(ENDPOINTS[name], query_params)
        print(f"\n{'='*60}")
        print(f"📖 Página {page_number} - Skip: {query_params['skip']}")
        print(f"{'='*60}")
        
        df_page, has_more_pages = fetch_data_page(url, name, page_number, PAGE_SIZE)
        
        if df_page is not None and not df_page.empty:
            # Concatenar datos
            all_data = pd.concat([all_data, df_page], ignore_index=True)
            current_total = len(all_data)
            
            print(f"📦 Total acumulado: {current_total:,} registros")
            print(f"📊 Progreso: {(current_total/total_expected*100):.1f}%")
            
            if has_more_pages:
                print(f"⏳ Esperando {REQUEST_DELAY} segundos para la siguiente página...")
                time.sleep(REQUEST_DELAY)
            else:
                print(f"🎯 Se alcanzó el final de los datos en página {page_number}")
                
        else:
            print(f"❌ Error en página {page_number}, deteniendo la consulta")
            break
        
        page_number += 1
        
        if len(all_data) >= total_expected:
            print(f"🎉 ¡Meta alcanzada! {len(all_data):,} registros obtenidos")
            break

    # Guardar datos finales
    if not all_data.empty:
        print(f"\n💾 Guardando datos finales...")
        print(f"📊 Total de registros obtenidos: {len(all_data):,}")
        
        csv_path = save_data_csv(all_data, name)
        
        if csv_path:
            print(f"\n🎉 PROCESO COMPLETADO EXITOSAMENTE")
            print(f"📁 Archivo CSV: {csv_path}")
            print(f"📊 Total registros: {len(all_data):,}")
            print(f"📄 Total páginas procesadas: {page_number - 1}")
        else:
            print("❌ Error al guardar el archivo CSV final")
    else:
        print("❌ No se obtuvieron datos para guardar")

    duration = time.time() - start_time
    minutes = duration / 60
    print(f"\n⏱️  Tiempo total: {duration:.2f} segundos ({minutes:.1f} minutos)")

if __name__ == "__main__":
    main()
