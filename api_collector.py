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

# Configuración optimizada para mayor velocidad
MAX_RETRIES = 0  # Sin reintentos
REQUEST_DELAY = 15  # Reducido a 15 segundos entre páginas
PAGE_SIZE = 15000  # Aumentado a 15,000 registros por página
MAX_PAGES = 47  # Para 700,000 registros (700,000 / 15,000 = 46.67 → 47 páginas)

# ENDPOINTS - Actualizado
ENDPOINTS = {
    "Consulta_1": "System.InventoryItemsSnap.List.View1"
}

# Configuración de la consulta - Simplificada
QUERY_CONFIG = [
    {
        "name": "Consulta_1",
        "params": {
            "orderby": "civi_snapshot_date desc",  # Campo actualizado
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
            
            f.write("\n=== MUESTRA DEL CONTENIDO (primeros 2000 caracteres) ===\n")
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
    
    try:
        response = requests.get(url, headers=HEADERS, timeout=90)  # Timeout optimizado
        response.raise_for_status()
        
        # Obtener la respuesta JSON
        data = response.json()
        
        # 🔍 Debug solo para las primeras 2 páginas para no hacer lento el proceso
        if page_number <= 2:
            debug_response_structure(data, page_number)
        
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
        
        print(f"📊 Página {page_number}: {len(df)} registros obtenidos")
        
        # Verificar si hay más páginas
        has_more_pages = len(df) >= expected_records
        
        if not has_more_pages:
            print(f"📄 Última página detectada: se obtuvieron {len(df)} registros")
        
        return df, has_more_pages
        
    except requests.exceptions.Timeout:
        print(f"⏰ Timeout en página {page_number}")
        return None, True
        
    except Exception as e:
        print(f"❌ Error en página {page_number}: {str(e)}")
        return None, False

def save_data_csv_chunked(df, name, max_size_mb=90):
    """Guarda el DataFrame en múltiples archivos CSV que no excedan el tamaño máximo"""
    os.makedirs("data", exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    base_filename = f"Historico_{timestamp}"
    
    # Dividir en chunks de aproximadamente 400,000 filas cada uno
    rows_per_chunk = 400000
    total_chunks = (len(df) // rows_per_chunk) + 1
    saved_files = []
    
    print(f"📦 Dividiendo {len(df):,} registros en {total_chunks} chunks...")
    
    for chunk_num in range(total_chunks):
        start_idx = chunk_num * rows_per_chunk
        end_idx = min((chunk_num + 1) * rows_per_chunk, len(df))
        chunk_df = df.iloc[start_idx:end_idx]
        
        if not chunk_df.empty:
            chunk_filename = f"{base_filename}_part{chunk_num + 1}_of{total_chunks}.csv"
            chunk_path = f"data/{chunk_filename}"
            
            chunk_df.to_csv(chunk_path, index=False, encoding='utf-8')
            chunk_size = os.path.getsize(chunk_path) / 1024 / 1024
            
            print(f"💾 Chunk {chunk_num + 1}: {len(chunk_df):,} filas, {chunk_size:.2f} MB")
            saved_files.append(chunk_path)
    
    return saved_files

def main():
    print("🚀 INICIANDO CONSULTA MANUAL - Histórico de Inventarios")
    print("=" * 60)
    print(f"📊 Página size: {PAGE_SIZE:,} registros")
    print(f"🎯 Total esperado: 700,000 registros")
    print(f"📈 Páginas estimadas: {MAX_PAGES}")
    print(f"⏱️  Tiempo estimado: 20-25 minutos")
    print("=" * 60)
    
    start_time = time.time()
    all_data = pd.DataFrame()
    query = QUERY_CONFIG[0]
    name = query["name"]
    page_number = 1
    has_more_pages = True
    total_expected = 700000
    
    try:
        while has_more_pages and page_number <= MAX_PAGES:
            # Actualizar parámetros de paginación
            query_params = query["params"].copy()
            query_params["skip"] = (page_number - 1) * PAGE_SIZE
            query_params["take"] = PAGE_SIZE
            
            url = build_url(ENDPOINTS[name], query_params)
            print(f"\n📖 Página {page_number} - Skip: {query_params['skip']:,}")
            
            df_page, has_more_pages = fetch_data_page(url, name, page_number, PAGE_SIZE)
            
            if df_page is not None and not df_page.empty:
                # Concatenar datos
                all_data = pd.concat([all_data, df_page], ignore_index=True)
                current_total = len(all_data)
                
                print(f"📦 Total acumulado: {current_total:,} registros")
                print(f"📊 Progreso: {(current_total/total_expected*100):.1f}%")
                
                # Calcular tiempo estimado restante
                elapsed_time = time.time() - start_time
                if page_number > 1:
                    time_per_page = elapsed_time / (page_number - 1)
                    remaining_pages = MAX_PAGES - page_number
                    estimated_remaining = time_per_page * remaining_pages
                    print(f"⏱️  Tiempo estimado restante: {estimated_remaining/60:.1f} minutos")
                
                if has_more_pages:
                    print(f"⏳ Esperando {REQUEST_DELAY} segundos...")
                    time.sleep(REQUEST_DELAY)
                else:
                    print(f"🎯 Se alcanzó el final de los datos")
                    
            else:
                print(f"❌ Error en página {page_number}, deteniendo la consulta")
                break
            
            page_number += 1
            
            if len(all_data) >= total_expected:
                print(f"🎉 ¡Meta alcanzada! {len(all_data):,} registros obtenidos")
                break

    except KeyboardInterrupt:
        print("\n⏹️  Ejecución interrumpida por el usuario")
    
    # Guardar datos finales
    if not all_data.empty:
        print(f"\n💾 Guardando {len(all_data):,} registros...")
        
        saved_files = save_data_csv_chunked(all_data, name)
        
        if saved_files:
            print(f"\n🎉 PROCESO COMPLETADO EXITOSAMENTE")
            print(f"📁 Archivos generados:")
            for file_path in saved_files:
                file_size = os.path.getsize(file_path) / 1024 / 1024
                print(f"   • {os.path.basename(file_path)} ({file_size:.1f} MB)")
        else:
            print("❌ Error al guardar los archivos CSV")
    else:
        print("❌ No se obtuvieron datos para guardar")

    duration = time.time() - start_time
    minutes = duration / 60
    print(f"\n⏱️  TIEMPO TOTAL: {duration:.2f} segundos ({minutes:.1f} minutos)")
    print(f"📊 Velocidad: {len(all_data)/duration:.1f} registros/segundo")
    print(f"📈 Rendimiento: {len(all_data)/minutes:.0f} registros/minuto")

if __name__ == "__main__":
    main()
