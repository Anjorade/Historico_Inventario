import os
import requests
import pandas as pd
import time
from datetime import datetime
from urllib.parse import quote

# Configuración desde variables de entorno
TOKEN = os.getenv("API_TOKEN")
BASE_URL = os.getenv("API_BASE_URL")
HEADERS = {"token": TOKEN}

# Configuración - Sin modificar delays
REQUEST_DELAY = 20  # Mantener 20 segundos entre páginas
PAGE_SIZE = 15000
MAX_PAGES = 47

# ENDPOINTS - Actualizado
ENDPOINTS = {
    "Consulta_1": "System.InventoryItemsSnap.List.View1"
}

# Configuración de la consulta - Simplificada
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
    """Construye URL sin modificaciones"""
    param_parts = []
    for key, value in params.items():
        encoded_value = quote(str(value))
        param_parts.append(f"{key}={encoded_value}")
    url = f"{BASE_URL}{endpoint}?{'&'.join(param_parts)}"
    return url

def fix_encoding(text):
    """Corrige caracteres especiales de forma optimizada"""
    if not isinstance(text, str):
        return text
    
    # Mapa de corrección de caracteres mal codificados
    encoding_fixes = {
        'Ã': 'Í', 'Ã': 'í', 'Ã¡': 'á', 'Ã©': 'é',
        'Ã­': 'í', 'Ã³': 'ó', 'Ãº': 'ú', 'Ã±': 'ñ',
        'Ã': 'Ñ', 'Â¡': '¡', 'Â¿': '¿', 'â': '"',
        'â': '"', 'â': "'", 'â': "'", 'â¦': '...'
    }
    
    for wrong, correct in encoding_fixes.items():
        text = text.replace(wrong, correct)
    
    return text

def process_dataframe_encoding(df):
    """Procesa DataFrame para corregir encoding en columnas de texto"""
    text_columns = df.select_dtypes(include=['object']).columns
    
    for column in text_columns:
        df[column] = df[column].apply(lambda x: fix_encoding(x) if isinstance(x, str) else x)
    
    return df

def extract_message_data(data):
    """Extrae el array 'message' de la respuesta JSON"""
    try:
        if isinstance(data, dict) and 'message' in data:
            message_data = data['message']
            if isinstance(message_data, list):
                return message_data
        return None
    except:
        return None

def fetch_data_page(url, name, page_number, expected_records=PAGE_SIZE):
    """Obtiene una página de datos"""
    print(f"📄 Consultando página {page_number} para {name}")
    
    try:
        response = requests.get(url, headers=HEADERS, timeout=60)
        response.encoding = 'utf-8'
        response.raise_for_status()
        
        data = response.json()
        message_array = extract_message_data(data)
        
        if not message_array:
            return None, False
        
        df = pd.DataFrame(message_array)
        
        if df.empty:
            return None, False
        
        # Corregir encoding
        df = process_dataframe_encoding(df)
        
        print(f"✅ Página {page_number}: {len(df):,} registros obtenidos")
        
        # Verificar si hay más páginas
        has_more_pages = len(df) >= expected_records
        
        return df, has_more_pages
        
    except requests.exceptions.Timeout:
        print(f"⏰ Timeout en página {page_number}")
        return None, True
        
    except Exception as e:
        print(f"❌ Error en página {page_number}: {str(e)}")
        return None, False

def save_csv_chunks(df, base_filename):
    """Divide y guarda el DataFrame en múltiples archivos CSV"""
    os.makedirs("data", exist_ok=True)
    saved_files = []
    
    # Dividir en chunks de 300,000 registros cada uno
    chunk_size = 300000
    total_records = len(df)
    total_chunks = (total_records // chunk_size) + 1
    
    print(f"📦 Dividiendo {total_records:,} registros en {total_chunks} chunks...")
    
    for chunk_num in range(total_chunks):
        start_idx = chunk_num * chunk_size
        end_idx = min((chunk_num + 1) * chunk_size, total_records)
        chunk_df = df.iloc[start_idx:end_idx]
        
        if not chunk_df.empty:
            if total_chunks > 1:
                filename = f"{base_filename}_part{chunk_num + 1}_of{total_chunks}.csv"
            else:
                filename = f"{base_filename}.csv"
            
            filepath = f"data/{filename}"
            
            # Guardar como CSV sin comprimir con UTF-8
            chunk_df.to_csv(filepath, index=False, encoding='utf-8')
            
            file_size = os.path.getsize(filepath) / 1024 / 1024
            print(f"💾 {filename}: {len(chunk_df):,} filas, {file_size:.2f} MB")
            
            saved_files.append(filepath)
    
    return saved_files

def main():
    print("🚀 INICIANDO CONSULTA - Histórico de Inventarios")
    print("=" * 60)
    print(f"📊 Página size: {PAGE_SIZE:,} registros")
    print(f"⏱️ Delay entre páginas: {REQUEST_DELAY} segundos")
    print(f"🎯 Total esperado: {PAGE_SIZE * MAX_PAGES:,} registros")
    print("=" * 60)
    
    start_time = time.time()
    all_data = []
    query = QUERY_CONFIG[0]
    name = query["name"]
    page_number = 1
    has_more_pages = True
    total_expected = 700000
    
    try:
        while has_more_pages and page_number <= MAX_PAGES:
            # Construir URL con parámetros originales
            query_params = query["params"].copy()
            query_params["skip"] = (page_number - 1) * PAGE_SIZE
            query_params["take"] = PAGE_SIZE
            
            url = build_url(ENDPOINTS[name], query_params)
            
            df_page, has_more_pages = fetch_data_page(url, name, page_number, PAGE_SIZE)
            
            if df_page is not None and not df_page.empty:
                all_data.append(df_page)
                current_total = sum(len(df) for df in all_data)
                
                # Mostrar progreso cada 2 páginas
                if page_number % 2 == 0:
                    progress = (current_total / total_expected * 100)
                    print(f"📊 Progreso: {current_total:,} registros ({progress:.1f}%)")
                
                if has_more_pages:
                    print(f"⏳ Esperando {REQUEST_DELAY} segundos...")
                    time.sleep(REQUEST_DELAY)
            else:
                print(f"⏹️ Fin de datos en página {page_number}")
                break
            
            page_number += 1
            
            if len(all_data) >= total_expected:
                print(f"🎉 ¡Meta alcanzada! {len(all_data):,} registros")
                break

    except KeyboardInterrupt:
        print("\n⏹️ Ejecución interrumpida por el usuario")
    
    # Consolidar y guardar datos
    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        total_records = len(final_df)
        
        print(f"\n💾 Guardando {total_records:,} registros...")
        
        # Generar nombre base con timestamp
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        base_filename = f"Historico_{timestamp}"
        
        # Guardar en chunks
        saved_files = save_csv_chunks(final_df, base_filename)
        
        if saved_files:
            print(f"\n✅ PROCESO COMPLETADO EXITOSAMENTE")
            print(f"📁 Archivos generados en carpeta 'data':")
            for file_path in saved_files:
                file_size = os.path.getsize(file_path) / 1024 / 1024
                print(f"   • {os.path.basename(file_path)} ({file_size:.1f} MB)")
            
            print(f"📊 Total registros guardados: {total_records:,}")
        else:
            print("❌ Error al guardar los archivos CSV")
    else:
        print("❌ No se obtuvieron datos para guardar")

    duration = time.time() - start_time
    minutes = duration / 60
    print(f"\n⏱️ TIEMPO TOTAL: {minutes:.1f} minutos")

if __name__ == "__main__":
    # Asegurar que la carpeta data existe
    os.makedirs("data", exist_ok=True)
    main()
