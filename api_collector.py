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

# Configuración (sin modificar)
REQUEST_DELAY = 20
PAGE_SIZE = 15000
MAX_PAGES = 2

# Endpoint
ENDPOINT = "System.InventoryItemsSnap.List.View1"

def build_url(skip):
    """Construye URL (sin modificar)"""
    params = {
        "orderby": "civi_snapshot_date desc",
        "take": PAGE_SIZE,
        "skip": skip
    }
    param_str = "&".join([f"{k}={quote(str(v))}" for k, v in params.items()])
    return f"{BASE_URL}{ENDPOINT}?{param_str}"

def fetch_data_page(page_number, skip):
    """Obtiene una página de datos de forma optimizada"""
    url = build_url(skip)
    
    try:
        # Timeout reducido para mejor performance
        response = requests.get(url, headers=HEADERS, timeout=45)
        response.raise_for_status()
        
        data = response.json()
        
        # Extraer array 'message' directamente
        if isinstance(data, dict) and 'message' in data and isinstance(data['message'], list):
            df = pd.DataFrame(data['message'])
            
            if not df.empty:
                print(f"✅ Página {page_number}: {len(df):,} registros")
                return df, True
            
        return None, False
        
    except requests.exceptions.Timeout:
        print(f"⏰ Timeout página {page_number}")
        return None, True
        
    except Exception as e:
        print(f"❌ Error página {page_number}: {str(e)}")
        return None, False

def save_csv(df, filename):
    """Guarda DataFrame como CSV con UTF-8 de forma optimizada"""
    filepath = f"data/{filename}"
    
    # Guardar con UTF-8 sin forzar comillas (más eficiente)
    df.to_csv(filepath, index=False, encoding='utf-8')
    
    file_size = os.path.getsize(filepath) / 1024 / 1024
    print(f"💾 {filename}: {len(df):,} filas, {file_size:.1f} MB")
    
    return filepath

def main():
    print("🚀 INICIANDO CONSULTA - Histórico de Inventarios")
    print("=" * 50)
    print(f"📊 Página size: {PAGE_SIZE:,} registros")
    print(f"⏱️ Delay entre páginas: {REQUEST_DELAY}s")
    print("=" * 50)
    
    start_time = time.time()
    all_data = []
    page_number = 1
    has_more_pages = True
    
    # Usar lista en lugar de concatenación continua para mejor performance
    try:
        while has_more_pages and page_number <= MAX_PAGES:
            skip = (page_number - 1) * PAGE_SIZE
            df_page, has_more_pages = fetch_data_page(page_number, skip)
            
            if df_page is not None:
                all_data.append(df_page)
                current_total = sum(len(df) for df in all_data)
                
                # Mostrar progreso cada página para mejor feedback
                print(f"📊 Progreso: {current_total:,} registros")
                
                if has_more_pages:
                    time.sleep(REQUEST_DELAY)
            else:
                print(f"⏹️ Fin de datos en página {page_number}")
                break
            
            page_number += 1

    except KeyboardInterrupt:
        print("\n⏹️ Ejecución interrumpida por usuario")
    
    # Consolidar y guardar datos
    if all_data:
        # Concatenar todos los DataFrames al final (más eficiente)
        final_df = pd.concat(all_data, ignore_index=True)
        total_records = len(final_df)
        
        print(f"\n💾 Guardando {total_records:,} registros...")
        
        timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        base_filename = f"Historico_{timestamp}"
        
        # Dividir en chunks de 400,000 registros para archivos manejables
        if total_records > 400000:
            chunk_size = 400000
            chunks = (total_records // chunk_size) + 1
            
            for i in range(chunks):
                start_idx = i * chunk_size
                end_idx = min((i + 1) * chunk_size, total_records)
                chunk_df = final_df.iloc[start_idx:end_idx]
                
                filename = f"{base_filename}_part{i+1}.csv"
                save_csv(chunk_df, filename)
        else:
            # Archivo único
            filename = f"{base_filename}.csv"
            save_csv(final_df, filename)
        
        print(f"\n✅ PROCESO COMPLETADO")
        print(f"📊 Total registros: {total_records:,}")
        print(f"📁 Archivos guardados en: data/")
        
    else:
        print("❌ No se obtuvieron datos")
    
    duration = time.time() - start_time
    minutes = duration / 60
    print(f"⏱️ Tiempo total: {minutes:.1f} minutos")

if __name__ == "__main__":
    # Asegurar que la carpeta data existe
    os.makedirs("data", exist_ok=True)
    main()
