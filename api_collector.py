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

# Configuración
REQUEST_DELAY = 20
PAGE_SIZE = 15000
MAX_PAGES = 2

# Endpoint
ENDPOINT = "System.InventoryItemsSnap.List.View1"

def build_url(skip):
    """Construye URL"""
    params = {
        "orderby": "civi_snapshot_date desc",
        "take": PAGE_SIZE,
        "skip": skip
    }
    param_str = "&".join([f"{k}={quote(str(v))}" for k, v in params.items()])
    return f"{BASE_URL}{ENDPOINT}?{param_str}"

def fix_encoding_complete(text):
    """Corrección completa de encoding para caracteres españoles"""
    if not isinstance(text, str):
        return text
    
    # Si el texto parece estar en Latin-1 pero debería ser UTF-8
    try:
        # Intentar decodificar como Latin-1 y luego re-encodar a UTF-8
        if any(char in text for char in ['Ã', 'Â', 'â']):
            # Decodificar como Latin-1 y luego codificar como UTF-8
            corrected = text.encode('latin-1').decode('utf-8')
            return corrected
    except (UnicodeEncodeError, UnicodeDecodeError):
        pass
    
    # Mapa de correcciones específicas para caracteres comunes
    encoding_map = {
        'Ã': 'Í', 'Ã': 'í', 'Ã¡': 'á', 'Ã©': 'é',
        'Ã­': 'í', 'Ã³': 'ó', 'Ãº': 'ú', 'Ã±': 'ñ',
        'Ã': 'Ñ', 'Â¡': '¡', 'Â¿': '¿', 'Ã€': 'À',
        'Ã': 'È', 'ÃŒ': 'Ì', 'Ã': 'Ò', 'Ã™': 'Ù',
        'Ã§': 'ç', 'Ã£': 'ã', 'Ãµ': 'õ', 'Ãª': 'ê',
        'Ã®': 'î', 'Ã´': 'ô', 'Ã»': 'û', 'Ã¤': 'ä',
        'Ã«': 'ë', 'Ã¯': 'ï', 'Ã¶': 'ö', 'Ã¼': 'ü',
        'Ã¿': 'ÿ', 'Ã¦': 'æ', 'Å': 'œ', 'Å¡': 'š',
        'Å¾': 'ž', 'Å¸': 'Ÿ', 'â‚¬': '€', 'â€š': '‚',
        'â€ž': '„', 'â€¦': '…', 'â€¡': '‡', 'â€°': '‰',
        'â€¹': '‹', 'â€˜': '‘', 'â€™': '’', 'â€œ': '“',
        'â€¢': '•', 'â€“': '–', 'â€”': '—', 'â„¢': '™',
        'â€º': '›'
    }
    
    for wrong, correct in encoding_map.items():
        text = text.replace(wrong, correct)
    
    return text

def process_dataframe_encoding(df):
    """Procesa todas las columnas de texto para corregir encoding"""
    print("🔧 Corrigiendo encoding de caracteres especiales...")
    
    for col in df.columns:
        if df[col].dtype == 'object':
            # Aplicar corrección a todos los valores string
            df[col] = df[col].apply(lambda x: fix_encoding_complete(x) if isinstance(x, str) else x)
    
    return df

def fetch_data_page(page_number, skip):
    """Obtiene una página de datos"""
    url = build_url(skip)
    
    try:
        response = requests.get(url, headers=HEADERS, timeout=60)
        
        # Forzar encoding UTF-8 en la respuesta
        response.encoding = 'utf-8'
        response.raise_for_status()
        
        data = response.json()
        
        # Extraer array 'message'
        if isinstance(data, dict) and 'message' in data and isinstance(data['message'], list):
            df = pd.DataFrame(data['message'])
            
            if not df.empty:
                # Aplicar corrección de encoding
                df = process_dataframe_encoding(df)
                print(f"✅ Página {page_number}: {len(df):,} registros")
                return df, True
            
        return None, False
        
    except requests.exceptions.Timeout:
        print(f"⏰ Timeout página {page_number}")
        return None, True
        
    except Exception as e:
        print(f"❌ Error página {page_number}: {str(e)}")
        return None, False

def save_csv_with_correct_encoding(df, filename):
    """Guarda CSV con encoding UTF-8 garantizado"""
    filepath = f"data/{filename}"
    
    # Guardar con UTF-8 y forzar comillas para preservar encoding
    df.to_csv(filepath, index=False, encoding='utf-8', quoting=1)
    
    # Verificar que el encoding sea correcto
    file_size = os.path.getsize(filepath) / 1024 / 1024
    print(f"💾 {filename}: {len(df):,} filas, {file_size:.1f} MB")
    
    # Verificación rápida de encoding
    try:
        with open(filepath, 'r', encoding='utf-8', errors='strict') as f:
            first_line = f.readline()
            if 'Ã' in first_line or 'Â' in first_line:
                print(f"⚠️  Posible problema de encoding en {filename}")
    except:
        pass
    
    return filepath

def main():
    print("🚀 INICIANDO CONSULTA - Histórico de Inventarios")
    print("=" * 60)
    print("🔠 Encoding: UTF-8 con corrección de caracteres especiales")
    print("=" * 60)
    
    start_time = time.time()
    all_data = []
    page_number = 1
    has_more_pages = True
    
    try:
        while has_more_pages and page_number <= MAX_PAGES:
            skip = (page_number - 1) * PAGE_SIZE
            df_page, has_more_pages = fetch_data_page(page_number, skip)
            
            if df_page is not None:
                all_data.append(df_page)
                current_total = sum(len(df) for df in all_data)
                
                if page_number % 2 == 0:
                    print(f"📊 Progreso: {current_total:,} registros")
                
                if has_more_pages:
                    time.sleep(REQUEST_DELAY)
            else:
                has_more_pages = False
            
            page_number += 1

    except KeyboardInterrupt:
        print("\n⏹️ Ejecución interrumpida")
    
    # Consolidar y guardar datos
    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        total_records = len(final_df)
        
        print(f"\n💾 Guardando {total_records:,} registros con encoding UTF-8...")
        
        timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        
        # Dividir en chunks si es necesario
        if total_records > 300000:
            chunk_size = 300000
            chunks = (total_records // chunk_size) + 1
            
            for i in range(chunks):
                start_idx = i * chunk_size
                end_idx = min((i + 1) * chunk_size, total_records)
                chunk_df = final_df.iloc[start_idx:end_idx]
                
                filename = f"Historico_{timestamp}_part{i+1}.csv"
                save_csv_with_correct_encoding(chunk_df, filename)
        else:
            filename = f"Historico_{timestamp}.csv"
            save_csv_with_correct_encoding(final_df, filename)
        
        print(f"\n✅ PROCESO COMPLETADO")
        print(f"📊 Registros totales: {total_records:,}")
        print("🔠 Encoding: UTF-8 con caracteres especiales corregidos")
        
    else:
        print("❌ No se obtuvieron datos")
    
    duration = time.time() - start_time
    minutes = duration / 60
    print(f"⏱️ Tiempo total: {minutes:.1f} minutos")

if __name__ == "__main__":
    os.makedirs("data", exist_ok=True)
    main()
