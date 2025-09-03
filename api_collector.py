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

# ... (código anterior igual)

def fetch_data_page(url, name, page_number):
    print(f"📄 Consultando página {page_number} para {name}")
    try:
        response = requests.get(url, headers=HEADERS, timeout=60)
        response.raise_for_status()
        
        # Guardar respuesta cruda para debug
        raw_response = response.text
        print(f"📄 Respuesta cruda (primeros 500 chars): {raw_response[:500]}...")
        
        data = response.json()
        
        # Debug detallado
        print(f"🔍 Tipo de respuesta: {type(data)}")
        if isinstance(data, dict):
            print(f"🔍 Keys del objeto: {list(data.keys())}")
            for key in data.keys():
                if isinstance(data[key], (list, dict)):
                    print(f"🔍 Tipo de '{key}': {type(data[key])}")
                    if isinstance(data[key], list):
                        print(f"🔍 Tamaño de '{key}': {len(data[key])}")
        
        # Procesar la respuesta
        records = []
        
        if isinstance(data, list):
            records = data
        elif isinstance(data, dict):
            # Buscar cualquier clave que contenga un array
            for key, value in data.items():
                if isinstance(value, list):
                    records = value
                    break
            # Si no encontramos array, usar el dict completo como único registro
            if not records:
                records = [data]
        else:
            print(f"⚠️  Tipo de respuesta no reconocido: {type(data)}")
            return None, False
            
        # Crear DataFrame
        df = pd.DataFrame(records)
        
        if df.empty:
            print(f"⚠️  Página {page_number} devolvió DataFrame vacío")
            return None, False
            
        df["load_timestamp"] = datetime.now().isoformat()
        df["page_number"] = page_number
        
        has_more_pages = len(df) == PAGE_SIZE
        return df, has_more_pages
        
    except Exception as e:
        print(f"⚠️  Error en página {page_number}: {e}")
        return None, False
