import requests
import zipfile
import os
import io
import logging
import sys
import shutil
from bs4 import BeautifulSoup
from datetime import datetime

# --- CONFIGURATION ---
# Define the Year-Month you want to process
ANOMES = "202510"

# Paths
# The source URL logic remains the same
URL_SOURCE = f"https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/{ANOMES[:4]}-{ANOMES[4:]}/"

# LOCAL OUTPUT DIRECTORY (Where files will be saved on your machine)
BASE_OUTPUT_DIR = os.path.abspath(f"raw")
LOG_DIR = os.path.join(BASE_OUTPUT_DIR, "logs")

# Create directories if they don't exist
os.makedirs(BASE_OUTPUT_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

# --- LOGGING SYSTEM CONFIGURATION ---
log_filename = f"data_ingestion_receita_{ANOMES}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
log_path = os.path.join(LOG_DIR, log_filename)

# Configure Logger
logger = logging.getLogger(f"ETL_LOCAL_{ANOMES}")
logger.setLevel(logging.INFO)

if logger.hasHandlers():
    logger.handlers.clear()

# 1. Screen Handler
stream_handler = logging.StreamHandler(sys.stdout)
stream_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
stream_handler.setFormatter(stream_formatter)
logger.addHandler(stream_handler)

# 2. File Handler
file_handler = logging.FileHandler(log_path)
file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(file_formatter)
logger.addHandler(file_handler)

# --- FILE MAPPING ---
FILE_TYPE_MAP = {
    "EMPRESAS": "fact_empresas",
    "ESTABELECIMENTOS": "fact_estabelecimentos",
    "SOCIOS": "fact_socios",
    # "CNAES": "dim_cnae",
    # "MOTIVOS": "dim_motivo_situacao",
    # "MUNICIPIOS": "dim_municipios",
    # "NATUREZAS": "dim_natureza_juridica",
    # "PAISES": "dim_pais",
    # "QUALIFICACOES": "dim_qualificacao_socio"
}

# --- FUNCTIONS ---

def get_file_links(url):
    logger.info(f"Starting scan at URL: {url}")
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
    
    try:
        response = requests.get(url, headers=headers, timeout=60)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        
        links = [a['href'] for a in soup.find_all('a', href=True) if a['href'].lower().endswith('.zip')]
        logger.info(f"Found {len(links)} ZIP files to process.")
        return links
    except Exception as e:
        logger.error(f"Critical error accessing source page: {e}")
        return []

def download_and_extract(filename, source_url, target_root, anomes):
    # Identify target table
    target_table = None
    for keyword, folder_name in FILE_TYPE_MAP.items():
        if keyword in filename.upper():
            target_table = folder_name
            break
    
    if not target_table:
        logger.warning(f"File ignored (out of scope): {filename}")
        return

    # Define paths
    # Structure: ./dados_receita_202512/fact_empresas/202512/
    final_folder_path = os.path.join(target_root, target_table, anomes)
    os.makedirs(final_folder_path, exist_ok=True)
    
    local_zip_path = os.path.join(final_folder_path, filename)
    download_url = source_url + filename

    logger.info(f"Starting processing: {filename} -> {target_table}")

    try:
        # Check if file already exists (skip download if valid)
        if os.path.exists(local_zip_path):
            logger.info("File already exists. Skipping download.")
        else:
            # Download
            logger.info(f"Downloading...")
            with requests.get(download_url, headers={'User-Agent': 'Mozilla/5.0'}, stream=True) as r:
                r.raise_for_status()
                with open(local_zip_path, 'wb') as f:
                    shutil.copyfileobj(r.raw, f)
                    
        # Extraction
        logger.info(f"Extracting...")
        try:
            with zipfile.ZipFile(local_zip_path, 'r') as z:
                z.extractall(final_folder_path)
            
            # Cleanup (Delete ZIP to save space)
            os.remove(local_zip_path)
            logger.info(f"SUCCESS: {filename} finished.")
        except zipfile.BadZipFile:
            logger.error(f"CORRUPTED ZIP: {filename}. Deleting file.")
            os.remove(local_zip_path)
        
    except Exception as e:
        logger.error(f"FAILURE in {filename}: {e}")

# --- MAIN EXECUTION ---

if __name__ == "__main__":
    logger.info(f"STARTING LOCAL INGESTION JOB - PERIOD {ANOMES}")
    logger.info(f"Files will be saved to: {BASE_OUTPUT_DIR}")
    logger.info(f"Logs path: {log_path}")

    files = get_file_links(URL_SOURCE)

    if not files:
        logger.error("No files found or connection error. Check your internet.")
    else:
        for file in files:
            download_and_extract(file, URL_SOURCE, BASE_OUTPUT_DIR, ANOMES)

    logger.info("JOB FINISHED SUCCESSFULLY")
