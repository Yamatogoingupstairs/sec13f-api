import os
import re
import time
import shutil
import magic
import requests
import pandas as pd
from zipfile import ZipFile, BadZipFile
from bs4 import BeautifulSoup
from argparse import ArgumentParser
from datetime import datetime

BASE_FOLDER = 'sec_13f_data'
ANNUAL_FOLDER = 'sec_13f_annual'
os.makedirs(BASE_FOLDER, exist_ok=True)
os.makedirs(ANNUAL_FOLDER, exist_ok=True)

def get_zip_links(start_year, end_year):
    url = "https://www.sec.gov/data-research/sec-markets-data/form-13f-data-sets"
    headers = {
        "User-Agent": "YourAppName/1.0 (your@email.com)",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.sec.gov/",
    }

    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        zip_links = []

        for year in range(start_year, end_year + 1):
            for link in soup.find_all('a', href=True):
                href = link['href']
                if href.endswith('.zip') and str(year) in href:
                    zip_links.append(f"https://www.sec.gov{href}")

        if not zip_links:
            raise Exception("❌ 未找到任何 ZIP 下載連結")
        return zip_links

    except requests.RequestException as e:
        raise Exception(f"❌ 無法獲取 SEC 下載連結: {e}")

def is_valid_zip(file_path):
    mime_type = magic.Magic(mime=True).from_file(file_path)
    return mime_type == 'application/zip'

def download_zip(zip_url, output_folder):
    os.makedirs(output_folder, exist_ok=True)
    local_zip = os.path.join(output_folder, os.path.basename(zip_url))
    headers = {
        "User-Agent": "YourAppName/1.0 (your@email.com)"
    }
    response = requests.get(zip_url, headers=headers, stream=True)
    response.raise_for_status()

    with open(local_zip, 'wb') as f:
        for chunk in response.iter_content(chunk_size=1024):
            if chunk:
                f.write(chunk)

    if not is_valid_zip(local_zip):
        os.remove(local_zip)
        raise Exception("❌ 下載檔案不是合法的 ZIP")

    time.sleep(2)
    return local_zip

def extract_zip(zip_path, extract_to):
    try:
        with ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_to)
    except BadZipFile:
        raise Exception("❌ ZIP 壓縮檔損毀")

def process_tsv_files(folder, output_csv):
    infotable_df = None
    for file in os.listdir(folder):
        if file.endswith('.tsv'):
            tsv_path = os.path.join(folder, file)
            try:
                df = pd.read_csv(tsv_path, sep='\t', encoding='utf-8', engine='python')
                if 'INFOTABLE' in file:
                    infotable_df = df.copy()
            except Exception as e:
                print(f"[ERROR] 讀取 {file} 失敗: {e}")

    if infotable_df is None:
        raise ValueError("❌ 找不到 INFOTABLE 檔案")

    if 'ACCESSION_NUMBER' in infotable_df.columns:
        infotable_df['CIK'] = infotable_df['ACCESSION_NUMBER'].str.split('-').str[0]

    for file in os.listdir(folder):
        if file.endswith('.tsv') and 'INFOTABLE' not in file:
            tsv_path = os.path.join(folder, file)
            try:
                df = pd.read_csv(tsv_path, sep='\t', encoding='utf-8', engine='python')
                if 'ACCESSION_NUMBER' in df.columns:
                    infotable_df = pd.merge(infotable_df, df, on='ACCESSION_NUMBER', how='left')
            except Exception as e:
                print(f"[ERROR] 合併 {file} 失敗: {e}")

    subset_columns = [c for c in ['CIK', 'NAMEOFISSUER', 'CUSIP', 'VALUE'] if c in infotable_df.columns]
    infotable_df.drop_duplicates(subset=subset_columns, inplace=True)

    columns_to_keep = {
        'CIK': 'cik', 'NAMEOFISSUER': 'nameOfIssuer',
        'CUSIP': 'cusip', 'VALUE': 'value', 'PERIODOFREPORT': 'rdate'
    }
    final_df = infotable_df[[c for c in columns_to_keep if c in infotable_df.columns]].copy()
    final_df.rename(columns=columns_to_keep, inplace=True)
    final_df.to_csv(output_csv, index=False)
    print(f"✅ 已儲存 CSV: {output_csv}")
    shutil.rmtree(folder)

def aggregate_csv_by_year():
    all_files = [os.path.join(BASE_FOLDER, f) for f in os.listdir(BASE_FOLDER) if f.endswith('.csv')]
    annual_data = {}
    for file in all_files:
        df = pd.read_csv(file)
        if 'rdate' in df.columns:
            df['year'] = pd.to_datetime(df['rdate'], errors='coerce').dt.year
            for year, group in df.groupby('year'):
                if not pd.isna(year):
                    annual_data[year] = pd.concat([annual_data.get(year, pd.DataFrame()), group])
    for year, df in annual_data.items():
        output = os.path.join(ANNUAL_FOLDER, f"13F_{year}.csv")
        df.to_csv(output, index=False)
        print(f"✅ 年度彙總檔案已儲存：{output}")

def main():
    parser = ArgumentParser()
    parser.add_argument('--start_year', type=int, required=True)
    parser.add_argument('--end_year', type=int, required=True)
    args = parser.parse_args()

    zip_links = get_zip_links(args.start_year, args.end_year)

    for zip_link in zip_links:
        zip_folder = os.path.join(BASE_FOLDER, os.path.basename(zip_link).replace('.zip', ''))
        zip_path = download_zip(zip_link, zip_folder)
        extract_zip(zip_path, zip_folder)
        csv_output = os.path.join(BASE_FOLDER, f"{os.path.basename(zip_link).replace('.zip', '')}.csv")
        process_tsv_files(zip_folder, csv_output)

    aggregate_csv_by_year()

if __name__ == "__main__":
    main()
