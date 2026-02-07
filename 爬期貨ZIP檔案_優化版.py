#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
期貨檔案下載爬蟲 - 簡化版
專注於下載與排程，移除多餘驗證邏輯。
"""

import os
import logging
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import requests
from bs4 import BeautifulSoup

# 基本配置
CONFIG = {
    'url': 'http://www.taifex.com.tw/cht/3/dlFutPrevious30DaysSalesData',
    'path': 'E:/rpt',
    'workers': 3,
    'headers': {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.54 Safari/537.36'}
}

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[logging.FileHandler('download.log', encoding='utf-8'), logging.StreamHandler()]
    )
    return logging.getLogger(__name__)

logger = setup_logging()

def get_links():
    """解析網頁獲取所有 .zip 下載連結"""
    try:
        resp = requests.get(CONFIG['url'], headers=CONFIG['headers'], timeout=30)
        resp.raise_for_status()
        
        # 嘗試使用 lxml，不行則用 html.parser
        try:
            soup = BeautifulSoup(resp.text, 'lxml')
        except:
            soup = BeautifulSoup(resp.text, 'html.parser')
            
        links = []
        for tag in soup.select('input[onclick*="window.open("]'):
            onclick = tag.get('onclick', '')
            parts = onclick.split("'")
            if len(parts) >= 2:
                url = parts[1]
                if url.startswith('http') and url.endswith('.zip'):
                    links.append(url)
        
        # 去重、反轉（讓舊的先載，或根據需求調整）
        return list(dict.fromkeys(links))[::-1]
    except Exception as e:
        logger.error(f"獲取連結失敗: {e}")
        return []

def download_file(url, session):
    """執行單個檔案下載"""
    filename = os.path.basename(urllib.parse.urlparse(url).path)
    filepath = Path(CONFIG['path']) / filename
    
    if filepath.exists():
        return f"跳過: {filename}"
        
    try:
        with session.get(url, stream=True, timeout=30) as r:
            r.raise_for_status()
            with open(filepath, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        return f"完成: {filename}"
    except Exception as e:
        return f"失敗: {filename} ({e})"

def main():
    logger.info("=== 開始期貨檔案下載任務 ===")
    Path(CONFIG['path']).mkdir(parents=True, exist_ok=True)
    
    links = get_links()
    if not links:
        logger.warning("未找到任何下載連結")
        return

    logger.info(f"預計檢查 {len(links)} 個連結")
    
    with requests.Session() as session:
        session.headers.update(CONFIG['headers'])
        with ThreadPoolExecutor(max_workers=CONFIG['workers']) as executor:
            futures = [executor.submit(download_file, link, session) for link in links]
            for future in as_completed(futures):
                logger.info(future.result())

    logger.info("=== 任務結束 ===")

if __name__ == "__main__":
    main()
