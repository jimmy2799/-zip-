#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
期貨檔案下載爬蟲 - 優化版
功能：日期過濾 + RPT 檔案選擇 + 效能優化
"""

import os
import logging
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from datetime import datetime, timedelta
import re
import requests
from bs4 import BeautifulSoup

# 優化後的配置
CONFIG = {
    'url': 'http://www.taifex.com.tw/cht/3/dlFutPrevious30DaysSalesData',
    'path': 'E:/rpt',
    'workers': 5,  # 增加並行數量
    'timeout': 30,
    'date_filter_days': 1,  # 只下載昨天及之前的檔案
    'headers': {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.54 Safari/537.36'
    },
    'retry_attempts': 3,  # 重試次數
    'retry_delay': 2  # 重試間隔（秒）
}

def setup_logging():
    """設置日誌"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('download.log', encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

logger = setup_logging()

def get_links():
    """解析網頁獲取 RPT 版本的 .zip 下載連結"""
    try:
        resp = requests.get(CONFIG['url'], headers=CONFIG['headers'], timeout=CONFIG['timeout'])
        resp.raise_for_status()
        
        # 優化：優先使用 lxml，失敗則使用 html.parser
        try:
            soup = BeautifulSoup(resp.text, 'lxml')
        except:
            soup = BeautifulSoup(resp.text, 'html.parser')
            
        # 使用列表推導式精簡代碼
        links = [
            parts[1] for tag in soup.select('input[onclick*="window.open("]')
            if len(parts := tag.get('onclick', '').split("'")) >= 2
            and parts[1].startswith('http')
            and parts[1].endswith('.zip')
            and '/DailydownloadCSV/' not in parts[1]  # 只保留 RPT 版本
        ]
        
        # 去重並反轉（讓舊的先下載）
        return list(dict.fromkeys(links))[::-1]
        
    except Exception as e:
        logger.error(f"獲取連結失敗: {e}")
        return []

def filter_links_by_date(links):
    """過濾連結，只保留指定天數內的檔案"""
    cutoff_date = datetime.now() - timedelta(days=CONFIG['date_filter_days'])
    
    def is_valid_date(link):
        filename = os.path.basename(urllib.parse.urlparse(link).path)
        date_match = re.search(r'(\d{4})(\d{2})(\d{2})', filename)
        
        if date_match:
            try:
                year, month, day = map(int, date_match.groups())
                file_date = datetime(year, month, day)
                return file_date <= cutoff_date
            except ValueError:
                return True  # 日期格式無效，保留
        return True  # 找不到日期，保留
    
    return [link for link in links if is_valid_date(link)]

def download_file_with_retry(url, session, max_retries=None):
    """帶重試機制的檔案下載"""
    if max_retries is None:
        max_retries = CONFIG['retry_attempts']
    
    filename = os.path.basename(urllib.parse.urlparse(url).path)
    filepath = Path(CONFIG['path']) / filename
    
    # 檢查檔案是否已存在
    if filepath.exists():
        return f"跳過: {filename} (檔案已存在)"
    
    for attempt in range(max_retries):
        try:
            with session.get(url, stream=True, timeout=CONFIG['timeout']) as r:
                r.raise_for_status()
                with open(filepath, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
            return f"完成: {filename}"
            
        except Exception as e:
            if attempt < max_retries - 1:
                logger.warning(f"下載失敗 {filename} (嘗試 {attempt + 1}/{max_retries}): {e}")
                time.sleep(CONFIG['retry_delay'])
            else:
                return f"失敗: {filename} ({e})"
    
    return f"失敗: {filename} (重試次數已達上限)"

def main():
    """主函數"""
    logger.info("=== 開始期貨檔案下載任務 ===")
    
    # 創建目錄
    Path(CONFIG['path']).mkdir(parents=True, exist_ok=True)
    
    # 獲取所有連結
    links = get_links()
    if not links:
        logger.warning("未找到任何下載連結")
        return

    logger.info(f"找到 {len(links)} 個連結")
    
    # 應用日期過濾
    original_count = len(links)
    links = filter_links_by_date(links)
    filtered_count = len(links)
    
    logger.info(f"過濾前: {original_count} 個連結，過濾後: {filtered_count} 個連結")
    
    if not links:
        logger.warning("過濾後無有效連結")
        return
    
    # 並行下載
    with requests.Session() as session:
        session.headers.update(CONFIG['headers'])
        with ThreadPoolExecutor(max_workers=CONFIG['workers']) as executor:
            # 提交所有下載任務
            future_to_url = {
                executor.submit(download_file_with_retry, link, session): link 
                for link in links
            }
            
            # 統計結果
            success_count = 0
            skip_count = 0
            
            for future in as_completed(future_to_url):
                result = future.result()
                logger.info(result)
                
                if "完成" in result:
                    success_count += 1
                elif "跳過" in result:
                    skip_count += 1
    
    logger.info(f"=== 任務結束 === 成功: {success_count}, 跳過: {skip_count}, 失敗: {len(links) - success_count - skip_count}")

if __name__ == "__main__":
    main()
