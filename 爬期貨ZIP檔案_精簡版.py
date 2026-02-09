#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, logging, time, re, requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from datetime import datetime, timedelta
from bs4 import BeautifulSoup

# 簡化配置
CONFIG = {
    'url': 'http://www.taifex.com.tw/cht/3/dlFutPrevious30DaysSalesData',
    'path': 'E:/rpt',
    'workers': 5,
    'timeout': 30,
    'headers': {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
}

# 設置日誌
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger()

def get_last_date():
    """讀取上次下載日期"""
    try:
        with open(f"{CONFIG['path']}/last_downloaded_date.txt") as f:
            return datetime.strptime(f.read().strip(), '%Y-%m-%d')
    except: return None

def save_last_date(date):
    """保存下載日期"""
    try:
        with open(f"{CONFIG['path']}/last_downloaded_date.txt", 'w') as f:
            f.write(date.strftime('%Y-%m-%d'))
    except: pass

def get_links():
    """獲取下載連結"""
    try:
        resp = requests.get(CONFIG['url'], headers=CONFIG['headers'], timeout=CONFIG['timeout'])
        # 優先使用 lxml，失敗則使用 html.parser
        try:
            soup = BeautifulSoup(resp.text, 'lxml')
        except:
            soup = BeautifulSoup(resp.text, 'html.parser')
        return [parts[1] for tag in soup.select('input[onclick*="window.open("]')
                if len(parts := tag.get('onclick', '').split("'")) >= 2
                and parts[1].startswith('http') and parts[1].endswith('.zip')
                and '/DailydownloadCSV/' not in parts[1]][::-1]
    except Exception as e:
        logger.error(f"獲取連結失敗: {e}")
        return []

def filter_links(links):
    """過濾連結"""
    if not links: return []
    
    # 找出最新日期（根據檔案名稱）
    max_date = max((datetime(*map(int, re.search(r'(\d{4})(\d{2})(\d{2})', 
        os.path.basename(link)).groups())) for link in links if re.search(r'(\d{4})(\d{2})(\d{2})', 
        os.path.basename(link))), default=None)
    
    if not max_date: return links
    
    # 檢查是否需要下載
    last_date = get_last_date()
    if last_date and max_date <= last_date:
        logger.info(f"無新檔案，跳過下載")
        return []
    
    # 過濾檔案：只保留比最新日期早2天以上的檔案
    cutoff_date = max_date - timedelta(days=2)
    filtered = [link for link in links if datetime(*map(int, re.search(r'(\d{4})(\d{2})(\d{2})', 
        os.path.basename(link)).groups())) <= cutoff_date]
    
    logger.info(f"找到最新日期: {max_date.strftime('%Y-%m-%d')}, 截止日期: {cutoff_date.strftime('%Y-%m-%d')}")
    return filtered

def download_file(url, session):
    """下載檔案"""
    filename = os.path.basename(url)
    filepath = Path(CONFIG['path']) / filename
    
    if filepath.exists():
        return f"跳過: {filename}"
    
    for _ in range(3):
        try:
            with session.get(url, stream=True, timeout=CONFIG['timeout']) as r:
                r.raise_for_status()
                with open(filepath, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
            return f"完成: {filename}"
        except Exception as e:
            time.sleep(2)
    return f"失敗: {filename}"

def main():
    """主函數"""
    logger.info("=== 開始下載任務 ===")
    Path(CONFIG['path']).mkdir(parents=True, exist_ok=True)
    
    links = filter_links(get_links())
    if not links:
        logger.info("無需下載")
        return
    
    with requests.Session() as session:
        session.headers.update(CONFIG['headers'])
        with ThreadPoolExecutor(max_workers=CONFIG['workers']) as executor:
            results = [executor.submit(download_file, link, session) for link in links]
            success = sum(1 for r in as_completed(results) if "完成" in r.result())
    
    if success > 0:
        max_date = max((datetime(*map(int, re.search(r'(\d{4})(\d{2})(\d{2})', 
            os.path.basename(link)).groups())) for link in links))
        save_last_date(max_date)
        logger.info(f"保存日期: {max_date.strftime('%Y-%m-%d')}")
    
    logger.info(f"完成: 成功{success}, 跳過{len(links)-success}")

if __name__ == "__main__":
    main()