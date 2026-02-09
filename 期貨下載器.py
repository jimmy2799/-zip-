#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""期貨ZIP檔案下載器 - 精簡版"""

import os
import re
import sys
import time
import logging
import requests
import subprocess
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup

# ========== 設定 ==========
CONFIG = {
    'url': 'http://www.taifex.com.tw/cht/3/dlFutPrevious30DaysSalesData',
    'path': 'E:/rpt',
    'workers': 5,
    'timeout': 30,
    'headers': {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
}

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger()

# ========== 下載核心 ==========
def get_links():
    """獲取所有ZIP下載連結"""
    try:
        resp = requests.get(CONFIG['url'], headers=CONFIG['headers'], timeout=CONFIG['timeout'])
        soup = BeautifulSoup(resp.text, 'lxml')
        links = []
        for tag in soup.select('input[onclick*="window.open("]'):
            onclick = tag.get('onclick', '')
            parts = onclick.split("'")
            if len(parts) >= 2 and parts[1].startswith('http') and parts[1].endswith('.zip') and '/DailydownloadCSV/' not in parts[1]:
                links.append(parts[1])
        return links[::-1]
    except Exception as e:
        logger.error(f"獲取連結失敗: {e}")
        return []

def download_file(url, session):
    """下載單一檔案"""
    filename = os.path.basename(url)
    filepath = Path(CONFIG['path']) / filename
    
    if filepath.exists():
        return f"已存在: {filename}"
    
    for attempt in range(3):
        try:
            with session.get(url, stream=True, timeout=CONFIG['timeout']) as r:
                r.raise_for_status()
                with open(filepath, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
            return f"完成: {filename}"
        except Exception as e:
            if attempt < 2:
                time.sleep(2)
            else:
                return f"失敗: {filename} ({str(e)[:30]})"
    return f"失敗: {filename}"

def run_download():
    """執行下載任務"""
    logger.info("=== 開始下載期貨檔案 ===")
    Path(CONFIG['path']).mkdir(parents=True, exist_ok=True)
    
    links = get_links()
    if not links:
        logger.info("無檔案可下載")
        return
    
    # 找出最新日期並排除
    dates = []
    for link in links:
        match = re.search(r'(\d{4})(\d{2})(\d{2})', os.path.basename(link))
        if match:
            dates.append(datetime(int(match.group(1)), int(match.group(2)), int(match.group(3))))
    
    if dates:
        max_date = max(dates)
        logger.info(f"最新檔案日期: {max_date.strftime('%Y-%m-%d')} (將跳過)")
    
    # 下載（排除最新檔案）
    with requests.Session() as session:
        session.headers.update(CONFIG['headers'])
        with ThreadPoolExecutor(max_workers=CONFIG['workers']) as executor:
            futures = {}
            for link in links:
                filename = os.path.basename(link)
                match = re.search(r'(\d{4})(\d{2})(\d{2})', filename)
                if match and dates and datetime(int(match.group(1)), int(match.group(2)), int(match.group(3))) == max(dates):
                    logger.info(f"跳過最新: {filename}")
                    continue
                futures[executor.submit(download_file, link, session)] = filename
            
            completed = 0
            failed = 0
            skipped = 0
            for future in as_completed(futures):
                result = future.result()
                if "完成" in result:
                    completed += 1
                elif "已存在" in result:
                    skipped += 1
                else:
                    failed += 1
                logger.info(result)
    
    logger.info(f"=== 完成: 成功 {completed}, 已存在 {skipped}, 失敗 {failed} ===")

# ========== 排程功能 ==========
def setup_schedule():
    """設定自動排程"""
    print("\n=== 設定自動排程 ===")
    print("1. 每周日 22:00")
    print("2. 工作日 10:00")
    print("3. 每日 09:00")
    print("0. 取消")
    
    choice = input("\n選擇 (0-3): ").strip()
    
    task_name = "期貨檔案下載"
    python_exe = sys.executable
    script_path = Path(__file__).resolve()
    
    if choice == "1":
        cmd = f'schtasks /create /tn "{task_name}" /tr "\\"{python_exe}\" \\"{script_path}\" download" /sc weekly /d SUN /st 22:00 /f'
    elif choice == "2":
        cmd = f'schtasks /create /tn "{task_name}" /tr "\\"{python_exe}\" \\"{script_path}\" download" /sc weekly /d MON,TUE,WED,THU,FRI /st 10:00 /f'
    elif choice == "3":
        cmd = f'schtasks /create /tn "{task_name}" /tr "\\"{python_exe}\" \\"{script_path}\" download" /sc daily /st 09:00 /f'
    else:
        print("取消設定")
        return
    
    try:
        subprocess.run(cmd, shell=True, check=True, capture_output=True)
        print(f"✅ 排程設定成功!")
    except subprocess.CalledProcessError as e:
        print(f"❌ 設定失敗: {e}")
        print("提示: 可能需要以系統管理員身份執行")

def remove_schedule():
    """移除排程"""
    try:
        subprocess.run('schtasks /delete /tn "期貨檔案下載" /f', shell=True, check=True, capture_output=True)
        print("✅ 排程已移除")
    except:
        print("❌ 移除失敗或不存在")

def check_schedule():
    """查看排程狀態"""
    try:
        result = subprocess.run('schtasks /query /tn "期貨檔案下載" /fo LIST', shell=True, capture_output=True, text=True)
        print(result.stdout if result.returncode == 0 else "找不到排程任務")
    except:
        print("查詢失敗")

# ========== 主程式 ==========
def main():
    if len(sys.argv) > 1 and sys.argv[1] == "download":
        run_download()
        return
    
    while True:
        print("\n" + "="*40)
        print("期貨ZIP檔案下載器")
        print("="*40)
        print("1. 立即下載")
        print("2. 設定自動排程")
        print("3. 查看排程")
        print("4. 移除排程")
        print("0. 離開")
        print("="*40)
        
        choice = input("選擇: ").strip()
        
        if choice == "1":
            run_download()
        elif choice == "2":
            setup_schedule()
        elif choice == "3":
            check_schedule()
        elif choice == "4":
            remove_schedule()
        elif choice == "0":
            break
        else:
            print("無效選擇")

if __name__ == "__main__":
    main()
