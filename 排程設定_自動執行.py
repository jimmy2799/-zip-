#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Windows任務排程器自動設定腳本
用於自動設定期貨檔案下載器的定時執行
"""

import os
import subprocess
import sys
from datetime import datetime, time
from pathlib import Path


class SchedulerSetup:
    """排程設定管理器"""
    
    def __init__(self):
        self.python_exe = sys.executable
        self.script_path = Path(__file__).parent / "爬期貨ZIP檔案_優化版.py"
        self.task_name = "期貨檔案自動下載"
        
    def check_requirements(self):
        """檢查必要條件"""
        print("=== 檢查排程設定條件 ===")
        
        # 檢查Python路徑
        if not self.python_exe:
            print("❌ 找不到Python執行檔")
            return False
        print(f"✅ Python路徑: {self.python_exe}")
        
        # 檢查腳本文件
        if not self.script_path.exists():
            print(f"❌ 找不到腳本文件: {self.script_path}")
            return False
        print(f"✅ 腳本文件: {self.script_path}")
        
        # 檢查是否為管理員權限
        try:
            import ctypes
            is_admin = ctypes.windll.shell32.IsUserAnAdmin()
            if not is_admin:
                print("⚠️  建議以管理員身份執行此腳本以獲得最佳效果")
            else:
                print("✅ 以管理員身份運行")
        except:
            print("⚠️  無法檢測管理員權限")
            
        return True
        
    def create_sunday_task(self, run_time="22:00"):
        """創建週日執行任務"""
        print(f"\n=== 設定每周日自動執行任務 ===")
        print(f"任務名稱: {self.task_name}_週日版")
        print(f"執行時間: 每週日 {run_time}")
        print(f"Python路徑: {self.python_exe}")
        print(f"腳本路徑: {self.script_path}")
        
        # 構建schtasks命令
        task_name = f"{self.task_name}_週日版"
        command = [
            "schtasks", "/create",
            "/tn", task_name,
            "/tr", f'"{self.python_exe}" "{self.script_path}"',
            "/sc", "weekly",
            "/d", "SUN",
            "/st", run_time,
            "/ru", "SYSTEM",
            "/f"
        ]
        
        try:
            result = subprocess.run(command, capture_output=True, text=True, check=True)
            print("✅ 週日任務排程設定成功!")
            print(f"輸出: {result.stdout}")
            return True
        except subprocess.CalledProcessError as e:
            print(f"❌ 週日排程設定失敗: {e}")
            print(f"錯誤輸出: {e.stderr}")
            return False
            
    def create_saturday_task(self, run_time="10:00"):
        """創建週六執行任務"""
        print(f"\n=== 設定週六自動執行任務 ===")
        print(f"任務名稱: {self.task_name}_週六版")
        print(f"執行時間: 每週六 {run_time}")
        
        task_name = f"{self.task_name}_週六版"
        command = [
            "schtasks", "/create",
            "/tn", task_name,
            "/tr", f'"{self.python_exe}" "{self.script_path}"',
            "/sc", "weekly",
            "/d", "SAT",
            "/st", run_time,
            "/ru", "SYSTEM",
            "/f"
        ]
        
        try:
            result = subprocess.run(command, capture_output=True, text=True, check=True)
            print("✅ 工作日任務排程設定成功!")
            return True
        except subprocess.CalledProcessError as e:
            print(f"❌ 工作日排程設定失敗: {e}")
            return False
            
    def create_custom_task(self, frequency="daily", run_time="09:00", days=None):
        """創建自定義排程任務"""
        print(f"\n=== 設定自定義排程任務 ===")
        print(f"頻率: {frequency}")
        print(f"時間: {run_time}")
        
        task_name = f"{self.task_name}_自定義版"
        command = [
            "schtasks", "/create",
            "/tn", task_name,
            "/tr", f'"{self.python_exe}" "{self.script_path}"',
            "/sc", frequency,
            "/st", run_time,
            "/ru", "SYSTEM",
            "/f"
        ]
        
        # 添加天數參數（如果是weekly）
        if frequency == "weekly" and days:
            command.extend(["/d", days])
            
        try:
            result = subprocess.run(command, capture_output=True, text=True, check=True)
            print("✅ 自定義任務排程設定成功!")
            return True
        except subprocess.CalledProcessError as e:
            print(f"❌ 自定義排程設定失敗: {e}")
            return False
            
    def list_existing_tasks(self):
        """列出已存在的相關任務"""
        print("\n=== 檢查現有排程任務 ===")
        
        try:
            result = subprocess.run(
                ["schtasks", "/query", "/fo", "LIST", "/v"],
                capture_output=True, text=True, check=True
            )
            
            lines = result.stdout.split('\n')
            relevant_tasks = []
            
            for i, line in enumerate(lines):
                if self.task_name in line and "TaskName:" in line:
                    # 找到任務名稱，提取相關信息
                    task_info = {}
                    task_info['name'] = line.split(':', 1)[1].strip()
                    
                    # 查找狀態
                    for j in range(i, min(i+20, len(lines))):
                        if "Status:" in lines[j]:
                            task_info['status'] = lines[j].split(':', 1)[1].strip()
                        elif "Next Run Time:" in lines[j]:
                            task_info['next_run'] = lines[j].split(':', 1)[1].strip()
                        elif "Last Run Time:" in lines[j]:
                            task_info['last_run'] = lines[j].split(':', 1)[1].strip()
                            
                    relevant_tasks.append(task_info)
                    
            if relevant_tasks:
                print("找到以下相關任務:")
                for task in relevant_tasks:
                    print(f"  - {task['name']}")
                    print(f"    狀態: {task['status']}")
                    if 'next_run' in task:
                        print(f"    下次執行: {task['next_run']}")
                    if 'last_run' in task:
                        print(f"    上次執行: {task['last_run']}")
                    print()
            else:
                print("未找到相關的排程任務")
                
        except subprocess.CalledProcessError as e:
            print(f"❌ 查詢排程任務失敗: {e}")
            
    def delete_task(self, task_name=None):
        """刪除排程任務"""
        if not task_name:
            task_name = self.task_name
            
        print(f"\n=== 刪除排程任務: {task_name} ===")
        
        try:
            result = subprocess.run(
                ["schtasks", "/delete", "/tn", task_name, "/f"],
                capture_output=True, text=True, check=True
            )
            print("✅ 任務刪除成功!")
            return True
        except subprocess.CalledProcessError as e:
            print(f"❌ 刪除任務失敗: {e}")
            return False
            
    def run_manual_test(self):
        """手動測試執行"""
        print("\n=== 手動測試執行 ===")
        print(f"即將執行: {self.script_path}")
        
        try:
            result = subprocess.run([self.python_exe, str(self.script_path)], 
                                  capture_output=True, text=True, timeout=300)
            
            print("執行輸出:")
            print(result.stdout)
            
            if result.stderr:
                print("錯誤輸出:")
                print(result.stderr)
                
            print(f"執行結果: {'成功' if result.returncode == 0 else '失敗'}")
            return result.returncode == 0
            
        except subprocess.TimeoutExpired:
            print("❌ 執行超時")
            return False
        except Exception as e:
            print(f"❌ 執行失敗: {e}")
            return False
            
    def create_batch_file(self):
        """創建批次文件以便手動執行"""
        batch_content = f'''@echo off
echo 開始執行期貨檔案下載...
echo Python路徑: {self.python_exe}
echo 腳本路徑: {self.script_path}
echo.

"{self.python_exe}" "{self.script_path}"

echo.
echo 按任意鍵退出...
pause >nul
'''
        
        batch_file = Path(__file__).parent / "執行期貨下載.bat"
        try:
            with open(batch_file, 'w', encoding='utf-8') as f:
                f.write(batch_content)
            print(f"✅ 批次文件已創建: {batch_file}")
            return True
        except Exception as e:
            print(f"❌ 創建批次文件失敗: {e}")
            return False
            
    def show_help(self):
        """顯示使用說明"""
        help_text = """
=== 期貨檔案下載排程設定說明 ===

功能說明:
1. 自動設定Windows任務排程器，定時執行期貨檔案下載
2. 支援多種排程模式: 週日、工作日、自定義
3. 提供任務管理功能: 查詢、刪除、測試

使用方法:
1. 週日執行: 每周日固定時間自動下載（推薦: 22:00）
2. 工作日執行: 週一至週五自動下載（推薦: 10:00）
3. 自定義排程: 可設定不同頻率和時間

注意事項:
- 建議設定在交易日的收盤後執行
- 週日執行適合在週末整理一週的資料
- 需要確保E:/rpt目錄有寫入權限
- 可以通過Windows任務排程器手動管理任務

管理命令:
- 查詢任務: schtasks /query /tn "期貨檔案自動下載"
- 手動執行: schtasks /run /tn "期貨檔案自動下載"
- 刪除任務: schtasks /delete /tn "期貨檔案自動下載"
"""
        print(help_text)
        
    def interactive_setup(self):
        """互動式設定"""
        print("=== 期貨檔案下載排程設定向導 ===")
        
        if not self.check_requirements():
            return False
            
        self.show_help()
        
        while True:
            print("\n=== 選擇操作 ===")
            print("1. 設定每周日自動執行 (22:00)")
            print("2. 設定工作日自動執行 (週一至週五)")
            print("3. 自定義排程設定")
            print("4. 查詢現有任務")
            print("5. 刪除任務")
            print("6. 手動測試執行")
            print("7. 創建批次文件")
            print("0. 離開")
            
            choice = input("\n請選擇 (0-7): ").strip()
            
            if choice == "1":
                time_input = input("設定執行時間 (格式: HH:MM, 預設: 22:00): ").strip()
                if not time_input:
                    time_input = "22:00"
                self.create_sunday_task(time_input)
                
            elif choice == "2":
                time_input = input("設定執行時間 (格式: HH:MM, 預設: 10:00): ").strip()
                if not time_input:
                    time_input = "10:00"
                self.create_saturday_task(time_input)
                
            elif choice == "3":
                print("可選頻率: daily, weekly, monthly, once")
                freq = input("設定頻率: ").strip()
                time_input = input("設定執行時間 (格式: HH:MM): ").strip()
                if freq == "weekly":
                    days = input("設定星期幾 (MON,TUE等, 逗號分隔): ").strip()
                    self.create_custom_task(freq, time_input, days)
                else:
                    self.create_custom_task(freq, time_input)
                    
            elif choice == "4":
                self.list_existing_tasks()
                
            elif choice == "5":
                task_name = input("輸入要刪除的任務名稱 (留空使用預設): ").strip()
                if not task_name:
                    task_name = None
                self.delete_task(task_name)
                
            elif choice == "6":
                self.run_manual_test()
                
            elif choice == "7":
                self.create_batch_file()
                
            elif choice == "0":
                print("感謝使用，再見!")
                break
                
            else:
                print("無效選擇，請重新輸入")


def main():
    """主函數"""
    scheduler = SchedulerSetup()
    scheduler.interactive_setup()


if __name__ == "__main__":
    main()