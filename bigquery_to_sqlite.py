"""
BigQuery 資料轉換為 SQLite 的程式
從 BigQuery 的 daily_kbars 資料表讀取資料並轉換為 SQLite 格式
"""

import sqlite3
from google.cloud import bigquery
from google.oauth2 import service_account
import os
from typing import Optional


def create_sqlite_table(cursor: sqlite3.Cursor):
    """建立 SQLite 資料表結構"""
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS daily_kbars (
        ts TIMESTAMP NOT NULL,
        stock_code TEXT NOT NULL,
        Open REAL NOT NULL,
        High REAL NOT NULL,
        Low REAL NOT NULL,
        Close REAL NOT NULL,
        Volume INTEGER NOT NULL,
        PRIMARY KEY (ts, stock_code)
    )
    """
    cursor.execute(create_table_sql)
    print("SQLite 資料表已建立或已存在")


def convert_bigquery_to_sqlite(
    project_id: str,
    dataset_id: str,
    table_id: str,
    sqlite_db_path: str,
    credentials_path: Optional[str] = None,
    limit: Optional[int] = None
):
    """
    從 BigQuery 讀取資料並轉換為 SQLite 格式
    
    Args:
        project_id: BigQuery 專案 ID
        dataset_id: BigQuery 資料集 ID
        table_id: BigQuery 資料表 ID
        sqlite_db_path: SQLite 資料庫檔案路徑
        credentials_path: Google Cloud 認證檔案路徑（可選，如果未提供則使用預設認證）
        limit: 限制讀取的資料筆數（可選，用於測試）
    """
    # 初始化 BigQuery 客戶端
    if credentials_path and os.path.exists(credentials_path):
        credentials = service_account.Credentials.from_service_account_file(
            credentials_path
        )
        client = bigquery.Client(credentials=credentials, project=project_id)
        print(f"使用認證檔案: {credentials_path}")
    else:
        # 使用預設認證（環境變數 GOOGLE_APPLICATION_CREDENTIALS）
        client = bigquery.Client(project=project_id)
        print("使用預設認證")
    
    # 建立 SQLite 連線
    conn = sqlite3.connect(sqlite_db_path)
    cursor = conn.cursor()
    
    try:
        # 建立資料表
        create_sqlite_table(cursor)
        
        # 建構查詢語句
        query = f"""
        SELECT 
            ts,
            stock_code,
            Open,
            High,
            Low,
            Close,
            Volume
        FROM `{project_id}.{dataset_id}.{table_id}`
        ORDER BY ts, stock_code
        """
        
        if limit:
            query += f" LIMIT {limit}"
        
        print(f"執行 BigQuery 查詢...")
        print(f"查詢語句: {query[:200]}...")
        
        # 執行 BigQuery 查詢
        query_job = client.query(query)
        results = query_job.result()
        
        # 準備插入 SQLite 的語句
        insert_sql = """
        INSERT OR REPLACE INTO daily_kbars 
        (ts, stock_code, Open, High, Low, Close, Volume)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """
        
        # 批次插入資料
        batch_size = 1000
        batch = []
        total_rows = 0
        
        print("開始轉換資料...")
        for row in results:
            # 轉換 BigQuery TIMESTAMP 為字串格式（SQLite 支援 ISO 8601 格式）
            ts_str = row.ts.isoformat() if hasattr(row.ts, 'isoformat') else str(row.ts)
            
            batch.append((
                ts_str,
                row.stock_code,
                float(row.Open),
                float(row.High),
                float(row.Low),
                float(row.Close),
                int(row.Volume)
            ))
            
            if len(batch) >= batch_size:
                cursor.executemany(insert_sql, batch)
                total_rows += len(batch)
                print(f"已處理 {total_rows} 筆資料...")
                batch = []
        
        # 插入剩餘的資料
        if batch:
            cursor.executemany(insert_sql, batch)
            total_rows += len(batch)
        
        # 提交交易
        conn.commit()
        print(f"轉換完成！總共處理 {total_rows} 筆資料")
        print(f"SQLite 資料庫已儲存至: {sqlite_db_path}")
        
    except Exception as e:
        conn.rollback()
        print(f"發生錯誤: {str(e)}")
        raise
    finally:
        cursor.close()
        conn.close()


def main():
    """主程式"""
    # 設定參數
    PROJECT_ID = "life-is-a-vacation"
    DATASET_ID = "stock_data"
    TABLE_ID = "daily_kbars"
    SQLITE_DB_PATH = "daily_kbars.db"
    
    # 可選：指定認證檔案路徑
    # CREDENTIALS_PATH = "path/to/your/credentials.json"
    CREDENTIALS_PATH = None  # 使用預設認證
    
    # 可選：限制讀取筆數（用於測試，設為 None 則讀取全部）
    LIMIT = None
    
    try:
        convert_bigquery_to_sqlite(
            project_id=PROJECT_ID,
            dataset_id=DATASET_ID,
            table_id=TABLE_ID,
            sqlite_db_path=SQLITE_DB_PATH,
            credentials_path=CREDENTIALS_PATH,
            limit=LIMIT
        )
    except Exception as e:
        print(f"程式執行失敗: {str(e)}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())

