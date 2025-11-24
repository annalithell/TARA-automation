"""
Reads and explores the SQLite database AAD (Automotive Attack Database) V3.0  
"""

import sqlite3
import pandas as pd
from pathlib import Path
from datetime import datetime

# Database path
DB_PATH = Path(__file__).parent.parent / "datasets" / "AAD" / "Automotive_Attack_Database_(AAD)_V3.0.db"

# Log file path
LOG_DIR = Path(__file__).parent.parent / "logs"
LOG_DIR.mkdir(exist_ok=True)
LOG_FILE = LOG_DIR / f"AAD_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

# Custom logger function
def log_output(message="", print_to_console=False):
    """Write output to log file and optionally to console"""
    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(message + "\n")
    if print_to_console:
        print(message)


def inspect_database():
    """Inspect the database structure"""
    log_output("=" * 60)
    log_output("INSPECTING DATABASE STRUCTURE")
    log_output("=" * 60)
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Get all tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    
    log_output(f"\nFound {len(tables)} table(s):\n")
    
    for table in tables:
        table_name = table[0]
        log_output(f"Table: {table_name}")
        log_output("-" * 40)
        
        # Get column info
        cursor.execute(f'PRAGMA table_info("{table_name}")')
        columns = cursor.fetchall()
        
        for col in columns:
            col_id, col_name, col_type, not_null, default, pk = col
            log_output(f"  {col_name:<30} {col_type:<10}")
        
        # Get row count
        cursor.execute(f'SELECT COUNT(*) FROM "{table_name}"')
        row_count = cursor.fetchone()[0]
        log_output(f"\n  Total rows: {row_count}\n")
    
    conn.close()


def load_data():
    """Load data from the database into pandas DataFrames"""
    log_output("=" * 60)
    log_output("LOADING DATA INTO PANDAS")
    log_output("=" * 60)
    
    conn = sqlite3.connect(DB_PATH)
    
    # Get table names
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [table[0] for table in cursor.fetchall()]
    
    dataframes = {}
    
    for table_name in tables:
        df = pd.read_sql_query(f'SELECT * FROM "{table_name}"', conn)
        dataframes[table_name] = df
        log_output(f"\nTable: {table_name}")
        log_output(f"Shape: {df.shape}")
        log_output(f"Columns: {list(df.columns)}\n")
        log_output(df.head(3).to_string())
        log_output()
    
    conn.close()
    return dataframes


def example_queries():
    """Run some example queries"""
    log_output("=" * 60)
    log_output("EXAMPLE QUERIES")
    log_output("=" * 60)
    
    conn = sqlite3.connect(DB_PATH)
    
    try:
        # Example query 1: Get all data from main table
        df = pd.read_sql_query('SELECT * FROM "Automotive Security Attacks" LIMIT 5', conn)
        log_output("\nFirst 5 attacks:")
        log_output(df.to_string())
        
        # Example query 2: Attacks by year
        log_output("\n" + "=" * 60)
        log_output("Attacks by Year:")
        log_output("=" * 60)
        df_by_year = pd.read_sql_query(
            'SELECT Year, COUNT(*) as Count FROM "Automotive Security Attacks" GROUP BY Year ORDER BY Year',
            conn
        )
        # Clean up the Year field by extracting just the year number
        df_by_year['Year'] = df_by_year['Year'].str.extract(r'(\d{4})', expand=False)
        df_by_year_grouped = df_by_year.groupby('Year', as_index=False)['Count'].sum().sort_values('Year')
        log_output(df_by_year_grouped.to_string(index=False))
        
        # Example query 3: Attack types
        log_output("\n" + "=" * 60)
        log_output("Top 10 Attack Types:")
        log_output("=" * 60)
        df_types = pd.read_sql_query(
            'SELECT "Attack Type", COUNT(*) as Count FROM "Automotive Security Attacks" GROUP BY "Attack Type" ORDER BY Count DESC LIMIT 10',
            conn
        )
        log_output(df_types.to_string())
        
        # Example query 4: Security properties violated
        log_output("\n" + "=" * 60)
        log_output("Security Properties Violated:")
        log_output("=" * 60)
        df_props = pd.read_sql_query(
            'SELECT "Violated Security Property", COUNT(*) as Count FROM "Automotive Security Attacks" GROUP BY "Violated Security Property" ORDER BY Count DESC LIMIT 10',
            conn
        )
        log_output(df_props.to_string())
        
    except Exception as e:
        log_output(f"Query error: {e}")
    
    conn.close()


def export_to_csv():
    """Export the entire database to a CSV file"""
    log_output("=" * 60)
    log_output("EXPORTING DATA TO CSV")
    log_output("=" * 60)
    
    # Create output directory
    OUTPUT_DIR = Path(__file__).parent.parent / "exports"
    OUTPUT_DIR.mkdir(exist_ok=True)
    
    conn = sqlite3.connect(DB_PATH)
    
    try:
        # Get all tables
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [table[0] for table in cursor.fetchall()]
        
        for table_name in tables:
            # Read entire table into DataFrame
            df = pd.read_sql_query(f'SELECT * FROM "{table_name}"', conn)
            
            # Clean up data by replacing newlines with spaces
            for col in df.columns:
                if df[col].dtype == 'object':
                    df[col] = df[col].str.replace('\n', ' ').str.replace('\r', ' ').str.strip()
            
            # Create CSV file
            csv_filename = f"AAD_{table_name.replace(' ', '_')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            csv_path = OUTPUT_DIR / csv_filename
            
            # Export to CSV
            df.to_csv(csv_path, index=False, encoding='utf-8')
            
            log_output(f"\nExported: {table_name}")
            log_output(f"  Rows: {len(df)}")
            log_output(f"  Columns: {len(df.columns)}")
            log_output(f"  File: {csv_path}")
        
        log_output(f"\nAll exports complete! Files saved to: {OUTPUT_DIR}")
        print(f"\nCSV export complete! Files saved to: {OUTPUT_DIR}")
        
    except Exception as e:
        log_output(f"Export error: {e}")
        print(f"Export error: {e}")
    
    conn.close()


if __name__ == "__main__":
    print(f"\nDatabase path: {DB_PATH}")
    print(f"Database exists: {DB_PATH.exists()}")
    print(f"Log file: {LOG_FILE}\n")
    
    if DB_PATH.exists():
        inspect_database()
        dataframes = load_data()
        example_queries()
        export_to_csv()
        print(f"\nAnalysis complete! Results saved to: {LOG_FILE}")
    else:
        print("ERROR: Database file not found!")