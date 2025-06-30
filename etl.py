import psycopg2
from sqlalchemy import create_engine
import pandas as pd
import logging
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
base_url = "https://data.gov.il/api/3/action/datastore_search"

def generate_table_schema(df: pd.DataFrame, table_name: str) -> str:
    type_mapping = {
        'int64': 'INTEGER',
        'float64': 'FLOAT',
        'object': 'TEXT',
        'string': 'TEXT',
        'bool': 'BOOLEAN',
        'Int64': 'INTEGER',
        'datetime64[ns]': 'TIMESTAMP'
        }
    columns = []
    for col in df.columns:
        col = col.replace(' ', '_').replace(':', '_').replace('-', '_')
        dtype = str(df[col].dtype)
        if col == '_id':
            columns.append("_id INTEGER PRIMARY KEY")
        else:
            columns.append(f'"{col}" {type_mapping.get(dtype, "TEXT")}')
    return f'CREATE TABLE IF NOT EXISTS public.{table_name} ({", ".join(columns)});'

def extractor(resource_id: str):
    limit = 1000
    session = requests.Session()
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retries))
    
    try:
        res = session.get(base_url, params={"resource_id": resource_id, "limit": 1})
        res.raise_for_status()
        total = res.json()['result']['total']
        logging.info(f"Total records to fetch for resource {resource_id}: {total}")
        
        all_records = []
        for offset in range(0, total, limit):
            params = {"resource_id": resource_id, "limit": limit, "offset": offset}
            res = session.get(base_url, params=params)
            if res.status_code == 200:
                records = res.json()['result']['records']
                all_records.extend(records)
                logging.info(f"Fetched {len(records)} records at offset {offset}")
            else:
                logging.error(f"Error at offset {offset}: {res.status_code}")
                break
        df = pd.DataFrame(all_records)
        logging.info(f"Extracted {len(df)} records for resource {resource_id}")
        return df
    except Exception as e:
        logging.error(f"Extraction failed for resource {resource_id}: {e}")
        return None

def load(resource_id: str, table_name: str, conn_str: str):
    # Use raw psycopg2 connection
    conn = psycopg2.connect(conn_str)
    conn.autocommit = False
    cursor = conn.cursor()
    
    try:
        df = extractor(resource_id)
        if df is None or df.empty:
            logging.warning(f"No data to load or DataFrame is empty for {table_name}.")
            return

        # Data preprocessing
        for column in df.columns:
            if df[column].dtype == 'object':
                df[column] = df[column].astype(str).str.lower()
            df[column].replace({pd.NA: None, float('nan'): None}, inplace=True)

        if '_id' in df.columns:
            df['_id'] = pd.to_numeric(df['_id'], errors='coerce').astype('Int64')
            if df['_id'].isna().any():
                logging.error(f"Found NaN values in _id column for {table_name}, which is not allowed for PRIMARY KEY")
                return
            if not df['_id'].is_unique:
                logging.error(f"Duplicate _id values found for {table_name}: {df[df['_id'].duplicated()]['_id'].tolist()}")
                return

        numeric_columns = [col for col in df.columns if col.startswith('kamut_') or col.endswith('_WLTP') or col == 'nikud_betihut' or col == 'CO2_WLTP_NEDC']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # Set schema search path
        cursor.execute("SET search_path TO public;")
        
        # Create table
        create_table_sql = generate_table_schema(df, table_name)
        logging.info(f"Generated CREATE TABLE SQL: {create_table_sql}")
        cursor.execute(f"DROP TABLE IF EXISTS public.{table_name};")
        cursor.execute(create_table_sql)
        logging.info(f"Table public.{table_name} created successfully.")
        
        # Verify table existence
        cursor.execute(f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'public' AND table_name = '{table_name}');")
        table_exists = cursor.fetchone()[0]
        if not table_exists:
            logging.error(f"Table public.{table_name} was not created successfully.")
            return
        logging.info(f"Table public.{table_name} verified in database.")

        # Load into staging table using SQLAlchemy
        engine = create_engine(conn_str)
        temp_table = f"public.{table_name}_staging"
        df.to_sql(temp_table, con=engine, index=False, if_exists='replace', schema='public')
        logging.info(f"Data loaded into {temp_table} successfully with {len(df)} rows.")

        # Perform upsert
        upsert_sql = f"""
        INSERT INTO public.{table_name} ({", ".join([f'"{col}"' for col in df.columns])})
        SELECT {", ".join([f'"{col}"' for col in df.columns])} FROM {temp_table}
        ON CONFLICT (_id) DO UPDATE SET
        {", ".join([f'"{col}" = EXCLUDED."{col}"' for col in df.columns if col != '_id'])};
        """
        logging.info(f"Executing upsert SQL: {upsert_sql}")
        cursor.execute(upsert_sql)
        logging.info(f"Data upserted successfully into public.{table_name}.")
        
        # Clean up staging table
        cursor.execute(f"DROP TABLE IF EXISTS {temp_table};")
        logging.info(f"Staging table {temp_table} dropped.")
        
        conn.commit()
    except Exception as e:
        logging.error(f"Error processing {table_name}: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()