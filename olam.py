from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import logging
from langchain_ollama import OllamaLLM
from langchain_community.embeddings import OllamaEmbeddings

import os
from langchain_community.document_loaders import TextLoader
import pandas as pd

from llama_index.core import Document
from llama_index.core.node_parser import SentenceSplitter
from qdrant_client import QdrantClient
from qdrant_client.http import models
from qdrant_client.http.models import VectorParams, Distance

# ================================ Config ============================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


llm = OllamaLLM(model="llama3.1", temperature=0.5)
client = QdrantClient(host="qdrant", port=6333)

default_args = {
    'owner': 'Trace',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}
# ===================================================================================
collection = "trace"

def load(**context):
    """Load data from JSON file and return texts."""
    import os
    file_path = os.path.join(os.path.dirname(__file__), "data", "traceminerals_data.json")
    try:
        df = pd.read_json(file_path)
        if df.empty:
            logging.error("No data found in the specified JSON file.")
        else:
            logging.info("Data loaded successfully.")
        
        # Save to file AND capture as string
        text = df.to_csv(sep="\t", index=False)
        with open("record.txt", "w") as f:
            f.write(text)
        
        logging.info(f"Pushing text with length: {len(text)}")
        return text  
    except Exception as e:
        logging.error(f"Error loading file: {e}")
        raise


def vector_index(**context):
    """Index texts into Qdrant vector store."""
    # =============== Retrieve =================
    texts = context['ti'].xcom_pull(task_ids='load', key='return_value')
    logging.info(f"Received texts with length: {len(texts)}")
    
    if not texts:
        logging.error("No texts received for indexing.")
        raise ValueError("No texts received for indexing.")

    #========== Create collection =====================
    try:
        if not client.collection_exists(collection_name=collection):
            client.create_collection(
                collection_name=collection,
                vectors_config=VectorParams(size=768, distance=Distance.COSINE)
            )
            logging.info(f"Created Qdrant collection '{collection}'.")
    except Exception as e:
        logging.error(f"Failed to connect to or create collection in Qdrant: {e}")
        raise

    # ============== Parse texts into nodes ====================
    try:
        df = pd.read_csv(pd.io.common.StringIO(texts), sep="\t")
        if df.empty:
            logging.error("No data found in the CSV string from XCom.")
            raise ValueError("Empty DataFrame after parsing CSV.")
        # Convert each row to a single text string by joining column values
        docs = [Document(text=" ".join(str(value) for value in row)) for row in df.values]
        parser = SentenceSplitter(chunk_size=500, chunk_overlap=10)
        nodes = []
        for doc in docs:
            nodes.extend(parser.get_nodes_from_documents([doc]))
    except Exception as e:
        logging.error(f"Failed to parse texts into nodes: {e}")
        raise

    # ==========Generate embeddings and prepare data for upsert
    ids = []
    vectors = []
    payloads = []
    embeddings_model = OllamaEmbeddings(model="nomic-embed-text")

    for idx, node in enumerate(nodes):
        try:
            embedding = embeddings_model.embed_documents([node.text])[0]
            ids.append(idx)
            vectors.append(embedding)
            payloads.append({"text": node.text})
        except Exception as e:
            logging.error(f"Embedding failed for item {idx}: {e}")
            continue

    if not ids:
        logging.error("No valid embeddings generated.")
        raise ValueError("No valid embeddings generated.")

    # ====Upsert vectors to Qdrant
    try:
        client.upsert(
            collection_name=collection,
            points=models.Batch(
                ids=ids,
                vectors=vectors,
                payloads=payloads
            )
        )
        logging.info(f"Upserted {len(ids)} vectors into collection '{collection}'.")
    except Exception as e:
        logging.error(f"Failed to upsert vectors to Qdrant: {e}")
        raise

# ==============Define the DAG==============
with DAG(
    dag_id='test_ollama',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False
) as dag:
    # Task to load data
    text_task = PythonOperator(
        task_id='load',
        python_callable=load,
        provide_context=True,
        priority_weight=2,
    )

    # Task to index vectors
    index_task = PythonOperator(
        task_id='index',
        python_callable=vector_index,
        provide_context=True,
    )

    
    text_task >> index_task