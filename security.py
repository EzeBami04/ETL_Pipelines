from airflow.decorators import dag, task

from datetime import datetime, timedelta
from airflow.models import Variable
from cryptography.fernet import Fernet
import os

fernet = Fernet(Fernet.generate_key())

def encrypt_message(message: str) -> str:
    return fernet.encrypt(message.encode()).decode()

def decrypt_message(encrypted_message: str) -> str:
    return fernet.decrypt(encrypted_message.encode()).decode()

@dag(
    start_date=datetime(2025, 3, 5),
    schedule_interval="@daily",
    catchup=False,
    default_args={
        "owner": "Trace",
        "retries": 3,
        "email_on_failure": False
    }
    )
def sensitive_data_management():
    @task
    def retrieve_and_decrypt():
        try:
            encrypted_value_existing = Variable.get("Gcp_bucket_credentials", default_var=None)
            encrypted_value_new = Variable.get("Gcp_bucket_credentials_update", default_var=None)

            if encrypted_value_new:
                encrypted_value = encrypted_value_new
            elif encrypted_value_existing:
                encrypted_value = encrypted_value_existing
            else:
                print("No credentials found.")
                return

            decrypted_value = decrypt_message(encrypted_value)
            print(f"Decrypted value: {decrypted_value}")

        except Exception as e:
            print(f"Error occurred: {str(e)}")

    @task
    def update_encrypted_value():
        new_sensitive_data = os.getenv("new_s3_bucket_credentials")
        if new_sensitive_data:
            encrypted_value = encrypt_message(new_sensitive_data)
            Variable.set("s3_bucket", encrypted_value)
        else:
            print("No new sensitive data found.")

    decrypt_task = retrieve_and_decrypt()
    encrypt_task = update_encrypted_value()

    decrypt_task >> encrypt_task

sensitive_data_management_dag = sensitive_data_management()
