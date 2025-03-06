import requests
import os
from supabase import create_client, Client
from dotenv import load_dotenv


load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
API_KEY = os.getenv("SUPABASE_API_KEY")
EMAIL = os.getenv("EMAIL")
PASSWORD = os.getenv("PASSWORD")
TABLE_NAME = "controls"
CONFIG_KEY = "last_lead_synced_on"

def login_and_access_project():
    
    url = f"{SUPABASE_URL}/auth/v1/token?grant_type=password"
    payload = {"email": EMAIL, "password": PASSWORD}
    headers = {"apikey": API_KEY, "Content-Type": "application/json"}

    response = requests.post(url, json=payload, headers=headers)

    if response.status_code == 200:
        print("Successfully logged in!")
    else:
        print(f"Login failed: {response.json()}")
        return

    
    supabase: Client = create_client(SUPABASE_URL, API_KEY)

    data = (
        supabase.table(TABLE_NAME)
        .select("config_value")
        .eq("config_key", CONFIG_KEY)
        .execute()
    )

    if data.data:
        last_synced = data.data[0]["config_value"]
        print(f"Last Lead Synced On: {last_synced}")
    else:
        print("Config key not found.")


if __name__ == "__main__":
    login_and_access_project()
