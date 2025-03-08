import requests
import json
from supabase import create_client, Client

def login_and_fetch_tables(credentials):
    try:
        url = f"{credentials['SUPABASE_URL']}/auth/v1/token?grant_type=password"
        payload = {"email": credentials["EMAIL"], "password": credentials["PASSWORD"]}
        headers = {"apikey": credentials["SUPABASE_API_KEY"], "Content-Type": "application/json"}

        response = requests.post(url, json=payload, headers=headers)

        if response.status_code != 200:
            print(f"Login failed: {response.json()}")
            return None

        print(f"Successfully logged in !")

        return

    except Exception as e:
        print(f"Error accessing {credentials['SUPABASE_URL']}: {e}")
        return 

def main():
    with open("credentials.json", "r") as file:
        credentials_list = json.load(file)

    for credentials in credentials_list:
        login_and_fetch_tables(credentials)

if __name__ == "__main__":
    main()