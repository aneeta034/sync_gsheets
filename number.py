import os
import pandas as pd
from supabase import create_client, Client
from dotenv import load_dotenv
from datetime import datetime
from dateutil import parser

# Load environment variables
load_dotenv()

# Supabase configuration
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_API_KEY = os.getenv("SUPABASE_API_KEY")

# Initialize Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_API_KEY)

def fetch_google_sheet():
    """
    Fetches data from a Google Sheet and returns it as a pandas DataFrame.
    """
    try:
        csv_url = "https://docs.google.com/spreadsheets/d/e/2PACX-1vRCe57F8CKLdvpbA959NXTt7I85CIWGE50p8rHwnVk-hH4x1wrGyPvtDe0qrx8pUoPYiVzIF4weF3bK/pub?gid=25398211&single=true&output=csv"
        df = pd.read_csv(csv_url)
        df = df.where(pd.notna(df), None)  # Replace NaN with None
        print(f"Fetched {len(df)} records from Google Sheet")
        return df
    except Exception as e:
        print(f"Error fetching Google Sheet: {str(e)}")
        return pd.DataFrame()

def normalize_phone_number(phone_number):
    """
    Normalizes phone numbers by converting them to integers and then to strings.
    Removes any trailing `.0` or invalid characters.
    """
    if pd.isna(phone_number) or phone_number is None:
        return None
    try:
        return str(int(float(phone_number))).strip()  # Convert to integer and then to string
    except (ValueError, TypeError):
        return None

def check_single_entry():
    """
    Processes the first row of the DataFrame, checks if the phone number exists in the database,
    and stops the process after verifying one entry.
    """
    # Fetch data from Google Sheet
    df = fetch_google_sheet()
    if df.empty:
        print("No data to process")
        return
    
    # Process only the first row
    first_row = df.iloc[0]
    phone_number = first_row.get("PHONE NUMBER ")
    
    # Normalize the phone number
    normalized_phone = normalize_phone_number(phone_number)
    print(f"Phone number from Google Sheet: {phone_number}")
    print(f"Normalized phone number: {normalized_phone}")
    
    # Check if the phone number exists in the database
    try:
        print("Checking if the phone number exists in the database...")
        response = (
            supabase.table("customers")
            .select("phone")
            .eq("phone", normalized_phone)
            .execute()
        )
        
        if response.data:
            print(f"Phone number '{normalized_phone}' already exists in the database.")
            print("Retrieved phone number from database:", response.data[0]["phone"])
        else:
            print(f"Phone number '{normalized_phone}' does not exist in the database.")
    except Exception as e:
        print(f"Error checking database: {str(e)}")

if __name__ == "__main__":
    check_single_entry()