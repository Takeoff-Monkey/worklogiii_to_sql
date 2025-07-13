
import os
import json
import requests
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from time import sleep
import datetime


load_dotenv()


# --- Unified Configuration (Cleaner and Easier to Maintain) ---
# Single source of truth for column renames and their purpose.
COLUMN_CONFIG = {
   'Job Name': {'new_name': "job_name", 'desc': "The name of the job"},
   'multiple_person_mkqnhsnf': {'new_name': "prep_team", 'desc': "Team responsible for prepping the job"},
   'multiple_person': {'new_name': "production_team", 'desc': "Team responsible for executing the project"},
   "people": {'new_name': "reviewer_deliverer", 'desc': "Person responsible for final review & delivery"},
   "text2": {'new_name': "sender_name", 'desc': "Name of customer who submitted the project"},
   "color56": {'new_name': "primary_status", 'desc': "Primary current status of the project"},
   'color': {'new_name': "product", 'desc': "The product category"},
   'date': {'new_name': "due_date", 'desc': "The internal due date for the project"},
   'long_text_mkqwc9v8': {'new_name': "upload_notes", 'desc': "Notes from the upload process"},
   'date_mkq9h641': {'new_name': "customer_due_date", 'desc': "The actual due date requested by the customer"},
   'dropdown35': {'new_name': "scope_of_work", 'desc': "The defined scope of work for the project"},
   'email': {'new_name': "sender_email", 'desc': "Email address of the person who submitted the project"},
   'date9': {'new_name': "completion_date", 'desc': "Date the project was marked as complete"},
   'text0': {'new_name': "customer_name", 'desc': "The name of the customer"},
   'date46': {'new_name': "delivered_date", 'desc': "Date the project was delivered to the customer"},
   'date4': {'new_name': "received_date", 'desc': "Date the project was received"},
   'numeric': {'new_name': "page_count_standard_mto", 'desc': "Standard MTO page counts"},
   'numeric8': {'new_name': "page_count_maintenance", 'desc': "Maintenance page count"},
   'numeric4': {'new_name': "page_count_building_trades", 'desc': "Building trades page count"},
   'numeric2': {'new_name': "page_count_irrigation_bid", 'desc': "Irrigation bid design page count"},
   'numeric21': {'new_name': "page_count_irrigation_full", 'desc': "Irrigation full design page counts"},
   'numeric7': {'new_name': "page_count_other_misc", 'desc': "Other/misc page counts"},
   'numbers': {'new_name': "page_count_surcharge", 'desc': "Additional surcharge page counts"},
   'numeric0': {'new_name': "page_count_trial", 'desc': "Trial page counts"},
   'color8': {'new_name': "billing_status", 'desc': "The current billing status"},
   'link': {'new_name': "completed_files_url", 'desc': "Dropbox URL for completed files"},
   'item_id': {'new_name': "monday_item_id", 'desc': "The unique item ID from Monday.com"}
}


# --- API and DB Constants ---
MONDAY_API_KEY = os.getenv("MONDAY_API_KEY")
BOARD_ID = os.getenv("MONDAY_BOARD_ID", 3874058084)  # Best to get from .env
FETCH_BATCH_SIZE = 100  # Max items to fetch in a single API call
DELAY = 0.5
HEADERS = {"Authorization": MONDAY_API_KEY, "Content-Type": "application/json"}


POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", 5432)




# --- DB Connection ---
def connect_postgres():
   """Establishes a connection to the PostgreSQL database."""
   return create_engine(
       f'postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}'
   )




# --- Get last update timestamp ---
def get_last_sync_time(engine):
   """Retrieves the most recent 'updated_at' timestamp from the index table."""
   with engine.begin() as conn:
       result = conn.execute(text("SELECT MAX(updated_at) FROM worklog_index"))
       ts = result.scalar()
       return ts.isoformat() if ts else "1970-01-01T00:00:00Z"




# --- Set up tracking table ---
def create_worklog_index_if_missing(engine):
   """Ensures the tracking table exists in the database."""
   with engine.begin() as conn:
       conn.execute(text("""
                         CREATE TABLE IF NOT EXISTS worklog_index
                         (
                             item_id
                             BIGINT
                             PRIMARY
                             KEY,
                             item_name
                             TEXT,
                             updated_at
                             TIMESTAMP
                             WITH
                             TIME
                             ZONE
                         );
                         """))




# --- Fetch metadata for updated items ---
def fetch_updated_items_since(board_id, last_sync_time_iso):
   """Fetches metadata for items updated since last sync OR received today."""
   # This query is now corrected to use the 'today' operator for date columns.
   query = f"""
   query {{
     boards(ids: {board_id}) {{
       items_page(
         limit: 500,
         query_params: {{
           rules: [
             {{
               column_id: "__last_updated__",
               compare_value: ["{last_sync_time_iso}"],
               operator: greater_than
             }},
             {{
               column_id: "date4",
               operator: today
             }}
           ],
           operator: or
         }}
       ) {{
         items {{
           id
           name
           updated_at
         }}
       }}
     }}
   }}
   """
   response = requests.post("https://api.monday.com/v2", headers=HEADERS, json={"query": query})
   data = response.json()


   if response.status_code != 200 or "errors" in data:
       raise Exception(f"Failed to fetch updated items: {response.status_code} - {data}")


   items = data["data"]["boards"][0]["items_page"]["items"]
   print(f"✅ Retrieved metadata for {len(items)} updated or backlogged items.")
   return items




# --- REFACTORED: Efficiently fetch full data for specific items ---
def fetch_full_items_by_id(board_id, item_ids):
   """
   Fetches full item data for a specific list of item IDs in batches.
   This is much more efficient than paginating the entire board.
   """
   print(f"📦 Starting batched fetch for {len(item_ids)} items...")
   all_rows = []
   item_ids_list = list(item_ids)  # Convert set to list for slicing


   for i in range(0, len(item_ids_list), FETCH_BATCH_SIZE):
       batch_ids = item_ids_list[i:i + FETCH_BATCH_SIZE]


       # The `json.dumps` is crucial to correctly format the list for the GraphQL query
       query = f"""
       query {{
         items(ids: {json.dumps(batch_ids)}) {{
           id
           name
           updated_at
           column_values {{
             id
             text
           }}
         }}
       }}
       """


       print(f"🌐 Sending request for batch of {len(batch_ids)} items...")
       response = requests.post("https://api.monday.com/v2", headers=HEADERS, json={"query": query})


       if response.status_code != 200:
           raise Exception(f"Request failed: {response.status_code}, {response.text}")


       data = response.json()
       if "errors" in data:
           raise Exception(f"GraphQL errors returned: {data['errors']}")


       items = data.get("data", {}).get("items", [])


       for item in items:
           row = {
               "monday_item_id": int(item["id"]),
               "job_name": item["name"],
               "updated_at": item["updated_at"]
           }
           for col in item["column_values"]:
               # Use the new_name from our config, fall back to original id if not found
               col_config = next((v for k, v in COLUMN_CONFIG.items() if k == col["id"]), None)
               col_name = col_config['new_name'] if col_config else col["id"]
               row[col_name] = col["text"]
           all_rows.append(row)


       sleep(DELAY)


   print(f"🎯 Done fetching. Retrieved full data for {len(all_rows)} items.")
   return pd.DataFrame(all_rows)




# --- Main Sync Logic ---
def sync_incremental(engine):
   """Orchestrates the entire incremental sync process."""
   print("🔧 Ensuring worklog_index exists...")
   create_worklog_index_if_missing(engine)


   last_sync_time = get_last_sync_time(engine)
   print(f"⏰ Last sync time from worklog_index: {last_sync_time}")


   print("📡 Requesting updated items from Monday.com...")
   updated_item_metadata = fetch_updated_items_since(BOARD_ID, last_sync_time)


   if not updated_item_metadata:
       print("🎉 No new updates to process.")
       return


   updated_ids = {int(item["id"]) for item in updated_item_metadata}
   print(f"🧩 Processing {len(updated_ids)} updated item IDs...")


   print("📥 Fetching full row data for updated items...")
   updated_df = fetch_full_items_by_id(BOARD_ID, updated_ids)


   if updated_df.empty:
       print("⚠️ No detailed item data was returned. Skipping database update.")
       return


   # Prepare data for the index table upsert
   index_data_to_upsert = [
       {"item_id": int(item["id"]), "item_name": item["name"], "updated_at": item["updated_at"]}
       for item in updated_item_metadata
   ]


   print("📝 Upserting to worklog_index...")
   with engine.begin() as conn:
       for row in index_data_to_upsert:
           conn.execute(text("""
                             INSERT INTO worklog_index (item_id, item_name, updated_at)
                             VALUES (:item_id, :item_name, :updated_at) ON CONFLICT (item_id) DO
                             UPDATE SET
                                 item_name = EXCLUDED.item_name,
                                 updated_at = EXCLUDED.updated_at;
                             """), parameters=row)


   print("🛠 Inserting/Updating worklog table...")
   # For simplicity, we'll append. A true upsert here is more complex.
   # You would typically load to a temp table, then merge.
   # For now, you may need to handle duplicates downstream or by deleting old rows first.


   # A simple approach: delete old versions of rows before inserting new ones
   with engine.begin() as conn:
       id_tuple = tuple(updated_df['monday_item_id'].unique())
       if id_tuple:
           conn.execute(text(f"DELETE FROM worklog WHERE monday_item_id IN {id_tuple}"))


   updated_df.to_sql("worklog", engine, if_exists="append", index=False)


   print("✅ Incremental sync complete.")




def main():
   print("🚀 Starting incremental sync...")
   engine = connect_postgres()
   sync_incremental(engine)
   print("🎉 Done.")




if __name__ == "__main__":
   main()


