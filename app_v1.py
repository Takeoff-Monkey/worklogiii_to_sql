
import os
import requests
import pandas as pd
from sqlalchemy import create_engine, text, inspect
from dotenv import load_dotenv
from time import sleep
import psycopg2
import datetime




load_dotenv()


COLUMN_RENAMES = {
   'Job Name': "job name",
   'multiple_person_mkqnhsnf': "prep team",
   'multiple_person': "production team",
   "people": "reviewer / deliverer",
   "text2": "sender name",
   "color56": "PRIMARY status",
   'color': "product",
   'date': "due date",
   'long_text_mkqwc9v8': "upload notes",
   'date_mkq9h641': "actual customer due date",
   'dropdown35': "scope of work",
   'email': "sender email address",
   'file': "files",
   'link9': "file link",
   'date9': "completion date",
   'text0': "customer name",
   'mirror_15': "customer name again",
   'date46': "delivered date",
   'date4': "received date",
   'numeric': "standard mto page counts",
   'numeric8': "maintenance page count",
   'numeric4': "building trades page count",
   'numeric2': "irrigaiton bid design page count",
   'numeric21': "irrigaiton full design page counts",
   'numeric7': "other - misc page counts",
   'numbers': "additional surcharge page counts",
   'numeric0': "trial page counts",
   'color8': "billing status",
   'mirror6': "invoice type",
   'link': "completed files dropbox URL",
   'duration': "time tracking",
   'multiple_person9': "client team",
   'formula0': "customer address",
   'item_id': "monday item id",
   'duration_mkqh5ne1': "prep time tracking"
   }




JOB_TYPE_MAP = {
         "MTO - Maint.": "Material takeoff on an existing property for landscape maintenance",
         "MTO - Const.": "material takeoff on construction drawings, any scope of work",
         "IR - FLD": "irrigaiton full design; complete design with details, schedules, hydraulics, etc.",
         "For US Team": "usually some sort of data or research that only the US team can handle",
         "LSD / Misc.": "drafting a landscape design or some other form of misc. drafting",
         "Submittal Pkg": "compiling a submittal packet; product data, photos, etc.",
         "IR - BD": "irrigation bid design; slightly less developed irrigation design, does not include laterals, wire, or sized valves",
         "QUOTE Only": "a project we need only to give a price for",
         "REVIEW Only": "usually an example project we need to review to learn from and ask questions from",
         "As Built": "actual field conditions of a newly installed irrigation system, essentially an irrigation full design color-coded",
         "Phasing": "separate a completed MTO or design into various zones or phases",
         "Qualify/Extract Pages": "go through a bid package and identify scope(s) and extract relevant documents",
         "Highlight/Identify Walls": "simply identifying the locations of retaining walls without measurements within a set of drawings",
         "MTO - SPECIAL": "some sort of exceptional type of material takeoff",
         "MTO - Mt.": "duplicate of MTO - Maint.",
         "Drafting / Other": "duplicate of LSD / Misc"
   }




STATUS_MAP = {
      "Pending": "project has been received, but not yet in production queue",
      "Complete in Procore": "HLS only; MTO is complete within Procore",
      "Uploaded": "job has been received, files, notes are loaded and prep, production teams have been assigned",
      "In Process": "job is currently being executed by production team",
      "Complete": "production team has completed the project, pending final review & delivery",
      "Delivered": "final review now complete, project has been delivered to the customer",
      "HOLD": "pause production; usually awaiting additional files and / or clarification",
      "CANCELLED": "project has been cancelled - can happen at any point",
      "PROCESSED": "final review is complete but has not been delivered",
      "Convo": "status used to filter out board items that are not projects, usually testing items, or spam emails",
      "ISSUES": "errors or omissions found during final review, job needs to go back to production for corrections",
      "Reached Out": "used only with brand new customers submitting projects thru the site",
      "PREP": "project is in the queue and is being prepped by the prep team",
      "Prep Complete": "project preparations complete, ready for production",
      "Design Prep": "production has completed a design, awaiting preparations in customer software",
      "Practice Job": "a project solely for the purposes of production or prep teams learning a new software or a new scope of work",
      "Prep in Process": "project is being prepped by prep team",
      "Accepted": "disregard"}




COLUMN_DESCRIPTIONS = {
   'Job Name': "the name of the job",
   'multiple_person_mkqnhsnf': "team responsible for prepping the job, not applicable to all customers",
   'multiple_person': "team responsible for executing the project",
   "people": "person responsible for final review & delivery of the completed project",
   "text2": "name of customer who submitted the project, or name of branch / division requesting the project",
   "color56": "primary current status of the project, changes often",
   'color': "product",
   'date': "due date",
   'long_text_mkqwc9v8': "upload notes",
   'date_mkq9h641': "actual customer due date",
   'dropdown35': "scope of work",
   'email': "sender email address",
   'file': "files",
   'link9': "file link",
   'date9': "completion date",
   'text0': "customer name",
   'mirror_15': "customer name again",
   'date46': "delivered date",
   'date4': "received date",
   'numeric': "standard mto page counts",
   'numeric8': "maintenance page count",
   'numeric4': "building trades page count",
   'numeric2': "irrigation bid design page count",
   'numeric21': "irrigation full design page counts",
   'numeric7': "other - misc page counts",
   'numbers': "additional surcharge page counts",
   'numeric0': "trial page counts",
   'color8': "billing status",
   'mirror6': "invoice type",
   'link': "completed files dropbox URL",
   'duration': "time tracking",
   'multiple_person9': "client team",
   'formula0': "customer address",
   'item_id': "monday item id",
   'duration_mkqh5ne1': "prep time tracking"
   }


#  Monday stuffs
MONDAY_API_KEY = os.getenv("MONDAY_API_KEY")
BOARD_ID = 3874058084
PAGE_SIZE = 500  # You can use 500–1000 safely
MAX_PAGES = 1000  # Adjust based on expected board size
DELAY = 0.5      # Add a small delay to avoid rate limits
HEADERS = {
   "Authorization": MONDAY_API_KEY,
   "Content-Type": "application/json"
}


#  Postgres stuffs
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", 5432)




# --- DB Connection ---
def connect_postgres():
   return create_engine(
      f'postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}'
   )




# --- Get last update timestamp ---
def get_last_sync_time(engine):
   with engine.begin() as conn:
      result = conn.execute(text("SELECT MAX(updated_at) FROM worklog_index"))
      ts = result.scalar()
      return ts.isoformat() if ts else "1970-01-01T00:00:00Z"




# --- Set up tracking table ---
def create_worklog_index_if_missing(engine):
   with engine.begin() as conn:
      conn.execute(text("""
      CREATE TABLE IF NOT EXISTS worklog_index (
         item_id TEXT PRIMARY KEY,
         item_name TEXT,
         updated_at TIMESTAMP
      );
      """))




# --- Fetch all item metadata (id + updated_at) ---
def fetch_updated_items_since(board_id, last_sync_time_iso):
   url = "https://api.monday.com/v2"
   headers = {
      "Authorization": os.getenv("MONDAY_API_KEY"),
      "Content-Type": "application/json"
   }


   query = f"""
   query {{
     boards(ids: {board_id}) {{
      items_page(
        limit: 100,
        query_params: {{
         rules: [
           {{
            column_id: "__last_updated__",
            compare_value: ["{last_sync_time_iso}"],
            operator: greater_than,
            compare_attribute: "UPDATED_AT"
           }},
           {{
            column_id: "date4",
            compare_value: ["TODAY"],
            operator: any_of
           }}
         ],
         operator: or
        }}
      ) {{
        items {{
         id
         name
         updated_at
         column_values {{
           id
           text
         }}
        }}
      }}
     }}
   }}
   """


   response = requests.post(url, headers=headers, json={"query": query})
   data = response.json()


   if response.status_code != 200 or "errors" in data:
      raise Exception(f"Failed to fetch updated items: {response.status_code} - {data}")


   items = data["data"]["boards"][0]["items_page"]["items"]
   print(f"✅ Retrieved {len(items)} updated or backlogged items.")
   return items




def clean_col(col):
   # Lowercase, replace spaces with underscores, remove weird chars, etc.
   import re
   return re.sub(r'\W+', '_', col.strip().lower())




def ensure_columns_exist(engine, df, table_name):
   with engine.connect() as conn:
      inspector = inspect(engine)
      existing_columns = [col["name"] for col in inspector.get_columns(table_name)]


      for col in df.columns:
         if col not in existing_columns:
            print(f"🛠️ Adding missing column '{col}' to '{table_name}' table...")
            conn.execute(text(f'ALTER TABLE "{table_name}" ADD COLUMN "{col}" TEXT;'))






# --- Fetch full data only for changed items ---
def fetch_full_items(board_id, changed_ids, column_mapping):
   print("📦 Starting full item fetch...")
   all_rows = []
   cursor = None
   total_processed = 0


   while True:
      after_clause = f', cursor: "{cursor}"' if cursor else ""
      query = f"""
      {{
        boards(ids: {board_id}) {{
         items_page(limit: {PAGE_SIZE}{after_clause}) {{
           cursor
           items {{
            id
            name
            updated_at
            column_values {{
              id
              text
            }}
           }}
         }}
        }}
      }}
      """


      print("🌐 Sending page request...")
      response = requests.post("https://api.monday.com/v2", headers=HEADERS, json={"query": query})


      if response.status_code != 200:
         print("❌ Failed request.")
         raise Exception(f"Request failed: {response.status_code}, {response.text}")


      data = response.json()
      items_page = data["data"]["boards"][0]["items_page"]
      items = items_page["items"]
      cursor = items_page["cursor"]


      print(f"📬 Retrieved {len(items)} items on this page.")


      if not items:
         break


      processed_ids = set()


      for item in items:
         if item["id"] not in changed_ids:
            continue
         row = {
            "item_id": item["id"],
            "Job Name": item["name"],
            "updated_at": item["updated_at"]
         }
         for col in item["column_values"]:
            title = column_mapping.get(col["id"], col["id"])
            row[title] = col["text"]
         all_rows.append(row)
         processed_ids.add(item["id"])


      # Stop early if we've processed all changed items
      if processed_ids == changed_ids:
         print("✅ All updated items processed.")
         break


      if not cursor:
         break


      # for item in items:
      #  if item["id"] not in changed_ids:
      #     continue
      #
      #  row = {
      #     "item_id": item["id"],
      #     "Job Name": item["name"],
      #     "updated_at": item["updated_at"]
      #  }
      #  for col in item["column_values"]:
      #     title = column_mapping.get(col["id"], col["id"])
      #     row[title] = col["text"]
      #
      #  all_rows.append(row)
      #  total_processed += 1
      #
      # print(f"✅ Processed so far: {total_processed} rows.")
      # if not cursor:
      #  break
      sleep(DELAY)


   print(f"🎯 Done fetching all full items. Total: {total_processed}")
   return pd.DataFrame(all_rows)






# --- Fetch column names (title mappings) ---
def fetch_column_mapping(board_id):
   query = f"""
   {{
     boards(ids: {board_id}) {{
      columns {{
        id
        title
      }}
     }}
   }}
   """
   response = requests.post("https://api.monday.com/v2", headers=HEADERS, json={"query": query})
   data = response.json()
   return {col["id"]: col["title"] for col in data["data"]["boards"][0]["columns"]}




# --- Update tables ---
def sync_incremental(engine):
   print("🔧 Ensuring worklog_index exists...")
   create_worklog_index_if_missing(engine)


   print("🔍 Fetching column mappings...")
   column_mapping = fetch_column_mapping(BOARD_ID)
   print(f"📚 Retrieved {len(column_mapping)} column mappings.")


   last_sync_time = get_last_sync_time(engine)
   print(f"⏰ Last sync time from worklog_index: {last_sync_time}")


   print("📡 Requesting updated items from Monday.com...")
   item_metadata = fetch_updated_items_since(BOARD_ID, last_sync_time)
   print(f"✅ Received metadata for {len(item_metadata)} items.")


   # Filter items updated since last sync
   updated_items = [item for item in item_metadata if item["updated_at"] > last_sync_time]
   print(f"📈 Found {len(updated_items)} items updated since last sync.")


   if not updated_items:
      print("🎉 No new updates to process.")
      return


   # Rename keys to match DB schema
   for item in updated_items:
      item["item_id"] = item.pop("id")
      item["item_name"] = item.pop("name")


   updated_ids = {item["item_id"] for item in updated_items}
   print(f"🧩 Processing {len(updated_ids)} updated item IDs...")


   print("📥 Fetching full row data for updated items...")
   updated_df = fetch_full_items(BOARD_ID, updated_ids, column_mapping)
   print(f"📊 Retrieved full data for {len(updated_df)} rows.")


   if updated_df.empty:
      print("⚠️ No detailed item data was returned. Skipping insert.")
      return


   print("📝 Upserting to worklog_index...")
   with engine.begin() as conn:
      for row in updated_items:
         conn.execute(text("""
            INSERT INTO worklog_index (item_id, item_name, updated_at)
            VALUES (:item_id, :item_name, :updated_at)
            ON CONFLICT (item_id) DO UPDATE SET
               item_name = EXCLUDED.item_name,
               updated_at = EXCLUDED.updated_at;
         """), parameters=row)


   print("📝 Upserting to worklog_index...")
   # ...
   print("cleaning all the fucking columns")
   updated_df.columns = [clean_col(c) for c in updated_df.columns]
   print("making sure all the fucking columns exist....")
   ensure_columns_exist(engine, updated_df, "worklog")
   print("🛠 Inserting to worklog table...")
   updated_df.to_sql("worklog", engine, if_exists="append", index=False)


   print("✅ Incremental sync complete.")






def main():
   print("🚀 Starting incremental sync...")
   engine = connect_postgres()
   sync_incremental(engine)
   print("🎉 Done.")




if __name__ == "__main__":
   main()


