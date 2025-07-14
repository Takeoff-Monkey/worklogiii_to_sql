import requests
import os
import json
from dotenv import load_dotenv
from sqlalchemy import create_engine, Engine, text
import pandas as pd
import sqlparse
from rich.console import Console
from rich.syntax import Syntax
from sqlalchemy import create_engine, Engine
from time import sleep

load_dotenv()


#  Monday API Stuffs
MONDAY_API_KEY = os.getenv("MONDAY_API_KEY")
BOARD_ID = 3874058084
PAGE_SIZE = 500  # You can use 500–1000 safely
MAX_PAGES = 100  # Adjust based on expected board size
DELAY = 0.5      # Add a small delay to avoid rate limits
headers = {
    "Authorization": MONDAY_API_KEY,
    "Content-Type": "application/json"
}

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




def create_metadata_tables(engine):
    with engine.begin() as conn:  # <-- Use begin() for transactional DDL
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS column_renames (
                column_id TEXT PRIMARY KEY,
                friendly_name TEXT
            );
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS column_descriptions (
                column_id TEXT PRIMARY KEY,
                description TEXT
            );
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS status_map (
                status TEXT PRIMARY KEY,
                description TEXT
            );
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS job_type_map (
                job_type TEXT PRIMARY KEY,
                description TEXT
            );
        """))

def upsert_dict_to_table(engine, table_name, mapping, key_col, value_col):
    with engine.connect() as conn:
        for key, value in mapping.items():
            conn.execute(
                text(f"""
                    INSERT INTO {table_name} ({key_col}, {value_col})
                    VALUES (:key, :value)
                    ON CONFLICT ({key_col}) DO UPDATE SET {value_col} = EXCLUDED.{value_col}
                """),
                {"key": key, "value": value}
            )



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
    response = requests.post("https://api.monday.com/v2", headers=headers, json={"query": query})
    data = response.json()
    return {col["id"]: col["title"] for col in data["data"]["boards"][0]["columns"]}


#  step 2 - fetch all items w pagination
def fetch_all_items(board_id, column_mapping):
    all_rows = []
    cursor = None

    while True:
        after_clause = f', cursor: "{cursor}"' if cursor else ""
        query = f"""
        {{
          boards(ids: {board_id}) {{
            items_page(limit: {PAGE_SIZE}{after_clause}) {{
              cursor
              items {{
                name
                column_values {{
                  id
                  text
                }}
              }}
            }}
          }}
        }}
        """
        response = requests.post("https://api.monday.com/v2", headers=headers, json={"query": query})
        if response.status_code != 200:
            raise Exception(f"Request failed: {response.status_code}, {response.text}")

        data = response.json()
        items_page = data["data"]["boards"][0]["items_page"]
        items = items_page["items"]
        cursor = items_page["cursor"]

        if not items:
            break

        for item in items:
            row = {"Job Name": item["name"]}
            for col in item["column_values"]:
                title = column_mapping.get(col["id"], col["id"])
                row[title] = col["text"]
            all_rows.append(row)

        print(f"Fetched {len(items)} items...")
        if not cursor:
            break

        sleep(DELAY)

    return pd.DataFrame(all_rows)


def save_df_to_postgres(df):
    db = os.getenv("POSTGRES_DB")
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    host = os.getenv("POSTGRES_HOST")
    port = os.getenv("POSTGRES_PORT", "5432")

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    df.to_sql('worklog', engine, if_exists='replace', index=False)
    print("dataframe saved to postgreSQL successfully!")


def main_postgres():
    print("🔗 Fetching column mapping...")
    column_mapping = fetch_column_mapping(BOARD_ID)

    print("📥 Fetching all worklog items from Monday.com...")
    df = fetch_all_items(BOARD_ID, column_mapping)

    save_df_to_postgres(df)
    print("great success motherfuckers!!!!!")


def main():
    # 1. Create your DB engine (using your env variables)
    db = os.getenv("POSTGRES_DB")
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    host = os.getenv("POSTGRES_HOST")
    port = os.getenv("POSTGRES_PORT", "5432")
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # 2. Create metadata tables
    create_metadata_tables(engine)

    # 3. Upsert mapping dictionaries into tables
    upsert_dict_to_table(engine, 'column_renames', COLUMN_RENAMES, "column_id", "friendly_name")
    upsert_dict_to_table(engine, 'column_descriptions', COLUMN_DESCRIPTIONS, "column_id", "description")
    upsert_dict_to_table(engine, 'status_map', STATUS_MAP, "status", "description")
    upsert_dict_to_table(engine, 'job_type_map', JOB_TYPE_MAP, "job_type", "description")

    # 4. Fetch and upload your Monday.com data as usual
    print("🔗 Fetching column mapping...")
    column_mapping = fetch_column_mapping(BOARD_ID)

    print("📥 Fetching all worklog items from Monday.com...")
    df = fetch_all_items(BOARD_ID, column_mapping)

    save_df_to_postgres(df)
    print("great success motherfuckers!!!!!")


if __name__ == "__main__":
    main()
