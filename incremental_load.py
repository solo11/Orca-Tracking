# imports
import json
from openai import OpenAI
import duckdb
import polars as pl
import gspread
from gspread_dataframe import set_with_dataframe, get_as_dataframe
from dotenv import load_dotenv
import os


from data_scraping import extract_markdown_from_orca_sightings
import SQLstatements

# Load ENV variables
load_dotenv()
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
MOTHERDUCK_TOKEN = os.getenv('MOTHERDUCK_TOKEN')
GOOGLE_SHEETS_DOCUMENT_ID = os.getenv('SHEETS_DOCUMENT_ID')

# Get data
markdown_list = extract_markdown_from_orca_sightings()

# Change the index of md_list depending upon how many days of incremental load you need. Ex: index 0 for 1 day load, index 1 for 2 days load
# Default = 1
md_list_incremental = markdown_list[1]

# Open AI function calling
# OPENAI data Extraction

client = OpenAI(api_key=OPENAI_API_KEY)

system_prompt = '''
   Process the whale sightings reported by users online and output in a structured format, the sightings are reported from across the world sometimes occurring in same date with different sighting time.
   For the latitude and longitude column use the location to infer these values
   These are the column defications:
   "Date": "The date when the orca or whale sighting occurred, formatted as MM/DD/YYYY. verify the date is correct from the input provided, all the sightings in December are from the year 2024 and all the sightings in January are from the year 2025",
    "Time": "The time of the sighting, typically in 12-hour format with AM/PM indication.",
    "Species": "The species or specific ecotype of whale or orca observed (e.g., Southern Residents, Bigg's).",
    "Location": "The reported location of the sighting, often including landmarks or geographic areas.",
    "Latitude": "The latitude coordinate of the sighting location, measured in decimal degrees. only contain a numeric value",
    "Longitude": "The longitude coordinate of the sighting location, measured in decimal degrees. only contain a numeric value",
    "Reported By": "The name of the individual or organization reporting the sighting.
    "Number of Animals": "The number of whales or orcas observed during the sighting, if reported. contain a numeric value",
    "Direction of Travel": "The general direction in which the animals were moving (e.g., North, South, Northwest).",
    "Observation": "Observations about the behavior/activity of the animals, such as traveling, milling, hunting, or spyhopping. Also Additional details about the sighting, such as observer information, unusual features, or actions",
    "Males": "Indicates whether any males were identified among the group, if known.",
    "Links": "References or URLs related to the sighting or Facebook links, if available. All URL's separated by comma",
    "Raw_Text" : "All the unprocessed text from which the data was extracted from."
'''

MODEL = "gpt-4o"

def parse_data(user_prompt):
    response = client.chat.completions.create(
        model=MODEL,
        messages=[
            {
                "role": "system",
                "content": system_prompt
            },
            {
                "role": "user",
                "content": user_prompt
            }
        ],
        response_format={

        "type": "json_schema",
        "json_schema": {
            "name": "sightings",
            "description": "Sightings list",
            "schema": {
                "type": "object",
                "properties": {
                    "sighting": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "Date": {"type": "string"},
                                "Time": {"type": "string"},
                                "Species": {"type": "string"},
                                "Location": {"type": "string"},
                                "Latitude": {"type": "string"},
                                "Longitude": {"type": "string"},
                                "Number of Animals": {"type": "integer"},
                                "Direction of Travel": {"type": "string"},
                                "Observation": {"type": "string"},
                                "Males": {"type": "string"},
                                "Links": {"type": "string"},
                                "Reported By": {"type": "string"},
                                "Raw_Text": {"type": "string"}
                            },
                        }
                    }
                },
                "required": ["sighting"],
                "additionalProperties": False
            }
        }
        }
    )
    return response.choices[0].message

final_results = []

for i in range(0,len(md_list_incremental)):
  user_prompt = f"""
  Here are the sightings reported in markdow, occurring in same day, each line starting with ** represents a sighting and the person reporting name starts with -:
  {md_list_incremental[i]}"""

  # Call the function on each data point
  result = parse_data(user_prompt)

  # Parse the JSON
  parsed_content = json.loads(result.content)
  data = json.dumps(parsed_content, indent=4)
  final_results.append(data)
  print(f"Processed {i}")


# Convert to polars dataframe
sightings = []

for sighting in final_results:
  d = json.loads(sighting)
  for sighting_2 in d['sighting']:
    sightings.append(sighting_2)
# Now create the DataFrame
sightings_df = pl.DataFrame(sightings)

# Data Cleaning

# Handling out of index time inference from the LLM model
#
# example cases:
# 13:00 PM; 14:35 PM

for i in sightings:
  time = i['Time']
  time = time.split(' ')
  if (i['Time'].isalpha()) or ('-' in i['Time']) or ('N/A' in i['Time']) or ('All Day' in i['Time']):
    i['Time'] = '00:00'
    continue
  try:
    if (time[1] == 'AM' or time[1] == 'PM'):
      t = time[0].split(':')
      if int(t[0]) > 12:
        i['Time'] = time[0]
  except:
      continue
  
# Categorizing the species
for i in sightings:
  species = i['Species'].lower().strip()
  category = ''
  if 'gray' in species:
    category = 'Gray whale'
  elif 'humpback' in species:
    category = 'Humpback whale'
  elif 'bottlenose' in species:
    category = 'Bottlenose whale'
  elif 'sperm' in species:
    category = 'Sperm whale'
  elif 'killer' in species:
    category = 'Killer whale (Orca)'
  elif 'orca' in species:
    category = 'Killer whale (Orca)'
  elif 'nurse' in species:
    category = 'Nurse whale'
  elif 'bigg' in species:
    category = "Bigg's killer whale"
  else:
    category = i['Species']
  i['Category'] = category

# Convert 'Number of Animals' to string for consistency
for i in sightings:
    i['Number of Animals'] = str(i['Number of Animals'])  


#   Upload Data to MotherDuck

# Initiate a MotherDuck connection using an access token
con = duckdb.connect(f'md:orca_db?motherduck_token={MOTHERDUCK_TOKEN}')

# create the DataFrame
sightings_df = pl.DataFrame(sightings)

# Create intermediate tables and append the data
con.sql("create or replace table orca_sightings_raw_intermediate as select * from sightings_df")

# Clean the data and load to output table
query = SQLstatements.data_cleaning_incremental
con.sql(query)

# Merge the intermediate table to the main table
query = SQLstatements.merge_intermediate_tables
con.sql(query)

# Upload to google sheets
gc = gspread.service_account(filename='access.json')

export_df = con.sql("select * from orca_sightings_cleaned").df()
sh= gc.open_by_key(GOOGLE_SHEETS_DOCUMENT_ID)
wrk = sh.get_worksheet(0)

old_data = get_as_dataframe(wrk)
updated_data = old_data.append(export_df)
set_with_dataframe(worksheet=wrk, dataframe=updated_data, include_index=False,
include_column_header=True, resize=True)