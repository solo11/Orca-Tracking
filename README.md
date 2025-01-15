# Tracking Orcas — Harnessing the Power of LLMs and Data Engineering


Read the Medium post to know more about the project - [Medium article](https://solo11.medium.com/tracking-orcas-harnessing-the-power-of-llms-and-data-engineering-bcf0132bacc6)\
View the Tableau dashboard - [Tableau](https://public.tableau.com/app/profile/solomon8607/viz/OrcaSightings/Dashboard1)\
View the Google Sheets file - [Sheets](https://docs.google.com/spreadsheets/d/13YeY1GwIMU7pZhrbbBvKPTPAh6RptK7AH3P8c7EFKuY/edit?gid=1523387241#gid=1523387241)

## Architecture
![image](https://github.com/user-attachments/assets/49d84ac9-68f9-4902-a978-fcc82613fe82)



## Install packages 
``` pip install -r requirements.txt ```

## File structure
```full_load.py``` Run this python file for full load \
```incremental_load.py``` Run this python file for incremental load - By default is set to 1 day incremental load \
```SQLstatements.py``` Contains the SQL queries used 

## .env variables
MOTHERDUCK_TOKEN="MotherDuck token to access the database" \
SHEETS_DOCUMENT_ID="Google sheets document id to loading the output data"\
OPENAI_API_KEY="Open AI api key for function calling"
