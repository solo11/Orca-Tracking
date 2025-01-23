data_cleaning_full = """
CREATE OR REPLACE TABLE orca_sightings_cleaned AS
  (SELECT STRPTIME("date", '%m/%d/%Y') AS date,
          CASE
              WHEN "time" LIKE '%AM'
                   OR "time" LIKE '%PM' THEN STRFTIME(STRPTIME("time", '%I:%M %p'), '%H:%M') -- For 12-hour format with AM/PM

              ELSE STRFTIME(STRPTIME("time", '%H:%M'), '%H:%M') -- For 24-hour format

          END AS "Time",
          CASE
              WHEN trim(latitude) LIKE '%[a-zA-Z]%'
                   OR latitude IN ('Unknown',
                                   '',
                                   'TBD') THEN NULL -- Check if latitude contains letters or is Unknown/empty

              ELSE CAST(latitude AS Decimal(8, 6))
          END AS latitude,
          CASE
              WHEN trim(longitude) LIKE '%[a-zA-Z]%'
                   OR longitude IN ('Unknown',
                                    '',
                                    'TBD') THEN NULL -- Check if longitude contains letters or is Unknown/empty

              ELSE CAST(longitude AS Decimal(9, 6))
          END AS longitude,
          "Location",
          lower(trim(Species)) AS Species,
          Category,
          Males,
          "Reported By",
          Links,
          Observation,
          CASE
              WHEN "Number of Animals" = 'None'
                   OR "Number of Animals" = 'Unknown' THEN NULL
              ELSE CAST("Number of Animals" AS INT)
          END AS "Number of Animals",
          "Direction of Travel",
          Raw_Text
   FROM orca_sightings_raw)
"""

data_cleaning_incremental = """
CREATE OR REPLACE TABLE orca_sightings_cleaned_intermediate AS
  (SELECT STRPTIME("date", '%m/%d/%Y') AS date,
          CASE
              WHEN "time" LIKE '%AM'
                   OR "time" LIKE '%PM' THEN STRFTIME(STRPTIME("time", '%I:%M %p'), '%H:%M') -- For 12-hour format with AM/PM

              ELSE STRFTIME(STRPTIME("time", '%H:%M'), '%H:%M') -- For 24-hour format

          END AS "Time",
          CASE
              WHEN trim(latitude) LIKE '%[a-zA-Z]%'
                   OR latitude IN ('Unknown',
                                   '',
                                   'TBD') THEN NULL -- Check if latitude contains letters or is Unknown/empty

              ELSE CAST(latitude AS Decimal(8, 6))
          END AS latitude,
          CASE
              WHEN trim(longitude) LIKE '%[a-zA-Z]%'
                   OR longitude IN ('Unknown',
                                    '',
                                    'TBD') THEN NULL -- Check if longitude contains letters or is Unknown/empty

              ELSE CAST(longitude AS Decimal(9, 6))
          END AS longitude,
          "Location",
          lower(trim(Species)) AS Species,
          Category,
          Males,
          "Reported By",
          Links,
          Observation,
          CASE
              WHEN "Number of Animals" = 'None'
                   OR "Number of Animals" = 'Unknown' THEN NULL
              ELSE CAST("Number of Animals" AS INT)
          END AS "Number of Animals",
          "Direction of Travel",
          Raw_Text
   FROM orca_sightings_raw_intermediate)
"""

merge_intermediate_tables = """
insert into orca_sightings_raw select * from orca_sightings_raw_intermediate;
insert into orca_sightings_cleaned select * from orca_sightings_cleaned_intermediate;
drop table orca_sightings_cleaned_intermediate;
drop table orca_sightings_raw_intermediate;
"""
