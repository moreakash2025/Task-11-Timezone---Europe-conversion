

# Task-11-Timezone---Europe-conversion

Timezone Conversion with Daylight Saving Time (DST) in Spark
Objective:
Convert timestamps from UTC to various European time zones, taking into account daylight saving time (DST) to display both the local time with and without DST.

Steps:
Initialize Spark Session:

Set up a Spark session to process the data.
Load Data:

Read sales data from a CSV file into a Spark DataFrame with a predefined schema to ensure proper data typing.
Convert to Timestamp:

Convert the ModifiedDate column from a string to a timestamp format for accurate time-based operations.
Create Time Zone Data:

Define a static list of European time zones, along with their respective local time designations with and without DST. This includes zones like Europe/London, Europe/Berlin, Europe/Paris, and others.
Convert Time Zones:

Use from_utc_timestamp() to convert the ModifiedDate from UTC to local time for each specified time zone.
Create two new columns for each time zone: one for local time with DST and another for local time without DST.
Determine Daylight Saving Status:

Use the dayofyear() function to extract the day of the year from the timestamp.
Apply a simple DST rule, assuming DST is active between day 60 and day 300 of the year. This adds a new column Is_DST_On to indicate whether DST is currently active.
Show Results:

Display the DataFrame with added columns for the local times in various time zones and a flag indicating if DST is on or off.
Key Considerations:
Time Zones:
European time zones differ in their observance of DST and have different local time designations, such as GMT/BST for London and CET/CEST for Central European regions.
DST Calculation:
The code assumes a simple DST range between day 60 and day 300. In real-world applications, a more accurate method may be required to handle different rules for each timezone or year.





