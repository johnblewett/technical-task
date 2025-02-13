# README.md
# Technical Task

This repo contains two Python scripts: 
- ingestRawFunction, which finds new files in the Statsbomb open-data repo and pushes it to an S3 bucket
- transformFunction, which moves the data from S3, processes it, and pushes it to an Amazon RDS PostgreSQL database

## ingestRawFunction
This script locates new files based on the repo commit history. The logic/flow is as follows:
- Read in the date of the last processed commit from process_date.txt in the S3 bucket.
- Get a list of all commits, and filter out any before the last processed commit date.
- Iterate through the commits, returning a list of files modified or added for each commit.
- Get competitions.json for that commit; subset for competition_id and season_id.
- Build data/match paths using the competition_id and season_id, returning a DataFrame competition_id, season_id and match_id.
- Build S3 paths for data upload using the DataFrame, and match against the relevant data/event files changed in the commit.
- Transfer data from the GitHub path to the S3 bucket path, and writes their paths to a text file also stored on S3.

Since the open-data repo is not currently being updated live, I simulated finding new data by looping through old commits. If the repo was being updated frequently, then I would just search for commits that have taken place in the last two hours.

Generally, this would have been much easier if the events files contained the competition_id, season_id and match_id, as this would have allowed for much quicker identification of the relevant S3 path to store the file at. Consequently, every time the script runs, it recollects competitions.json and regenerates the event paths.

The script is running as an AWS Lambda function, triggered by EventBridge. It has permissions to access objects in S3 and write logs to CloudWatch. Currently, the schedule is every 10 minutes since this allowed me to more easily verify the function is working, as well as pushing more data to S3, but this is easily editable to two hours.

## transformFunction
This script gets newly arrived files from S3 and transforms them for loading to RDS. The logic/flow is as follows:
- Grab the list of new files contained in the S3 bucket, and extract the data.
- Filter into four separate DataFrames (shots, passes, tackles, and fouls) based on the type id of each event entry.
- Clean them - ensure the columns don't contain any bad characters for PostgreSQL fields, added any columns that were missing etc.
- Connect to RDS using SQLAlchemy.
- Perform checks for each DataFrame: the relevant tables exist (and create if not), ensure table and DataFrame have the same columns, delete any event IDs in the table that exist in the DataFrame (to prevent duplication).
- Append DataFrame to table.

This function proved to be more of a challenge, primarily due to the limitations of loading packages in Lambda functions. The script works locally, but I am unable to run it through Lambda. In this case, if I had more time and greater familiarity with AWS, I would have liked to try running it through Glue. The trigger would be either the completion of the previous Lambda function or a modification to the S3 log file.

The schema was generated from the DataFrame data types: these were mapped against SQL types to create the relevant columns. This is potentially risky if the .json files are not well controlled/standardised, but I wasn't able to go through each table and set the schema manually. However, the .json files do seem to be consistent enough that this hasn't caused an issue.

Some examples of simple SQL queries that could be run on the tables are:

Get all goals scored after the 75th minute:
  SELECT * FROM shots
  WHERE shot_outcome_id = 96
  AND minute > 74

Get all occasions a player lost possession from a throw-in:
  SELECT * FROM tackles
  WHERE type_id = 3
  AND play_pattern_id = 4

Get the number of fouls committed by players when they were under pressure:
  SELECT COUNT(*) FROM fouls
  WHERE under_pressure = 'true'
  AND type_id = 22

# Ideal Technical Architecture

## Ingestion layer
The method for ingestion depends on the use-case of the specific data in question. As I have now discovered, setup and running simple process via Lambda is very easy out-of-the-box, allowing for fast response to real-time triggers. Since it's fully managed, it can also remove some overhead of managing servers etc. An example could be quickly accessing statistical data with regards to a specific player in game.

However, in the case of a heavier task, such as the technical task, Glue would have likely been a preferable option due to the more structured and complex nature of the task. Glue offers a more complete service than Lambda, containing its own triggers, jobs and catalog. In the case of the technical task, if I had some more knowledge of AWS when I began the task, I would have likely preferred Glue since it provides a more complete, end-to-end solution.

Airflow is possibly less useful in this instance. Unlike Glue, Airflow is not serverless, which means that configuration/setup/management provide a higher bar for entry. Consequently, Airflow provides more freedom but is harder to use for consistent ETL pipelines, especially considering Glue allows for easier integration with other AWS tools.

Kinesis provides real-time data streaming - likely very beneficial for tracking timeseries information such as player heartbeat, movement, etc. Lambda could be used for some light processing of near-live data from Kinesis, but generally they serve slightly different purposes.

There's likely a use-case for each of these tools, but I prefer Glue for quick setup of relatively more complex pipelines rather than Airflow, if only due to the fact it's easier to use straight-away and connects easily with other AWS services - although this may not be the case in a larger team able to dedicate more time to process management. Lambda is preferable for small, event-driven tasks due to its very lightweight nature - as I unfortunately discovered to my detriment.

## Storage layer
As with the ingestion layer, S3, Redshift and RDS serve different purposes. S3 is preferable for developing a data lake and as the point to bring data into the AWS ecosystem, since it handles both structured and unstructured data. Due to the lack of rigid structure, it is also ideal for storing raw data long-term, such that analysts can come back to it at a later date, rather than restructuring it into some format which loses some data.

Conversely, Redshift and RDS are much harder to maintain due to requiring strictly structured data, often requiring a lot of pre-processing. On the other hand, RDS and Redshift are simpler to understand on a casual level due to their relational nature (also benefitting from easy use of SQL - although Athena can make SQL queries to S3, I think this is trickier to handle due to the more varied nature of data in S3). Redshift and RDS are therefore more beneficial for consistently repeated and repeatable use-cases, rather than data science or machine learning workflows.

## Processing layer
Glue and Batch can fulfil similar purposes, although again Glue is probably easier to use straight away, since it does't have the customisability. Again, Glue is optimised for ETL workloads, integrates with S3, Redshift etc., and requires no infrastructure maintenance, but for tasks such as machine learning, Batch's customisability (Docker, control over compute and memory resources) makes it preferable.

Athena is designed specifically for querying S3 - but this is only really helpful if S3 is recieving relatively structured data. If S3 is being used as an entrypoint to the ecosystem, then Athena is probably less useful. If however, you keep metadata and structured data in S3 as well as unstructured data, then Athena could be useful.

## Analytics and visualisation
I prefer Tableau to Quicksight. Quicksight is cheaper, but Tableau provides a lot more customisability with regards to visualisations and the range of usable data sources. Tableau also has a larger user community, meaning it's much easier to find help or make use of other people's solutions.

## Security and governance
Glue Data Catalog allows for metadata management and data governance, keeping track of what data is where and tracking where data has been previously. It should be used together with IAM, to ensure that users don't access data they shouldn't  and track which users have permissions to interact with various AWS services. CloudTrail is complimentary, logging API calls and user actions, allowing admins to see which users interacted with what data. CloudWatch, on the other hand, provides logging and alarms to detect system performance issues and aid operational monitoring and troubleshooting. These tools in particular are not one or the other, but all central to maintaining data security.

## Proposed architecture
Raw data is read from source to an S3 datalake. S3 scales well and can handle large amounts of data effectively, so is ideal for this use. If the data is being stored completely raw, then a Lambda function is the best method for this. Minor requests (i.e., requsts which require little processing) should be handled through Lambda functions. Data for consistent, long-term metrics should be processed using Glue - I prefer this to Batch, due ot the ease of setup and configuration, although Batch is preferable for data science functions due to its customisability and resource provision - to a relational database (Redshift is preferable for analysis performed over the whole dataset since it's column-based and designed for large bulk read/writes). Any visualisation should ideally be performed on warehoused, structured data in Tableau, due to the greater collaborativeness and customisability.
