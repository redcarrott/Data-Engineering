# Project 3: Understanding User Behavior

- You're a data scientist at a game development company  

- Your latest mobile game has two events you're interested in tracking: `buy an ingredient`, `join a restaurant`, & `enter a contest`

- Each has metadata characterstic of such events (i.e., ingredient type, restaurant name,
  etc)


## Objective

Track user behavior in stream mode and prepare the infrastructure to land data in appropriate form and structure to be queried by data scientists. This process includes the following:

- Instrument the API server to log events into Kafka
- Assemble data pipeline: use Spark streaming to filter and select event types from Kafka, land into HDFS/Parquet to make data available for analysis using Presto
- Use Apache Bench to generate test data for pipeline
- Produce analytics report for development and design teams with analysis of events

Useful Links: \
[Final Report](https://github.com/redcarrott/Data-Engineering/blob/main/projects/Understanding_User_Behavior/REPORT.ipynb) \
[Project instructions](https://github.com/redcarrott/Data-Engineering/blob/main/projects/Understanding_User_Behavior/instructions.md)

