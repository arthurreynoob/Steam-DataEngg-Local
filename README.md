# Steam DataEngineering Project (Local Setup)

## Summary
This project extracts application/game data directly from the steam webstore API and SteamSpy API which are loaded in postgreSQL locally with some initial transformations performed on them. 

## SteamWebstore API
This is the official API of [**Steam**](https://store.steampowered.com/) with up to date data of the games and application they currently offer in their store. Current location is also taken into account when downloading the data, for example the price overview of the game is expressed in the users local currency. The data of each application is downloaded individually using requests module. With the request limit of only 200 requests every 5 minutes, downloading data of over 50,000+ games would take a considerable amount of time. 

## SteamSpy API
[**SteamSpy**](https://steamspy.com/) is a Steam stats-gathering service and crucially has data easily available through its own API. This api is also based off from the SteamWebstore API but there are additional metrics included in their data, such as estimation for total owners of each game and peak concurrent user count of the previous day. Also, data of games in this api are compiled in pages, so the download of data is done per page, with each page containing data for 1000 games. 

## Tools 
* Languages - **Python**, **SQL**
* Transformation - [**Pandas**](https://pandas.pydata.org/docs/), [**Spark**](https://spark.apache.org/)
* Data Warehouse - [**PostgreSQL**](https://www.postgresql.org/)
* Orchestration - [**Airflow**](https://airflow.apache.org/)
* Containerization - [**Docker**](https://docs.docker.com/compose/)

## Architecture

The data are extracted from SteamWebstore API and SteamSpy API using requests module then stored locally. The stored data are then transformed using pyspark then loaded into PostgreSQL. This is orchestrated using Apache Airflow and contained using Docker.

![Alt text](images/ETL%20Flowchart.png)

The SteamSpy API DAG is extracted and stored/categorized in a daily basis. The Steamwebstore API is extracted and stored/categorized in a monthly basis since downloading from the steamwebstore takes a rather long time. Using only a local machine, I can not download the data in one sitting since I can not simultaneously download data and do my work tasks.

## Orchestration
There are 2 Dags, one for each API:

* **local_ingest_steamspy_dag** : The SteamSpy API DAG is extracted and stored/categorized in a daily basis. The raw files are in JSON then converted to parquet using pandas. The parquet files are stored locally. Then by using Pyspark, data is loaded into **PostgreSQL** as tables

* **local_ingest_steamapi_dag**: The Steamwebstore API is extracted and stored/categorized in a monthly basis since downloading from the steamwebstore takes a rather long time. Using only a local machine, I can not download the data in one sitting since I can not simultaneously download data and do my work tasks. The raw files are in JSON then converted to parquet using pandas. The parquet files are stored locally then these stored parquet files are transformed using pyspark. After transformation, they are loaded as tables in **PostgreSQL**


## Transformation
Transformation are performed on the SteamWebstore API data using **PySpark**. Main function of the transformation was to unnest some columns present in the data, such as: Price_Overview, Metacritic, Categories, Genres. New tables are created from the unnested columns. 


## Improvements
* Add Data visualization
* Apply more transformations
* Incorporate Cloud technology