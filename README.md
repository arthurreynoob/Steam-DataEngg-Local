# Steam DataEngineering Project (Local Setup)

# Summary
This project extracts application/game data directly from the steam webstore API and SteamSPY API which are loaded in postgreSQL locally with some initial transformations performed on them. 

# SteamWebstore API
This is the official API of steam with up to date data of the games and application they currently offer in their store. Current location is also taken into account when downloading the data, for example the price overview of the game is expressed in the users local currency. The data of each application is downloaded individually using requests module. With the request limit of only 200 requests every 5 minutes, downloading data of over 50,000+ games would take a considerable amount of time. 

# SteamSpy API
SteamSpy is a Steam stats-gathering service and crucially has data easily available through its own API. This api is also based off from the SteamWebstore API but there are additional metrics included in their data, such as estimation for total owners of each game and peak concurrent user count of the previous day. Also, data of games in this api are compiled in pages, so the download of data is done per page, with each page containing data for 1000 games. 

# Tools 
Languages - Python, SQL
Transformation - Pandas, PySpark
Data Warehouse - PostgreSQL
Orchestration - Airflow
Containerization - Docker

![Alt text](images/ETL%20Flowchart.png)
