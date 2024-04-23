import json
import datetime
from azure.cosmos import CosmosClient, PartitionKey
import pyodbc
import os
from dotenv import load_dotenv
import argparse


class processHelper():
    def __init__(self):
        self.count = 0

    def increment(self, object):
        self.count += 1
        if self.count % 100 == 0:
            self.outputMessage(f"{object}s Written to Cosmos: {self.count}", "verbose")

    def reset(self, object):
        self.outputMessage(f"Total {object}s Written to Cosmos: {self.count}","success")
        self.count = 0
       
    def outputMessage(self, message, level):
    
        if level == "error":
            color = "\033[91m" # Red
        elif level == "success":
            color = "\033[92m" # Green
        elif level == "info":
            color = "\033[94m" # Blue
        elif level == "warning":
            color = "\033[93m"  # Yellow
        elif level == "debug":
            color = "\033[95m"  # Purple
        elif level == "verbose":
            color = "\033[96m"  # Cyan
        else:
            color = "\033[0m" # white

        print(f"{str(datetime.datetime.now())} - {color}{message}")
        print("\033[0m", end="") # Reset color

class cosmosClient:
    def __init__(self, process: processHelper):
        self.cosmos_url = os.getenv("COSMOS_URL")
        self.cosmos_key = os.getenv("COSMOS_KEY")
        self.cosmos_db = os.getenv("COSMOS_DATABASE")
        self.cosmos_container = ""
        self._process = process
            
    def getClient(self):
        self.client = CosmosClient(self.cosmos_url, credential=self.cosmos_key)
        self.database = self.client.get_database_client(self.cosmos_db)
        self.container = self.database.get_container_client(self.cosmos_container)
    
    def databaseClient(self):
        self.client = CosmosClient(self.cosmos_url, credential=self.cosmos_key)
        self.database = self.client.get_database_client(self.cosmos_db)

    def singleContainer(self):
        self.cosmos_container = os.getenv("COSMOS_CONTAINER_MEDIA_SINGLE")
    
    def embeddedContainer(self):
        self.cosmos_container = os.getenv("COSMOS_CONTAINER_MEDIA_EMBEDDED")
    
    def referenceContainer(self):
        self.cosmos_container = os.getenv("COSMOS_CONTAINER_MEDIA_REFERENCE")
    
    def hybridContainer(self):
        self.cosmos_container = os.getenv("COSMOS_CONTAINER_MEDIA_HYBRID")

    def WriteToCosmos(self, movie):
        self.container.upsert_item(movie)

    #TODO this needs to be a list of containers and not in the .env file and long form here, likely part of the class.
    def CleanDatabaseContainers(self):
        container_list = ['Single', 'Embedded', 'Reference', 'Hybrid']
        for container in container_list:
            try:
                self.database.delete_container(container)
                self._process.outputMessage(f"Deleted {container} Container", "success")
            except Exception as e:
                self._process.outputMessage(f"Error: {e}", "error")
    
    def CreateDatabaseContainers(self):
        container_list = ['Single', 'Embedded', 'Reference', 'Hybrid']
        for container in container_list:
            try:
                self.database.create_container(container, partition_key=PartitionKey(path="/title"))
                self._process.outputMessage(f"Created {container} Container", "success")
            except Exception as e:
                self._process.outputMessage(f"Error: {e}", "error")


class lastDate:
    def __init__(self, cosmos_client, process: processHelper):
        self.date = datetime.date(1900, 1, 1)
        self._cosmos_client = cosmos_client
        self._process = process

    def get_date(self):
        query = "SELECT VALUE MAX(c.release_date) FROM c"
        max_release_date = list(self._cosmos_client.container.query_items(
            query=query,
            enable_cross_partition_query=True
        ))[0]
        self.date = max_release_date if max_release_date else self.date
        self._process.outputMessage(f"Most Recent Movie Release Date in Cosmos: {last_date.date}","info")

class getSqlClient:
    def __init__(self):
        self.sql_server = os.getenv("SQL_SERVER")
        self.sql_database = os.getenv("SQL_DATABASE")
        self.sql_username = os.getenv("SQL_USERNAME")
        self.sql_password = os.getenv("SQL_PASSWORD")

    def connect(self):
        connection_string = f"Driver={{ODBC Driver 17 for SQL Server}};Server=tcp:{self.sql_server}.database.windows.net,1433;Database={self.sql_database};UID={self.sql_username};PWD={self.sql_password}"
        self.client = pyodbc.connect(connection_string)
        self.cursor = self.client.cursor()
    
    def close(self):
        self.client.close()

class mediaBuilder:
    def __init__(self, sql_client, last_date):
        self._sql_client = sql_client
        self._last_date = last_date

    def SingleMovieList(self):
        self._sql_client.cursor.execute(f"exec [cosmos].[GetMoviesJson] '{last_date.date}'")
        return self._sql_client.cursor.fetchall()
    
    def EmbeddedActorsList(self):
        self._sql_client.cursor.execute(f"exec [cosmos].[GetActorsEmbeddedJson] '{last_date.date}'")
        return self._sql_client.cursor.fetchall()
    
    def EmbeddedDirectorsList(self):
        self._sql_client.cursor.execute(f"exec [cosmos].[GetDirectorsEmbeddedJson] '{last_date.date}'")
        return self._sql_client.cursor.fetchall()
    
    def ReferenceActorsList(self):
        self._sql_client.cursor.execute(f"exec [cosmos].[GetActorsReferenceJson] '{last_date.date}'")
        return self._sql_client.cursor.fetchall()
    
    def ReferenceDirectorsList(self):
        self._sql_client.cursor.execute(f"exec [cosmos].[GetDirectorsReferenceJson] '{last_date.date}'")
        return self._sql_client.cursor.fetchall()
    
    # Based upon last movie processed date, find all actors starring in new movies
    def NewMovieActors(self):
        self._sql_client.cursor.execute(f"exec [cosmos].[GetNewMoviesActors] '{last_date.date}'")
        return self._sql_client.cursor.fetchall()
    
    # Based upon last movie processed date, find all directors directing new movies
    def NewMovieDirectors(self):
        self._sql_client.cursor.execute(f"exec [cosmos].[GetNewMoviesDirectors] '{last_date.date}'")
        return self._sql_client.cursor.fetchall()
    
    def HybridActorList(self, actor_id):
        self._sql_client.cursor.execute(f"exec [cosmos].[GetActorsHybridJson] {actor_id}")
        return self._sql_client.cursor.fetchall()

    def HybridDirectorList(self, director_id):
        self._sql_client.cursor.execute(f"exec [cosmos].[GetDirectorsHybridJson] {director_id}")
        return self._sql_client.cursor.fetchall()
    
    def close_connection(self):
        self.conn.close()


process = processHelper()

# Load environment variables      
load_dotenv()

# For command line arguments
parser = argparse.ArgumentParser(description="Provide various run commands.")
# Argument for verbose mode, to display object outputs
parser.add_argument("-r","--rebuild", action='store_true', help="Rebuild Cosmos Containers")
parser.add_argument("-d","--setdate", help="Allows to set the last date rather than relying on the last date in Cosmos. Format: 'YYYY-MM-DD'")
args = parser.parse_args()

if args.rebuild:
    # Get user input to rebuild cosmos database and containers
    confirm = input("Are you sure you want to rebuild Modeling Containers? (Y/N): ")
    if confirm.lower() == 'y':
        process.outputMessage("Rebuilding Cosmos Modeling Containers","info")
        cosmos_client = cosmosClient(process)
        cosmos_client.databaseClient()
        cosmos_client.CleanDatabaseContainers()
        cosmos_client.CreateDatabaseContainers()
        process.outputMessage("Cosmos Modeling Containers Rebuilt","success")
    else:
        exit(process.outputMessage("Rebuild and Excution Cancelled","warning"))


# Spin up all my cosmos clients for different model containers
cosmos_single_client = cosmosClient(process)
cosmos_single_client.singleContainer()
cosmos_single_client.getClient()

cosmos_embedded_client = cosmosClient(process)
cosmos_embedded_client.embeddedContainer()
cosmos_embedded_client.getClient()

cosmos_reference_client = cosmosClient(process)
cosmos_reference_client.referenceContainer()
cosmos_reference_client.getClient()

cosmos_hybrid_client = cosmosClient(process)
cosmos_hybrid_client.hybridContainer()
cosmos_hybrid_client.getClient()


# Allow user to set the last date to process
last_date = lastDate(cosmos_single_client, process)
if args.setdate:
    # Check if the date is in pattern YYYY-MM-DD
    try:
        passed_date = datetime.datetime.strptime(args.setdate, '%Y-%m-%d')
    except ValueError:
        exit(process.outputMessage("Date is not in the correct format. Please use YYYY-MM-DD","error"))
    last_date.date = passed_date
else:
    last_date.get_date()

sql_client = getSqlClient()
sql_client.connect()

# Getting all movies since date and writing to cosmos containers, as the movies will always have the same model.
process.outputMessage("Writing Movies to Cosmos","text")
sql_media = mediaBuilder(sql_client, last_date)
for item in sql_media.SingleMovieList():
    movie = json.loads(item.value)
    try:
        cosmos_single_client.WriteToCosmos(movie)
        process.increment("Movie")
        cosmos_embedded_client.WriteToCosmos(movie)
        process.increment("Movie")
        cosmos_reference_client.WriteToCosmos(movie)
        process.increment("Movie")
        cosmos_hybrid_client.WriteToCosmos(movie)
        process.increment("Movie")
    except Exception as e:
        process.outputMessage(f"Error: {e}", "error")
        break
process.reset("Movie")

process.outputMessage("Writing Embedded Actors and Directors to Cosmos","text")
# Writing embedded actors to cosmos embedded container
for item in sql_media.EmbeddedActorsList():
    movie = json.loads(item.value)
    try:
        cosmos_embedded_client.WriteToCosmos(movie)
        process.increment("Embedded Actor")
    except Exception as e:
        process.outputMessage(f"Error: {e}", "error")
        break
process.reset("Embedded Actor")

# Writing embedded directors to cosmos embedded container
for item in sql_media.EmbeddedDirectorsList():
    movie = json.loads(item.value)
    try:
        cosmos_embedded_client.WriteToCosmos(movie)
        process.increment("Embedded Director")
    except Exception as e:
        process.outputMessage(f"Error: {e}", "error")
        break
process.reset("Embedded Director")

process.outputMessage("Writing Reference Actors and Directors to Cosmos","text")
# Writing reference actors to cosmos reference container
for item in sql_media.ReferenceActorsList():
    movie = json.loads(item.value)
    try:
        cosmos_reference_client.WriteToCosmos(movie)
        process.increment("Reference Actor")
    except Exception as e:
        process.outputMessage(f"Error: {e}", "error")
        break
process.reset("Reference Actor")

# Writing reference directors to cosmos reference container
for item in sql_media.ReferenceDirectorsList():
    movie = json.loads(item.value)
    try:
        cosmos_reference_client.WriteToCosmos(movie)
        process.increment("Reference Director")
    except Exception as e:
        process.outputMessage(f"Error: {e}", "error")
        break
process.reset("Reference Director")

# Writing hybrid actors and directors to cosmos hybrid container
process.outputMessage("Writing Hybrid Actors and Directors to Cosmos","text")
for actor in sql_media.NewMovieActors():
    try:
        actor_document = sql_media.HybridActorList(actor.actor_id)
        for item in actor_document:
            movie = json.loads(item.value)
            cosmos_hybrid_client.WriteToCosmos(movie)
            process.increment("Hybrid Actor")
    except Exception as e:
        process.outputMessage(f"Error: {e}", "error")
        break
process.reset("Hybrid Actor")

for director in sql_media.NewMovieDirectors():
    try:
        director_document = sql_media.HybridDirectorList(director.director_id)
        for item in director_document:
            movie = json.loads(item.value)
            cosmos_hybrid_client.WriteToCosmos(movie)
            process.increment("Hybrid Director")
    except Exception as e:
        process.outputMessage(f"Error: {e}", "error")
        break
process.reset("Hybrid Actor")

# Close the connection
sql_client.close()