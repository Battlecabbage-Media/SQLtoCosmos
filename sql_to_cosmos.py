import json
import datetime
from azure.cosmos import CosmosClient
import pyodbc
import os
from dotenv import load_dotenv


class cosmosClient:
    def __init__(self):
        self.cosmos_url = os.getenv("COSMOS_URL")
        #self.cosmos_key = os.getenv("COSMOS_KEY")
        self.cosmos_key = "sntaGosEjCWmUsYOg6MJKZFraFiT8f3VejLVQ8lDgV6mIewRdpamKuxqI1ptMFyZ07o47mxntStcACDbHV7Rbg=="
        self.cosmos_db = os.getenv("COSMOS_DATABASE")
        self.cosmos_container = ""
        
    def getClient(self):
        self.client = CosmosClient(self.cosmos_url, credential=self.cosmos_key)
        self.database = self.client.get_database_client(self.cosmos_db)
        self.container = self.database.get_container_client(self.cosmos_container)
    
    def WriteToCosmos(self, movie):
        self.container.upsert_item(movie)

class cosmosClientSingle(cosmosClient):
    def __init__(self):
        super().__init__()
        self.cosmos_container = os.getenv("COSMOS_CONTAINER_MEDIA_SINGLE")
        
class cosmosClientEmbedded(cosmosClient):
    def __init__(self):
        super().__init__()
        self.cosmos_container = os.getenv("COSMOS_CONTAINER_MEDIA_EMBEDDED")

class cosmosClientReference(cosmosClient):
    def __init__(self):
        super().__init__()
        self.cosmos_container = os.getenv("COSMOS_CONTAINER_MEDIA_REFERENCE")

class cosmosClientHybrid(cosmosClient):
    def __init__(self):
        super().__init__()
        self.cosmos_container = os.getenv("COSMOS_CONTAINER_MEDIA_HYBRID")

class processHelper():
    def __init__(self):
        self.count = 0

    def increment(self, object):
        self.count += 1
        if self.count % 100 == 0:
            print(f"{object}s Written to Cosmos: {self.count}")

    def reset(self, object):
        print(f"Total {object}s Written to Cosmos: {self.count}")
        self.count = 0

class lastDate:
    def __init__(self, cosmos_client):
        self.date = datetime.date(1900, 1, 1)
        self._cosmos_client = cosmos_client

    def get_date(self):
        query = "SELECT VALUE MAX(c.release_date) FROM c"
        max_release_date = list(self._cosmos_client.container.query_items(
            query=query,
            enable_cross_partition_query=True
        ))[0]
        self.date = max_release_date if max_release_date else self.date
        print(f"Most Recent Movie Release Date in Cosmos: {last_date.date}")

class getSqlClient:
    def __init__(self):
        self.sql_server = os.getenv("SQL_SERVER")
        self.sql_database = os.getenv("SQL_DATABASE")
        self.sql_username = os.getenv("SQL_USERNAME")
        self.sql_password = os.getenv("SQL_PASSWORD")

    def connect(self):
        #print(f"server: {self.sql_server}, database: {self.sql_database}, username: {self.sql_username}, password: {self.sql_password}")
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
    

    def close_connection(self):
        self.conn.close()

#os.environ.clear()
load_dotenv()

# Spin up all my cosmos clients for different model containers
cosmos_single_client = cosmosClientSingle()
cosmos_single_client.getClient()
cosmos_embedded_client = cosmosClientEmbedded()
cosmos_embedded_client.getClient()
cosmos_reference_client = cosmosClientReference()
cosmos_reference_client.getClient()
cosmos_hybrid_client = cosmosClientHybrid()
cosmos_hybrid_client.getClient()

last_date = lastDate(cosmos_single_client)
last_date.get_date()

sql_client = getSqlClient()
sql_client.connect()

counter = processHelper()

# Getting all movies since date and writing to cosmos containers, as the movies will always have the same model.
print("Writing Movies to Cosmos")
sql_media = mediaBuilder(sql_client, last_date)
for item in sql_media.SingleMovieList():
    movie = json.loads(item.value)
    try:
        cosmos_single_client.WriteToCosmos(movie)
        counter.increment("Movie")
        cosmos_embedded_client.WriteToCosmos(movie)
        counter.increment("Movie")
        cosmos_reference_client.WriteToCosmos(movie)
        counter.increment("Movie")
        cosmos_hybrid_client.WriteToCosmos(movie)
        counter.increment("Movie")
    except Exception as e:
        print(f"Error: {e}")
        break
counter.reset("Movie")

print("Writing Embedded Actors and Directors to Cosmos")
# Writing embedded actors to cosmos embedded container
for item in sql_media.EmbeddedActorsList():
    movie = json.loads(item.value)
    try:
        cosmos_embedded_client.WriteToCosmos(movie)
        counter.increment("Embedded Actor")
    except Exception as e:
        print(f"Error: {e}")
        break
counter.reset("Embedded Actor")

# Writing embedded directors to cosmos embedded container
for item in sql_media.EmbeddedDirectorsList():
    movie = json.loads(item.value)
    try:
        cosmos_embedded_client.WriteToCosmos(movie)
        counter.increment("Embedded Director")
    except Exception as e:
        print(f"Error: {e}")
        break
counter.reset("Embedded Director")

print("Writing Reference Actors and Directors to Cosmos")
# Writing reference actors to cosmos reference container
for item in sql_media.ReferenceActorsList():
    movie = json.loads(item.value)
    try:
        cosmos_reference_client.WriteToCosmos(movie)
        counter.increment("Reference Actor")
    except Exception as e:
        print(f"Error: {e}")
        break
counter.reset("Reference Actor")

# Writing reference directors to cosmos reference container
for item in sql_media.ReferenceDirectorsList():
    movie = json.loads(item.value)
    try:
        cosmos_reference_client.WriteToCosmos(movie)
        counter.increment("Reference Director")
    except Exception as e:
        print(f"Error: {e}")
        break
counter.reset("Reference Director")

#print("Writing Hybrid Actors and Directors to Cosmos")
# # Writing hybrid actors to cosmos hybrid container
# for item in sql_media.HybridActorList():
#     movie = json.loads(item.value)
#     try:
#         cosmos_hybrid_client.WriteToCosmos(movie)
#         counter.increment("Hybrid Actor")
#     except Exception as e:
#         print(f"Error: {e}")
#         break
# counter.reset("Hybrid Actor")

# # Writing hybrid directors to cosmos hybrid container
# for item in sql_media.HybridDirectorList():
#     movie = json.loads(item.value)
#     try:
#         cosmos_hybrid_client.WriteToCosmos(movie)
#         counter.increment("Hybrid Director")
#     except Exception as e:
#         print(f"Error: {e}")
#         break
# counter.reset("Hybrid Director")

# Close the connection
sql_client.close()