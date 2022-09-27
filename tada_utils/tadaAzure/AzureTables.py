from azure.identity import ClientSecretCredential
from azure.data.tables import TableServiceClient
from azure.core.exceptions import HttpResponseError
import logging
logging.getLogger("py4j.java_gateway").setLevel(logging.ERROR)

class AzureTablesManager():
    def __init__(self, storageAccountName, dbutils):
        self.clientId = dbutils.secrets.get(scope = 'data-dtc-scope', key = 'ApplicationId')
        self.tenantId = dbutils.secrets.get(scope = 'data-dtc-scope', key = 'TenantId')
        self.secret = dbutils.secrets.get(scope = 'data-dtc-scope', key = 'SecretId')
        self.credential = ClientSecretCredential(self.tenantId, self.clientId, self.secret)
        self.azureSession = TableServiceClient(endpoint="https://{}.table.core.windows.net/".format(storageAccountName), credential = self.credential)
    
    def createTable(self, tableName):
        try:
            self.azureSession.create_table_if_not_exists(tableName)
            logging.info(f"table: {tableName} created successful")
        except HttpResponseError as e:
            raise e

    def deleteTable(self, tableName):
        try:
            self.azureSession.delete_table(tableName)
            logging.info(f"table: {tableName} deleted successful")
        except HttpResponseError as e:
            raise e
    
    def listTables(self):
        try:
            tables = self.azureSession.list_tables()
            tableList = [item.name for item in tables]
            logging.info("querying all existing tables")
            return tableList
        except HttpResponseError as e:
            raise e
    
    def listEntity(self, tableName):
        try:
            tableClient = self.azureSession.get_table_client(tableName)
            entity = tableClient.list_entities()
            entityList = [item for item in entity]
            return entityList
        except HttpResponseError as e:
            raise e
    
    def deleteEntity(self, tableName, partitionKey, rowKey):
        try:
            tableClient = self.azureSession.get_table_client(tableName)
            tableClient.delete_entity(partitionKey, rowKey)
            logging.info("entity deleted")
        except HttpResponseError as e:
            raise e
    
    def createEntity(self, tableName, partitionKey, rowKey, data):
        try:
            data['PartitionKey'] = partitionKey
            data['RowKey'] = rowKey
            tableClient = self.azureSession.get_table_client(tableName)
            tableClient.create_entity(entity = data)
            logging.info("entity created")
        except HttpResponseError as e:
            raise e
    
    def getEntity(self, tableName, partitionKey, rowKey):
        try:
            tableClient = self.azureSession.get_table_client(tableName)
            entity = tableClient.get_entity(partitionKey, rowKey)
            logging.info("query successfull")
            return dict(entity)
        except HttpResponseError as e:
            raise e
    
    def queryEntity(self, tableName, partitionKey, rowKey = None):
        try:
            if rowKey != None:
                query = f"PartitionKey eq '{partitionKey}' and RowKey eq '{rowKey}'"
            else:
                query = f"PartitionKey eq '{partitionKey}'"
            tableClient = self.azureSession.get_table_client(tableName)
            entities = tableClient.query_entities(query)
            listEntities = [item for item in entities]
            return listEntities
        except HttpResponseError as e:
            raise e