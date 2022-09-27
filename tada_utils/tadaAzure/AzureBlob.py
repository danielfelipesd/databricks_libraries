from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient
import json

class AzureBlobManager():

    def __init__(self, storageAccountName, dbutils):
        self.clientId = dbutils.secrets.get(scope = 'data-dtc-scope', key = 'ApplicationId')
        self.tenantId = dbutils.secrets.get(scope = 'data-dtc-scope', key = 'TenantId')
        self.secret = dbutils.secrets.get(scope = 'data-dtc-scope', key = 'SecretId')
        self.credential = ClientSecretCredential(self.tenantId, self.clientId, self.secret)
        self.azureSession = BlobServiceClient(account_url="https://{}.blob.core.windows.net/".format(storageAccountName), credential=self.credential)
        
    def uploadFile(self, container, path, data):
        blob_client = self.azureSession.get_blob_client(container = container, blob = path)
        blob_client.upload_blob(data)
    
    def uploadJsonFile(self, container, path, data):
        blob_client = self.azureSession.get_blob_client(container = container, blob = path)
        blob_client.upload_blob(json.dumps(data, ensure_ascii=False).encode('utf8'))
    
    def listFiles(self, container, path):
        container_client = self.azureSession.get_container_client(container = container)
        blob_list = container_client.list_blobs(name_starts_with = path)
        listFiles = []
        for blob in blob_list:
            listFiles.append(blob.name)
        
        return listFiles
    
    def listFileNames(self, listFiles):
        fileNames = []
        for file in listFiles:
            total = file.count('/')
            name = file.split('/')[total]
            fileNames.append(name)
    
        return fileNames
    
    def copyFiles(self, containerFrom, pathFrom, pathTo, containerTo = None):
        listFile = self.listFiles(containerFrom, pathFrom)
        listName = self.listFileNames(listFile)

        for file, name in zip(listFile, listName):
            source_blob = self.azureSession.get_blob_client(container = containerFrom, blob = file)
            if containerTo != None:
                target_blob = self.azureSession.get_blob_client(container = containerTo, blob = f"{pathTo}/{name}")
            else:
                target_blob = self.azureSession.get_blob_client(container = containerFrom, blob = f"{pathTo}/{name}")
                
            target_blob.start_copy_from_url(source_blob.url)
                
    def moveFiles(self, containerFrom, pathFrom, pathTo, containerTo = None):
        listFile = self.listFiles(containerFrom, pathFrom)
        listName = self.listFileNames(listFile)

        for file, name in zip(listFile, listName):
            source_blob = self.azureSession.get_blob_client(container = containerFrom, blob = file)
            if containerTo != None:
                target_blob = self.azureSession.get_blob_client(container = containerTo, blob = f"{pathTo}/{name}")
            else:
                target_blob = self.azureSession.get_blob_client(container = containerFrom, blob = f"{pathTo}/{name}")
                
            target_blob.start_copy_from_url(source_blob.url)
            source_blob.get_blob_properties().copy
        
        for file in listFile:
            source_blob = self.azureSession.get_blob_client(container = containerFrom, blob = file)
            source_blob.delete_blob()
    
    def deleteFiles(self, container, path):
        listFile = self.listFiles(container, path)
        for file in listFile:
            blob = self.azureSession.get_blob_client(container = container, blob = file)
            blob.delete_blob()
            
            
    def readFile(self, container, path):
        source_blob = self.azureSession.get_blob_client(container = container, blob = path)
        return source_blob.download_blob().readall().decode("utf-8")
        
    def copyFiles(self, containerFrom, pathFrom, pathTo, containerTo = None):
        
        source_blob = self.azureSession.get_blob_client(container = containerFrom, blob = pathFrom)

        if containerTo != None:
            target_blob = self.azureSession.get_blob_client(container = containerTo, blob = pathTo)
        else:
            target_blob = self.azureSession.get_blob_client(container = containerFrom, blob = pathTo)

        target_blob.start_copy_from_url(source_blob.url)