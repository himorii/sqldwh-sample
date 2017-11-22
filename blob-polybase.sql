-- Create a master key on the database.  
-- Required to encrypt the credential secret.  

CREATE MASTER KEY ENCRYPTION BY PASSWORD = '<password>';  
CLOSE MASTER KEY

DROP MASTER KEY;

-- Create a database scoped credential  for Azure blob storage.  
-- IDENTITY: any string (this is not used for authentication to Azure storage).  
-- SECRET: your Azure storage account key.  
CREATE DATABASE SCOPED CREDENTIAL AzureStorageCredential WITH IDENTITY = '<StorageAccountName>', Secret = '<StorageAccessKey>';


-- Create an external data source.  
-- LOCATION:  Azure account storage account name and blob container name.  
-- CREDENTIAL: The database scoped credential created above.  

CREATE EXTERNAL DATA SOURCE AzureStorage with (  
        TYPE = HADOOP,   
        LOCATION ='wasbs://<StorageContainerName>@<StorageAccountName>.blob.core.windows.net',  
        CREDENTIAL = AzureStorageCredential  
);  


DROP EXTERNAL FILE FORMAT MSCSV  
CREATE EXTERNAL FILE FORMAT MSCSV
WITH 
(
    FORMAT_TYPE = DELIMITEDTEXT,
    FORMAT_OPTIONS
    (
        FIELD_TERMINATOR =',',
        STRING_DELIMITER = '0x22'
    )
);

DROP  EXTERNAL TABLE [asb].[kanko_annai] ;
CREATE EXTERNAL TABLE [asb].[kanko_annai] (  
        [large]  nvarchar(4000) ,   
        [middle] nvarchar(4000) , 
        [small]  nvarchar(4000) ,  
        [op] nvarchar(4000) ,  
        [null1] nvarchar(4000) ,  
        [null2] nvarchar(4000) ,  
        [null3]nvarchar(4000) ,  
        [null4]nvarchar(4000) ,  
        [null5] nvarchar(4000) ,  
        [value] nvarchar(4000)
)  
WITH (LOCATION='<StorageLocation(Relative path)>',   
        DATA_SOURCE = AzureStorage,  
        FILE_FORMAT = MSCSV  
);  

DROP EXTERNAL TABLE [asb].[warning];
CREATE EXTERNAL TABLE [asb].[warning](
    [votingday] nvarchar(4000),
    [municipality] nvarchar(4000),
    [point] nvarchar(4000),
    [latitude] float,
    [longitude] float,
    [count] int,
    [runnynose] int,
    [cough] int,
    [throat] int,
    [fever] int,
    [0-6count] int,
    [0-6runnynose] int,
    [0-6cough] int,
    [0-6throat] int,
    [0-6fever] int,
    [7-12count] int,
    [7-12runnynose] int,
    [7-12cough] int,
    [7-12throat] int,
    [7-12fever] int,
    [13-18count] int,
    [13-18runnynose] int,
    [13-18cough] int,
    [13-18throat] int,
    [13-18fever] int,
    [19-64count] int,
    [19-64runnynose] int,
    [19-64cough] int,
    [19-64throat] int,
    [19-64fever] int,
    [over65count] int,
    [over65runnynose] int,
    [over65cough] int,
    [over65throat] int,
    [over65fever] int)


WITH (LOCATION='/hive/',   
        DATA_SOURCE = AzureStorage,  
        FILE_FORMAT = MSCSV  
);  

select count(*) from [asb].[warning];


-- EXPORT
CREATE EXTERNAL TABLE warning_exp WITH
(
    LOCATION='<StorageLocation(Relative path)>',
    DATA_SOURCE=AzureStorage,
    FILE_FORMAT=MSCSV
)
AS
SELECT [votingday]
      ,[municipality]
      ,[point]
      ,[latitude]
      ,[longitude]
      ,[count]
      ,[runnynose]
      ,[cough]
      ,[throat]
      ,[fever]
      ,[0-6count]
      ,[0-6runnynose]
      ,[0-6cough]
      ,[0-6throat]
      ,[0-6fever]
      ,[7-12count]
      ,[7-12runnynose]
      ,[7-12cough]
      ,[7-12throat]
      ,[7-12fever]
      ,[13-18count]
      ,[13-18runnynose]
      ,[13-18cough]
      ,[13-18throat]
      ,[13-18fever]
      ,[19-64count]
      ,[19-64runnynose]
      ,[19-64cough]
      ,[19-64throat]
      ,[19-64fever]
      ,[over65count]
      ,[over65runnynose]
      ,[over65cough]
      ,[over65throat]
      ,[over65fever]
  FROM [asb].[warning];

  -- CTAS
CREATE TABLE [warning_new]
WITH
(
    CLUSTERED COLUMNSTORE INDEX,
    DISTRIBUTION = HASH([votingday])
)
AS SELECT * FROM warning_exp;

select count(*) from [asb].[warning];
select count(*) from [warning_new]

UPDATE STATISTICS [warning_new]