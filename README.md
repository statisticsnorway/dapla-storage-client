# Dapla storage

Streams data from and to bucket storage

## Modules

The project is composed of the following modules; 

1. dapla-storage-client. The main modules, it contains the StorageClient class that can be used to write and read data.
2. dapla-storage-gcs. Backend implementation for Google Cloud Storage. 
3. dapla-storage-hadoop. Backend implementation for Hadoop fs.

## Usage

Add the desired modules to your project:  

```
<dependency>
    <groupId>no.ssb.dapla.storage</groupId>
    <artifactId>dapla-storage-client</artifactId>
    <version>0.0.1-SNAPSHOT</version>
</dependency>

<dependency>
    <groupId>no.ssb.dapla.storage</groupId>
    <artifactId>dapla-storage-gcs</artifactId>
    <version>0.0.1-SNAPSHOT</version>
</dependency>
```

Instantiate the client: 

```java

// Set the parquet settings.
ParquetProvider.Configuration parquetConfiguration = new ParquetProvider.Configuration();

// Set the parquet settings.
StorageClient.Configuration clientConfiguration = new StorageClient.Configuration();

BinaryBackend backend = /* ... */

client = StorageClient.builder()
        .withParquetProvider(new ParquetProvider(parquetConfiguration))
        .withBinaryBackend(backend)
        .withConfiguration(clientConfiguration)
        .withFormatConverters(/* ... */)
        .build();

```

## Class diagram

![Class diagram](http://www.plantuml.com/plantuml/proxy?src=https://raw.githubusercontent.com/statisticsnorway/dapla-storage/master/dapla-storage/src/main/resources/class-diagram.puml)