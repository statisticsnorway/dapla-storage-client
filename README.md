[![Build Status](https://drone.prod-bip-ci.ssb.no/api/badges/statisticsnorway/dapla-storage-client/status.svg)](https://drone.prod-bip-ci.ssb.no/statisticsnorway/dapla-storage-client)

# Dapla storage client

Streams data from and to bucket storage

## Usage

```xml
<dependency>
    <groupId>no.ssb.dapla.storage</groupId>
    <artifactId>dapla-storage-client</artifactId>
    <version>0.0.1</version>
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
