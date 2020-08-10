# Dapla Storage Client

<!-- TABLE OF CONTENTS -->
## Table of Contents

* [About](#about)
  * [Built With](#built-with)
* [Prerequisites](#prerequisites)
* [Usage](#usage)
* [License](#license)


<!-- ABOUT -->
## About
Dapla Storage Client enables crud operations on **datasets**, either locally or against google cloud storage (gcs).

A **dataset** in this context is a set of **records** written to one or more **dataset files**. **dataset files** are **[parquet](https://parquet.apache.org/documentation/latest/)** 
files stored locally or in a gcs bucket. With **parquet**, **records** within the **dataset files** are compressed and organized 
by column. 

Some things the Dapla Storage Client can do:
* Create a new dataset and upload it to gcs
* Read a dataset as a set of avro records
* List all the dataset files that make up a dataset
* Read a single dataset file
* Do an asynchronous stream-upload of bytes to gcs

### Built With

* [Java 11](https://jdk.java.net/11/)
* [parquet-mr](https://github.com/apache/parquet-mr)
* [RxJava](https://github.com/ReactiveX/RxJava)


## Prerequisites

* Java v11 or higher
* gcs credentials (either [service account credentials](https://cloud.google.com/iam/docs/creating-managing-service-account-keys) or [compute engine credentials](https://cloud.google.com/compute/docs/api/how-tos/authorization)) 


<!-- USAGE EXAMPLES -->
## Usage

Maven artifact
```xml
<dependency>
    <groupId>no.ssb.dapla.storage</groupId>
    <artifactId>dapla-storage-client</artifactId>
    <version><!-- replace me --></version>
</dependency>
```

### Write an unbounded sequence of [avro](https://avro.apache.org/docs/current/spec.html) records (`org.apache.avro.generic.GenericRecord`) to a given dataset
```java
DatasetUri datasetUri = DatasetUri.of("gs://my-bucket", "tmp", "3");

// Avro schema
Schema schema = new Schema.Parser().parse("{\n" +
                "  \"type\": \"record\",\n" +
                "  \"name\": \"person\",\n" +
                "  \"fields\": [\n" +
                "    {\n" +
                "      \"name\": \"name\",\n" +
                "      \"type\": \"string\"\n" +
                "    }\n" +
                "  ]\n" +
                "}");

Flowable<GenericRecord> records = ...

DatasetStorage storageClient = DatasetStorage.builder().withBinaryBackend(new GoogleCloudStorageBackend()).build();

// Flush records to file every 10 seconds, or after processing 1000 records, whatever comes first
Observable<GenericRecord> lastRecordEveryBatch = storageClient.writeDataUnbounded(datasetUri, schema, records, 10, TimeUnit.SECONDS, 1000);
```

<!-- LICENSE -->
## License

Distributed under the Apache-2.0 License. See `LICENSE` for more information.
