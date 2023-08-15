## Data Engineering Takeaways
*The following a important takaways that I collect time and again through out my data engineering journey.*
In my opinion I can define data engineering as a set of practices that model the smooth collecting, cleansing and storage of data hence making it usefull for downstream users.

_Note:_ my takeaways are going to be based on *Joe Reis* and *Matt Housley* incredible book, *Fundamentals of Data Engineeerig*. Please read the book for great insights and guidance through out your data engineering journey

According to the above authors data engineering's defination is not complete without the addition s of the undercurrents that support it. They include, _security, software eningeering, dataops, data management, orcherstation and data architecture._

Data engineering hence becomes well explained using a lifecycle which includes:
- Generation - Ingestion - Transformation - Storage  - Serving  
togethor with the undercurrents.
The maturity of data affects the comlexity of data in various ways ie 
- Starting with data.
- leading with data.
- Scaling with data.
Data can also be classified into three categories depending on the degree of access:
- hot data: easily accessed
- warm data: accessed maybe once or twice 
- cold data: used for backup ie accessed during emergencies.
Streaming frameworks like *Apache Kafka* can function as ingestion, query and storage of data.

**Questions to ask when selecting a storage system**

1. What is the formart of the data being stored?
2. What is the usecase of the stored data?
3. What is the volume of the data being stored?
4. What is the size of the data?
5. How frequently is the data being ingested?

Data can be ingested using _batch vs streaming or push vs pull_.
Streamed data is produced and updated continuously while batch the streams are produced in large chunks
- Pushing - writing data to a source.
- Pulling - fetching data from a source.
- Polling involves checking a source for changes.

*Deciding between batch and stream*

1. What is the usecase?
2. Can downstream handle the rate of data flow?
3. What tools will I require and who will manage them?

_Orchestration_ is ensuring many jobs run as quickly and effectively as possible we use airflow or prefect

*Data Architecture*
Architecture is the design decisions that shape a system.
_Operational architecture_ tell us what needs to be build.
_Technical architecture_ tell us how we buid it.
A data architecture always builds a system that solves a need while recognising the trade offs.

*Principles to consider when selecting a data architecture*
1. Always be architecturing
2. Build scalable systems.
3. Build loosely coupled sytems.
4. Plan for failures.
5. Choose common tools wisely.
6. Make reverable decisions.
7. Architecture is leadedership.
8. Prioritize security.

We have two architectral tiers, single tier( every thing is in one server and are tiighty coupled) and the muti-tier architecture (
    the layers are decpupled such that lower layers are independent and upper layers are dependant on the lower layers.
), ensure you consider perfomance and security during this stage.
A monolith is like a single tier, every thing is found on the same machine ie the codebase and the database.
In an event driven architecture, we have a _producer, router and a consumer_ 

A _lambda architecture_ onthe other hand reconsiles batch and streaming datails into one. The systems operate independently of each other with the stream serving low latency data.

Some common architectures include:
1. Data warehouse: used to store large amounts of data for reportng and analysis. The data is ussually ordered.Object storage is    mostly used eg snowflake(separates storage and compute).
2. Data lake: stores large amounts of data from all sources and of anykind without a particular structure
3. Data lakehouse: combines the abilities of both a data warehouse and a data lake into one solution.
4. Data marts: used to divide data in a warehouse and make it accessible to a particular team or group of people in an organisation.

**Techology**
When choosing a technology, ensure it adds value to the data and the broader business.
Map out the architecture first (what, when, who) then decide on the technical tools that solve your problem.
Consider the following:
1. Your team number and stregths
2. Interoperability
3. Speed to market.
4. Cost optimisation vs business value.
5. Immutable vs transitory techs
6. Cost of ownership
7. Total opportunity cost of ownership.

A message queue like _Apache Kafka_ uses  the pub/sub architecture, where the data in form of a message is published in the message queue. It is then deliveed to one or more of the subscribers. Once recieved the subscriber acknowledges the reciept of the message hence removing it from the queue.
Messages a re delivered using FIFO and the queue should be scalable.

### Generation
This stage entails fetching the data from various sources. 
Example of sources include:
1. Files and unstructured data.
2. Apis like webhooks(event based)
3. App databases via OLTP(Online Transaction Processing) - run transactional queries.
4. OLAP(Online Analytical Processing) - run analytical queries.
5. CDC(Change Data Capture) - once an update happens on the db it is captured.
6. Logs of information happenning on a system.

### Storage
This stage underlies all the other stages of the data engineering life cycle since regardlessof the stage data needs to be stored either partially or fully.
This stage requires you to know the usecase of data and how it will be retrieved in future.
Serialization and compression of data is very important in this stage.
There are various formarts used to serialise data, they include
- Row based serialization which orgarnises data by row eg CSV, JSON, JSONL, XML and Avro.
- Column base serialization which organises data by storing each column into its own set of files. Data in the files can be divided into batches with cluster for easy retrieval eg Parquet, ORC, Apache Arrow
- Hybrid Serialization - they combine multiple serialization techniques eg Hudi, Icerberg

There are various compression algorithims like snappy used in object storage , gzip, bzip.

Some example of data storages include:
1. Disk drives HDD and SDD
2. Memory
3. Caching
4. Distributed storage(Data stored in muliple servers) uses consistency patterns eg (Basically Available, Soft-state, Eventual)
5. Snapshot and journalling in relation to redis

**Considerations when storing data**
1. Purpose of storing the data.
2. How the data is being updated.
3. What is the cost of data storage.
4. Is the storage separate from compute.

Consider data retention and how long you need to keep the data in terms of the _data value, time and compliance_ , during storage.

### Ingestion
This involves the movement of data from one place to another eg from source to storage.
Ingestion is different from intergration. Intergration involves _collecting data from various sources into one dataset_.
This is the stage where a data pipeline is fully designed despite being defined all through the data engineering cycle.
A data pipeline fully defines the _movement of data , patterns used in movement (ETL, ELT and reverse ETL) and how its being shared all through_.
It combines architecture, systems and processes that move data all through.
A pipeline should be flexible enough to meet any needs during the journey.

*Questions to ask during ingestion*

1. What is the usecase?
2. How frequently is the data being ingested?
3. Can the data be reused instead of reingeting and creating multiple versions of the same thing?
4. Are the systems ingesting the data reliable?
5. How frequently do I need to access then data?
6. Does the data need any transformation before reaching its destination?
7. What is the volume and formart of the data?

**Factors to consider when designing an ingestion architecture**
1. Bounded vs Unbounded - real time or restricted by time.
2. Synchronised vs Asynchronised - is the data tightly coupled or free.
3. Serialized vs Deserialized - can the data be encoded while being stored and decoded once it reaches the destination and vise versa.
4. Frequency - is the data very frequent or semi frequent.
5. Throughput vs Scalability - do systems shrink and expand to match the throughput.
6. Payload - this is the data you are ingesting.(kind,shape and size)
7.  Push vs Pull vs Poll - sending data , reading data and checking for changes.

**Some common batch processing patterns used during ingestion**
1. Snapshot(whole) vs Incremental(interval)
2. File-based (used for both ingestion and export)
3. ETL and ELT
4. Data migration
5. OLTP and OLAP

**Stream processing factors**
1. Schema evolution
2. Late arriving data
3. Replay(range of messages from history).
4. TTL- how long will the event live before being acknowledged and ingested.
5. Error handling and dead letter queues(used to debug ingestion errors).
6. Location
7. Consumer pull and push.

**Various ways to ingest data**
1. From publishers you subscribe to.
2. Apis
3. Direct database connection
4. Change Data Capture (CDC)

### Transformation
This is the conversion of data into something usefull.
In order to better understand transformation, we need to understand queries and modelling of data as well
**_ A query_** is used to retrieve and manipulate data. 
_Data Defination Language(DDL)_ defines the data and include, Create, Update, Drop.
_Data Manipulation Language(DML)_ manipulate the data , they include Select, Insert, Update, Delete.
_Data Controll Language(DCL)_ controll who has access to what eg GRANT and REVOKE.
_Transaction controll language_ - support commands that controll the details of a transaction eg Commit and Rollback.
You can drastically improve a query perfomance using _CTEs, prejoining data, considering details and complexity of your joins and avoiding full table scans_, know how your database handles commits, vaccume dead records just to mention a few.

When handling streaming data, you can use the following patterns for guidance:
1. Place the analytics database one step behind production.
2. Use the Kappa architecture(stream source -> stream processing -> serving layer -> query) to handle all events and store all events as a stream rather than a table.
3. Handle windows, triggers, emitted statistics and late arriving data.

**_Data modeling_** maps out how multiple parts of a storage system are connected together.
The give us an upclose of how the business looks(what and what is conncected etc).
_Conceptual model_ – give abig picture view of what the model will contain. From object entities to constraints, relationship between them security and business rules involved. From the name concept defination.
_Logical modeling_ – build upon the conceptual modeling and entails datatypes their corresponding lengths and relationship among entities.
_Physical modeling_ - entails how data will be stored physically in tables illustrating the relationship between tables PK, FK DBMS

Entity relation diagrams and normalisation are also key concepts in data modelling

Rshp btwn RDBMS and data lakes
    • Normalization - it is the process of organizing data in a database to minimize redundancy and improve data integrity.
    • B/s use case – what is really wanted not what I want to build.
    • Pysical data location
    • Grain of database(resolution at which data is queried)
    • Data types and contstraints.

**Techniques for modelling batch anaytical data**
1. Kimball - where data is arranged in form of facts and dimensions(surround a fact in a relationship called a star schema).
2. Inmon - data is ussually normalised to first second or atleast third NF.
3. Data vault - separate aspects of source systems data from its attributes with hub stroring the bussiness key eg customer_id, a link creating a relationship between the keys and a satelite containing attributes and  context.
4. Slowly changing dimensions - used in data warehousing and business intelligence scenarios. 
    - **Static dimensions** the new value simply overides the old one
    - **Historical dimensions**  we have columns containing the change in data eg end_date column
    - **semi-historical dimensions** stores a certain amount of historiacal information and might also have columns like prev_name

**Streaming data** is unbounded in nature hence not many techniques are present to model streaming data

**Transformations**
The difference between a queue and transformation is the queue moves and manipulate the data wherase transformation persists the manipulated data. 
Transformation creates a lot of complexity due to the complex pipelines.
It fully depends on orchestration. 
A transformation pipeline spans not only multiple tables and datasets but also multiple systems.

Spark is used to transform data present in a data lake, when making complex transformations which cannot be done with sql. It is used for both batch and streaming jobs.

**Batch Transformation**
Happens using the following ways:
1. Distributed joins - break data into smaller node joins that run on individual servers.
                      - data on one side of the join is small enough to fit into a singlenode(broadcast join)
2. Broadcast join - a generally asymetric data distributed accross the joins. In this case we have one large table node and small nodes containing data from another table divided and distributed accross the nodes. The primary job now becomes reordering.
3. Shuffle join -  uses if neither of the tables is small enough to fit in a single nnode.

**Update patterns**

1. Truncate and reload - delete everything and reload database.
2. Insert only - new records are inserted without deleting the old ones and refference in this table are made using the recent record.
3. Delete - soft or hard delete
4. Update and merge

### Serving
Delivering the new data to downstream users.
Ths can be used in 
1. Analytics 
2. ML models
3. Reverse ETL

When serving data consider the following:
1. The usecase
2. Uterlise data validation and observability.
3. What are the data products?
4. Will you allow self service or not?
5. Data definition and logic.

Where window functions, ctes or subqueries cannot be used, use a self join eg  calculating the minimum year abook was borowed from the library we can find the minimum year using MIN() window functions but we can also join the table to itself.

ROW_NUMBER() assigns a row a particular value even if their values are the same. `ROW_NUMBER() OVER()`

Case statements can also be placed inside aggregate functions eg `COUNT(CASE WHEN val=1 THEN 1 ELSE 0 END)`

A lookup table stores values and definations(PK) that can be refferenced by other tables(FK) it can be used instead of many case when statements.