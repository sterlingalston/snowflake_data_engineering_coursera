# Snowflake Architecture Overview

### Core Objects and Architecture

Architecture Layers

![](assets\20250817_190847_snowflake_platform_architecture.png)

#### Optimized Storage

![](assets\20250817_191307_optimized_storage_layer.png)

#### Elastic Multi-Cluster Compute

![](assets\20250817_191526_elastic_multi-cluster_compute.png)

#### Cloud Services

![coud_services.png](./e30a203a44c65040cc4a60ea5237b837.png "coud_services.png")

#### Snowgrid

![snowgrid.png](./2b3cb9e03f863bacd62c8a3866cc3422.png "snowgrid.png")

### Time Travel

* UNDROP and SHOW TABLES uses timetravel
* AT,BEFORE (TIMESTAMPE=>, STATEMENT=>)

### Permanent, Transient, and Temporary Tables

### Cloning

- cloning can be combined with time travel

`---> clone a table based on an offset (so the table as it was at a certain interval in the past)  CREATE OR REPLACE TABLE tasty_bytes.raw_pos.truck_clone_time_travel  CLONE tasty_bytes.raw_pos.truck AT(OFFSET => -60*10); `

### Resource Monitoring

- *account-level monitor* - total credit consumption over all resources in account

- *warehouse-level* - total credit consumption for a particular warehouse

- ![](assets\20250820_192319_image.png)

- creating warehouse-level
  
  ![](assets\20250820_192427_image.png)

- 

[resource_monitors.sql](.\resource_monitors.sql)

### User-Defined Functions (UDFs) - Part I/II

![](assets\20250821_143706_image.png)

- '$$' are called bookends

### User-Defined Table Functions (UDTFs)

- cannot have Scala UTFS
  - can have JavaScript, Python, Java, and SQL

### Role-based Access Control

automatically generated roles

![](assets\20250821_164459_image.png)

- SHOW GRANTS TO ROLE {name of role}
  
  ![](assets\20250821_164606_image.png)

- ACCOUNTADMIN has access to Security Admin Role < User Admin role

- One, we learned about securable objects,   roles, privileges, and users.   Two, we learned about   the system defined roles, Org Admin,   account Admin, Security Admin,   user Admin, Sys Admin, public.   Three, we learned how to assume a role.   Four, we learned how to create a new role.   F, we learned how to grant privileges to a role.   Six, we learned how to show   what privileges a role has been granted,   and seven, we learned how to grant a role to a user.   That's a lot of stuff.   We're not quite Arbach black belts,   but we're probably at least Arbach blue belts   or something. Great job.

### Snowpark Dataframes

- library to help transform data
- Python/Java/Scala
- lazy-execution
  - df.show() will not execute until explicitly asked
- saving results to table

### Snowflake Data Engineering

![](assets\20250823_174903_image.png)

### Ingestion

![](assets\20250823_175717_image.png)

### Transformation, Orchestration, and Observability

#### Transformation

![](assets\20250823_175949_image.png)

#### Orchestration

- streams - created on a table and keeps track of changes on table
- tasks - actions on scheduled basis
- ![](assets\20250823_180134_image.png)
- ![](assets\20250823_180526_image.png)
- 

#### Observability

-![](assets\20250823_180850_image.png)

#### Snowpipe Part I

#### steps to create IAM and external storage

![](assets\20250823_224354_image.png)

![](assets\20250823_224417_image.png)

![](assets\20250823_224305_image.png)

![](assets\20250823_224543_image.png)

![](assets\20250823_224601_image.png)

![](assets\20250823_224636_image.png)

![](assets\20250823_224748_image.png)

![](assets\20250823_224846_image.png)

![](assets\20250823_224910_image.png)

### Snowflake GenAI - Part I and II

![](assets\20250823_232007_image.png)

- document A.I. let's you use SF LLM to extract data using cortex
- ![](assets\20250823_232133_image.png)
- universal search lets you use natural language to search for anything within SF acct.
- ![](assets\20250823_232230_image.png)
- snowflake copilot generates SQL from natural language
- ![](assets\20250823_232310_image.png)
- streamlit is not a GenAI application, but cortex can be embedded
- #### cortex
- ![](assets\20250823_232641_image.png)
- 

Later, we'll see that Snowflake Cortex's ML functions operate very similarly, except that under the hood they're powered by ML models instead of by GenAI ones.

SPCS - Snowpark Container Services

- code added to image
- image generates container and runs within snowflake system
- bring code to data and keep data in snowflake

### Cortex

- LLMs convert input to tokens and convert back to original language
  - cost of using complete can vary
  - larger charge more per token and smaller charge less
  - can specify max token size, which limits size of the response

### ML Overview

- ![](assets\20250824_141405_image.png)
- ML Functions in Snowflake
- ![](assets\20250824_141617_image.png)
- snowpark has scikit-learn, xgboost library equivalents
- ![](assets\20250824_142256_image.png)
- SF model registry helps with model management for version artifacts and metadata
- ![](assets/2025-10-08-09-41-45-image.png)
- input day of the week -- variable neighborhood
- "Do some neighborhoods come up more in the second half of the year?"
  - Tree-based models
- ![](assets/2025-10-08-09-43-59-image.png)
- ![](assets/2025-10-08-09-45-39-image.png)
- splitting data into training set and test set
- don't want to train model on same data testing on; like "cheating on a test"
- ![](assets/2025-10-08-09-48-48-image.png)
- ![](assets/2025-10-08-09-49-11-image.png)



### Snowflake Applications

- application = software product to help users do a certain thing

- highly sophisticated

- can be integrated

- ![](assets/2025-10-08-10-03-46-image.png)

- establish connection, execute sql commands -- accommodates applications

- 
