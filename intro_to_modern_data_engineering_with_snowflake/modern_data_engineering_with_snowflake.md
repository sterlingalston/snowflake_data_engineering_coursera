- by 2025 200 ZetaByte (200 billion TB)
- explosion in amount of data
- corresponding increase in demand to extract insights
- valuable usable insights

## Modern data engineering in snowflake

- [Cloud Data Engineering for Dummies](https://www.snowflake.com/resource/cloud-data-engineering-for-dummies/)
- ![image](.attachments/15039daac9cb1725cb75c37bad434cec7d3f35bb.png)
- ![image](.attachments/e14079bf4145c48b01c2dbbbcf26211daaabb2f1.png)
- ![image](.attachments/287bc5b029f0f09d8b66dbf75d73ed7cadd1ecf1.png)
- ingestion, transformation, delivery
- ![image](.attachments/214f1e5d050ef831e205d885a325634dd3f411c0.png)
- ![image](.attachments/cfb2056f05f51840a7343319d95b1b02d717ead7.png)
- ![image](.attachments/e611bda1510a1400e0aa80a719644c43d6206dd2.png)
- single platform will not solve the challenge of picking an approach to building a data pipeline

### You've probably done some data engineering

- Ingestion, Transformation, and Delivery (ITD)
- Example: importing .csv into excel/google sheet

### Completing the course

- malstonsnowcert12@gmail
- (same as ecosyst)
- https://github.com/Snowflake-Labs/modern-data-engineering-snowflake
- for SF acct:
  - first initial last name
  - pw same as bozman

## Batch data ingestion with Snowflake

### What is data ingestion?

- 80% is getting all of the data into one platform
- ![image](.attachments/03cdcaae60caf56fc96c654245275da728d418e1.png)
- Challenges:
  - scale
  - frequency
    - daily? real-time?
  - sources
    - sources are strong dictator to approach to ingestion
  - formats

### Batch ingestion with Snowflake

![image](.attachments/8c1f969bf7768c3ac86a78f1d33472579bd005b5.png) 

- commonly used when doing migration or large amounts

  ![image](.attachments/eac9ba6a51f7df8d24099387f3886871ded7d35d.png)

- streaming for financial trading or real-time monitoring

![image](.attachments/6a8cad2c0dcb480938672c474de6361264133690.png) 

![image](.attachments/d1561f682fd02346b1d17a9432ebe72fb5b3adb6.png) 


### Loading data from Snowflake Marketplace

- sf marketplace data completely owned by provider and it's a live dataset
- dataset for training an ml model
- ![image](.attachments/535cbcf3edece4f1df14f27ad78dc13e5c46411d.png)
- weather source = pelmorex
- ![image](.attachments/0b0dbdca44a8129209ec9ff552bc97298a5fe4f5.png)


### Loading data using Snowflake's web interface

- using GUI (add data)
- ![image](.attachments/fa61815cc6a51dd75fb9601d26adf73fc7a66dca.png)
- ![image](.attachments/06a99917ff77270626799628c1503eab76332a98.png)


### Optimize compute resources

- compute clusters (one or more nodes)
- For now, let's simply refer to them as compute clusters. ​Compute clusters can be made up of one or more nodes. 

​Each node is a virtual machine that provides a cpu, memory, and ​temporary storage to execute SQL and other operations against your data. ​A single node is able to perform multiple data operations in parallel using threads. ​

![image](.attachments/ad9e55af5ffcad0ad9b029ab9ec5ab36b04bc6b7.png) 
- warehouses T-shirt size
- ![image](.attachments/159c4d7597e65f5badfd236520c3bc9e65db1e2e.png)
- large the warehouse the more threads
- ![image](.attachments/b7d524fdb1ab4e34a297e8b22f3211865df555bd.png)
- ![image](.attachments/20616a99f84c430d9b9bf0644b827fd89293e908.png)
- ![image](.attachments/756f8dfb7e5b315c1e29888762da6b253a154e5e.png)
- OPTIMAL SIZE FOR A FILE 100 AND 250 MB compressed
- ![image](.attachments/a02178a630cb9ebff5435a158dd8779abb9f54d6.png)
- **smaller files can be processed more efficiently**
- ![image](.attachments/ef2c581f7128c5d385d53aae37271583eb010c1a.png)
-  ![image](.attachments/73118dd5037ab885a24e592d8e983d886b1e87f3.png)


### Loading data using Snowflake CLI

- configure config.tml and connection in snowflake cli
```
default_connection_name = "modern_data_engineering_snowflake"

[cli]
ignore_new_version_warning = false

[cli.logs]
save_logs = true
path = "/home/malston/.snowflake/logs"
level = "info"

[connections.modern_data_engineering_snowflake]
account="WWOCYIE-WNB43301"
user="malston"
password="my(same as password)!same as RI"
warehouse="COMPUTE_WH"
database="LOAD_DATA"
schema="PUBLIC"
role="ACCOUNTADMIN"
```
``` snow connection
 1682  snow connection add
 1683  snow connection test
 1685  cd ../snow*
 1687  cd modern-data-engineering-snowflake
 1692  snow stage create snowflake_cli_stage
 1693  snow connection test
 1694  snow stage create snowflake_cli_stage
 1695  snow stage copy sample_orders.csv @snowflake_cli_stage
 1696  history | grep -i snow
```

![image](.attachments/32ee994a9dbe856ab468afec76de10e90ebc37a0.png)
```snow stage copy load_from_cli_stage.sql @snowflake_cli_stage```

![image](.attachments/3661b7a1433dfdc329361802f9567809226907be.png) 

### COPY INTO command

- copy files into table from a staged file (internal or external)
- ![image](.attachments/a582ee81d571133f9e1df443674e3d601ecbd7ff.png)
- 

