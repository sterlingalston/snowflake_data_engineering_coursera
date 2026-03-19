You load the Tasty Bytes demo by running Snowflake’s prepared worksheet that creates the `tasty_bytes_sample_data` objects and copies data from their S3 stage.[](https://docs.snowflake.cn/en/user-guide/tutorials/tasty-bytes-sql-load)​

## One-time setup in Snowsight

1.  Log in to Snowflake and open **Snowsight** (the web UI).[](https://docs.snowflake.cn/en/user-guide/tutorials/tasty-bytes-sql-load)​
    
2.  Go to **Projects & Worksheets → Worksheets → Tutorials** and open the worksheet for “Load and query sample data using SQL (Tasty Bytes)”. This is pre-loaded by Snowflake.
    
3.  In that worksheet, run each block in order (cursor in the block → **Run**). The blocks will:[](https://docs.snowflake.cn/en/user-guide/tutorials/tasty-bytes-sql-load)​
    
    -   Create the database, schema, and table:
        
        ```sql
        CREATE OR REPLACE DATABASE tasty_bytes_sample_data; CREATE OR REPLACE SCHEMA tasty_bytes_sample_data.raw_pos; CREATE OR REPLACE TABLE tasty_bytes_sample_data.raw_pos.menu (   menu_id NUMBER(19,0),  menu_type_id NUMBER(38,0),  menu_type VARCHAR(16777216),  truck_brand_name VARCHAR(16777216),  menu_item_id NUMBER(38,0),  menu_item_name VARCHAR(16777216),  item_category VARCHAR(16777216),  item_subcategory VARCHAR(16777216),  cost_of_goods_usd NUMBER(38,4),  sale_price_usd NUMBER(38,4),  menu_item_health_metrics_obj VARIANT );
        
  
-   Create the external stage pointing to Snowflake’s demo S3 bucket:
            
        
```sql
CREATE OR REPLACE STAGE tasty_bytes_sample_data.public.blob_stage   url = 's3://sfquickstarts/tastybytes/'  file_format = (type = csv);
 ```
 -   (Optional) List stage contents:
        
       ```sql
         LIST @tasty_bytes_sample_data.public.blob_stage/raw_pos/menu/;
     ```
        
   -   Load data into the table:
        
        ```sql
        COPY INTO tasty_bytes_sample_data.raw_pos.menu FROM @tasty_bytes_sample_data.public.blob_stage/raw_pos/menu/;
        ```
        
  - Verify load:
        
    ```sql
        SELECT COUNT(*) AS row_count FROM tasty_bytes_sample_data.raw_pos.menu; SELECT TOP 10 * FROM tasty_bytes_sample_data.raw_pos.menu;

    ```
        
    
[These steps “load” the Tasty Bytes demo data into your account.](https://docs.snowflake.cn/en/user-guide/tutorials/tasty-bytes-sql-load)
    

## If you’re following a specific Tasty Bytes Quickstart

Some Tasty Bytes guides (Zero to Snowflake, Snowpark 101, React Native app, Iceberg tables) use slightly different database names (e.g., `tasty_bytes_db`, `frostbyte_tasty_bytes_app`) and additional tables. In those cases:

-   Open the Quickstart page you are using (e.g., Snowpark 101, Collaboration, Cost Management).
    
-   Download or open the provided **setup.sql** / worksheet (often linked on the page or from the GitHub `sf-samples` repo).
    
-   Run the entire setup script in a Snowsight worksheet; it will create the right DB, schemas, stages, and `COPY INTO` commands from `s3://sfquickstarts/frostbyte_tastybytes/` or similar.
    

## Small example from an Iceberg/Orders demo

For example, one Quickstart creates a Tasty Bytes orders table and loads it like this:[](https://www.snowflake.com/en/developers/guides/tasty-bytes-working-with-iceberg-tables/)​

sql

`CREATE OR REPLACE TABLE tasty_bytes_db.raw.order_header ( ... ); CREATE OR REPLACE FILE FORMAT tasty_bytes_db.raw.csv_ff   TYPE = 'csv'; CREATE OR REPLACE STAGE tasty_bytes_db.raw.s3load   URL = 's3://sfquickstarts/frostbyte_tastybytes/'  FILE_FORMAT = tasty_bytes_db.raw.csv_ff; COPY INTO tasty_bytes_db.raw.order_header FROM @tasty_bytes_db.raw.s3load/raw_pos/order_header/;`

Are you trying to load the basic “Load and query sample data using SQL” lab, or one of the newer Tasty Bytes Quickstarts like Snowpark 101 or the React Native SQL API app?