### What are data transformations?

![image](.attachments/75cf951acee20238c45624aaf78c367428c92153.png) 


### Transformations with SQL

#### snowpark

![image](.attachments/72d8d41ed22e00a92c2b18c99bc404d8c485fedf.png) 

#### Computations with user-defined functions

![image](.attachments/87b11a528173c8a79f8c6b04dc0c2d9882f22e60.png) 

```
USE ROLE accountadmin;

USE WAREHOUSE compute_wh;

USE DATABASE tasty_bytes;

  

CREATE OR REPLACE FUNCTION tasty_bytes.analytics.fahrenheit_to_celsius(temp_f NUMBER(35,4))

RETURNS NUMBER(35,4)

AS

$$

(temp_f - 32) * (5/9)

$$

;

  

CREATE OR REPLACE FUNCTION tasty_bytes.analytics.inch_to_millimeter(inch NUMBER(35,4))

RETURNS NUMBER(35,4)

AS

$$

inch * 25.4

$$

;
```
#### Efficient transformations with streams

- keep precise track of all changes made to a table
- ![image](.attachments/321893f5ea8094268e95bad45227829589534716.png)
- ![image](.attachments/447a73744f3995691bc9a8860a364cb23b276003.png)
- ![image](.attachments/ef0b3cc1204d4dea65dd06c1a84ff9b063c999c5.png)

  ```
  CREATE OR REPLACE STREAM order_header_stream ON TABLE tasty_bytes.raw_pos.order_header;
  ```
  ![image](.attachments/5db5d6653008ffd9e608bfa26c11f33608bfd199.png)
  ![image](.attachments/4a81ba35b4ac8891a307a4094971f61079e4e65c.png)
  - provide information about changes to the table

 ```

DELETE FROM tasty_bytes.raw_pos.order_header WHERE order_id=123456789;

-- This won't return the deleted action in the stream because of how standard streams work
-- See: https://docs.snowflake.com/en/user-guide/streams-intro#types-of-streams
SELECT * FROM tasty_bytes.raw_pos.order_header_stream;

```
![image](.attachments/9dd3f2e1cc17c204743f515fa5f8bd92941fd3bc.png) 

### Additional Resources

-   [User-defined functions overview | Snowflake Documentation](https://docs.snowflake.com/en/developer-guide/udf/udf-overview) 
    
-   [Snowflake 101: Working with Stored Procedures](https://select.dev/posts/snowflake-stored-procedures) 
    

**Streams**

Minor clarification on streams: **Querying** a stream returns the minimal set of changes from the stream's current offset to the current version of the table. You can query the stream over and over in this way, without affecting the stream's offset (i.e., you'll continue to get the same results). A stream advances the offset **only** when its contents are **consumed.** "Consuming", in this context, means specifically using stream contents in a [DML](https://en.wikipedia.org/wiki/Data_manipulation_language) transaction. 

-   [Stream Examples | Snowflake Documentation](https://docs.snowflake.com/en/user-guide/streams-examples) – The parent section "Streams" is excellent for more information on how Streams work "under the hood".
    
-   [Streaming with Snowflake](https://www.youtube.com/watch?v=zH5umveQW_c) – An excellent technical demo directly from the product manager who helped build Dynamic Tables.
    

**Dynamic Tables**

-   [Best practices for dynamic tables | Snowflake Documentation](https://docs.snowflake.com/en/user-guide/dynamic-tables-best-practices)
    
-   [Getting Started with Snowflake Dynamic Tables](https://quickstarts.snowflake.com/guide/getting_started_with_dynamic_tables/) – (Optional) For more hands-on practice with Snowflake Dynamic Tables.
    

**Snowflake Extension for Visual Studio Code**

-   [Snowflake Extension for Visual Studio Code](https://docs.snowflake.com/en/user-guide/vscode-ext)