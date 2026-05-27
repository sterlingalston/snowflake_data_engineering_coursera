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