### What is orchestration?

![image](.attachments/b8683d980335a2979c9af725e43ecaf70e4d8f47.png) 
- dynamic tables can help with automation
- training to help maintain ML model with fresh data
- orchestration is automation at scale
- TASKS are the magic behind automation

### Automation with tasks

![image](.attachments/47dd9d318e6799c483b9d40a6a1329de56d44b8d.png) 
- user manged tasks assigns compute resources
- serverless does this for you -- it manages compute resources
- one-line code change
- scheduled with cron expression
- specifying **WAREHOUSE = {...} ** is for a user managed task; omitting this will make this a serverless task
- ![image](.attachments/45cef8113374913a8a263fcc276f8be7cbac0c10.png) 

### Orchestration with DAGs

- ![image](.attachments/7b61b9fd4613c02040fe413934c48de62f4c1e1a.png)
- ![image](.attachments/3cafc9cc23be865eb08b49e1b9de6d9749b6bb35.png)
- DAG (directed acyclical graph)

```sql
USE ROLE accountadmin;

USE WAREHOUSE compute_wh;

USE DATABASE tasty_bytes;

  

-- Create an email integration

CREATE OR REPLACE NOTIFICATION INTEGRATION email_notification_int

TYPE = EMAIL

ENABLED = TRUE

ALLOWED_RECIPIENTS = ('email@address.com'); -- Update the recipient's email here

  

CREATE OR REPLACE PROCEDURE tasty_bytes.raw_pos.last_seven_days_report()

RETURNS STRING

LANGUAGE PYTHON

RUNTIME_VERSION = '3.10'

PACKAGES = ('snowflake-snowpark-python')

HANDLER = 'send_email'

AS

$$

import snowflake.snowpark.functions as F

from snowflake.snowpark import Session

  

def send_email(session: Session) -> str:

# Query the 7 most recent entries in the DAILY_SALES_HAMBURG_T table

recent_entries_df = session.table("RAW_POS.DAILY_SALES_HAMBURG_T") \

.sort(F.col("DATE_TRUNC(LITERAL(), ORDER_TS)").desc()) \

.limit(7) \

.to_pandas()

# Convert the DataFrame to an HTML table with styling

html_table = recent_entries_df.to_html(index=False, classes='styled-table')

  

# Define the email content with Snowflake branding and custom styling

email_content = f"""

<html>

<head>

<style>

body {{

font-family: Arial, sans-serif;

}}

h2 {{

color: #29B5E8;

}}

.styled-table {{

border-collapse: collapse;

margin: 25px 0;

font-size: 0.9em;

font-family: 'Trebuchet MS', 'Lucida Sans Unicode', 'Lucida Grande', 'Lucida Sans', Arial, sans-serif;

min-width: 400px;

border-radius: 5px 5px 0 0;

overflow: hidden;

box-shadow: 0 0 20px rgba(0, 0, 0, 0.15);

}}

.styled-table thead tr {{

background-color: #0072CE;

color: #ffffff;

text-align: left;

font-weight: bold;

}}

.styled-table th,

.styled-table td {{

padding: 12px 15px;

}}

.styled-table tbody tr {{

border-bottom: 1px solid #dddddd;

}}

.styled-table tbody tr:nth-of-type(even) {{

background-color: #f3f3f3;

}}

.styled-table tbody tr:last-of-type {{

border-bottom: 2px solid #0072CE;

}}

</style>

</head>

<body>

<h2>Weekly Sales Report for Hamburg</h2>

<p>Here are the last 7 entries in the <strong>DAILY_SALES_HAMBURG_T</strong> table:</p>

{html_table}

</body>

</html>

"""

# Send the email

session.call("system$send_email",

"email_notification_int",

"email@address.com",

"Weekly Sales Report for Hamburg",

email_content,

"text/html")

# Return a success message

return "Weekly sales report email sent successfully"

$$;

  

-- Create task that runs after the process_orders_header_sproc task

-- It calls the sproc above

CREATE OR REPLACE TASK tasty_bytes.raw_pos.send_last_seven_days_report

WAREHOUSE = 'COMPUTE_WH'

AFTER tasty_bytes.raw_pos.process_orders_header_sproc

AS

CALL tasty_bytes.raw_pos.last_seven_days_report();

  

-- Start the tasks

ALTER TASK tasty_bytes.raw_pos.send_last_seven_days_report RESUME;

ALTER TASK tasty_bytes.raw_pos.process_orders_header_sproc RESUME;

  

-- Start the DAG

EXECUTE TASK tasty_bytes.raw_pos.process_orders_header_sproc;

  

-- Stop the tasks

ALTER TASK tasty_bytes.raw_pos.process_orders_header_sproc SUSPEND;

ALTER TASK tasty_bytes.raw_pos.send_last_seven_days_report SUSPEND;
```

- ![image](.attachments/34e47c4920df388008beedcce1e6167579927b70.png)
- 