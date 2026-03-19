### Overview Part I
- software product to help users do things
- ![image](.attachments/7aba0c55d255e7105961aefb79abe80bb0d229ba.png)
- ![image](.attachments/a37aa84c199c05929269472c03d6bf700e0c5e06.png)
- easy to integrate snowflake into broader application stack
- establish connection, execute sql commands based on actions in UI, accommodates large applications
- django snowflake connector
  - works with common django application patterns
- hybrid table to work with web applications without managing different backend technologies

### Overview Part II

![image](.attachments/6eb7b9d94dc284a02ca533bf4d7c597c9a35639e.png) 
- can host everything within snowflake

  #### streamlit in snowflake

  - can use it within snowflake acct
    - front end use streamlit python
    - logic powered by snowflake
    - no headache with deployment
  - snowflake native app framework
    - users can purchase and install applications within snowflake account

  ### Streamlit in Snowflake - Part1

[sample code](https://github.com/sterlingalston/snowflake_data_engineering_coursera/blob/master/streamlit_in_snowflake.txt)


```
streamlit.io/gallery
```

open-source framework for dynamic applications with Python

![image](.attachments/31703185887dd892cd88fac8ed115af6c1f0065a.png) 

![image](.attachments/43363a92158845f8bcbbb79837779734494178e3.png) 
- function returns sales_data pandas and sql tuple
- ![image](.attachments/d4ce4554e816b2de3c1263e7dd136095c6230497.png)
- st.cache_data - looks to see if you've run the function for those inputs already and if you have it'll just use the results from that
- ![image](.attachments/63882d2f08f18cc3d670d30e622a60d9b3537d6c.png)
- generates chart using altair library
- 
![image](.attachments/5c833fafda38e8fa907d7cea89e7d1557f55f92f.png)
- setting parameters to pass into the function to create dataframe
- ![image](.attachments/3ef52bc8ecb12273813ffc37b2f7010b6bd4d1c3.png)
- setting parameters to pass into the chart

![image](.attachments/6714626658c0f983a4a011db13cc5b44654f368f.png) 

