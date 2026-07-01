### DevOps in the world of data engineering

- ![image](.attachments/9286c73b25a93f8188a800bae5ba7628ad4fde17.png)
- a core set of philosophies and best practices for allowing teams to quickly deliver and maintain software at scale
1) source control and collaboration
2) declarative management of code
3) automated deployments using CI/CD (continuous integration and continuous deployment)
4) tooling that improves productivity, like CLIs

### DevOps with Snowflake

#### source control within git

- snowflake git integration with platform (github)
- ![image](.attachments/f3a1bbe5bc22cecd0fe8ba232a84f8cd208e98b1.png)
- ![image](.attachments/b909b4292010a824b1baac652487e50243699c28.png)
- https://github.com/Snowflake-Labs/advanced-data-engineering-snowflake
- ![image](.attachments/a85f582da763de1b3d7cb38d3861c33cdfbc4968.png)
- ![image](.attachments/5c79774a9fdb33223d5dba829aecc46521f4d82d.png)
- ![image](.attachments/3a95c0b60b4c2d2fa2eba93bbc81abe9a9c77977.png)
- ```sql
  SHOW GIT REPOSITORIES
  ```
- ![image](.attachments/d6a5db6bdfcd0cce8df9e95501feeb8773454c04.png)
- ![image](.attachments/81c21360dc9a41d13c6049400aa17764f8bc3073.png)

#### snowflake cli

- ```bash
  snow git execute @advanced_data_engineering_snowflake/branches/main/module-1/hamburg_weather/pipeline/data/load_tasty_bytes.sql -D "env='STAGING'" --database=COURSE_REPO --schema=PUBLIC
  ```

  ```bash
  snow git execute @advanced_data_engineering_snowflake/branches/main/module-1/hamburg_weather/pipeline/objects/ -D "env='STAGING'" --database=COURSE_REPO --sche
ma=PUBLIC```

- ![image](.attachments/457e609fbdbdae8fa84914adbf7777f5c27dc5f1.png)
- ![image](.attachments/47d08178e133af7a6f371eb020706ba93852b2b2.png)
- ![image](.attachments/806af15c547b3e80c40cab7860afced73d42a494.png)
- ![image](.attachments/5babcb6a6c2ec8d759de1c6decc757fb0c5b6f51.png)

#### database change management (DCM)

- ![image](.attachments/7d873df88f233e4e101dda468c20befc1425a10a.png) 
- imperative step-by-step changes to database
- ![image](.attachments/4080c90ff1ab8719ef2bb78a125bf51cfcbde9ba.png)
- ![image](.attachments/ab5b368d9e17fa6f4cafd7955b374503f6f0aeaf.png)
- ![image](.attachments/9d33b5872d981c1028352994bb670d91ba8acc2d.png)
- schemachange/LiquidBase

#### declarative approach with CREATE OR ALTER

- ```sql
  CREATE OR ALTER
  ```
- Create or alter allows you to create an object if it doesn't exist, ​or modify it in place by applying only new changes to that object without dropping the object. ​It's pretty neat, right?
- means we can use declarative approach
- ![image](.attachments/c406cc8f164f46f9a4cca3df473ccaf84089ae5e.png)
- if need to change definition of object make changes there
- ![image](.attachments/efb1657bf03b24da2894c9fa08ba90667c875d43.png)
- snowflake recognizes new changes
- associations on object stay intact
- Now, be careful using this command. ​It's so powerful that you can also introduce changes like removing object properties, ​which could affect data. ​For example, if a modification to a create or alter table statement calls for dropping a column, ​any data that was in that column will also be dropped. ​You can, of course, recover that data using Snowflake's time travel feature, ​but I call this out to help you be mindful when wielding this powerful statement. ​A

- ![image](.attachments/23da22b72eb8dfd41e6374a641c2c730f16f2400.png)
- ![image](.attachments/6f990cc9dc2547db6162a834f02b2b44b344e198.png)
- create notebook from branch
- ```bash
  git fetch
  git add -p
  ```

  #### continuous integration and continuous delivery (CI/CD)

  - ![image](.attachments/fad1312b599b850db46878cc39a01d8297d3e29c.png)
  - ![image](.attachments/114cd2bf968fc1a6cc9c49a0e2c24e46ace9373e.png)
  - teams will have different environments (STG and PROD)
  - ![image](.attachments/ddfb5e0991049ca054e92e47bd03567a428bbf3a.png)
  - ![image](.attachments/b2fc20690dc5a3271e9c29f2163d3db1c4023db2.png)
  - ![image](.attachments/9c81d78e16c1139579aeae4308b0933e64f79cd8.png)
  - ![image](.attachments/603a6b44e1b7254f87953c64fc2c506cf442c754.png)
  - ![image](.attachments/4a1719daf2468d0ceb57647c5e593491279559ab.png)
  - 