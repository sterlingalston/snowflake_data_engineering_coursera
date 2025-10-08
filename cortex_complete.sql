---> use the mistral-7b model and Snowflake Cortex Complete to ask a question
SELECT SNOWFLAKE.CORTEX.COMPLETE(
    'mistral-7b', 'What are three reasons that Snowflake is positioned to become the go-to data platform?');

---> now send the result to the Snowflake Cortex Summarize function
SELECT SNOWFLAKE.CORTEX.SUMMARIZE(SNOWFLAKE.CORTEX.COMPLETE(
    'mistral-7b', 'What are three reasons that Snowflake is positioned to become the go-to data platform?'));

---> run Snowflake Cortex Complete on multiple rows at once
SELECT SNOWFLAKE.CORTEX.COMPLETE(
    'mistral-7b',
        CONCAT('Tell me why this food is tasty: ', menu_item_name)
) FROM FROSTBYTE_TASTY_BYTES.RAW_POS.MENU LIMIT 5;

---> check out what the table of prompts we’re feeding to Complete (roughly) looks like
SELECT CONCAT('Tell me why this food is tasty: ', menu_item_name)
FROM FROSTBYTE_TASTY_BYTES.RAW_POS.MENU LIMIT 5;

---> give Snowflake Cortex Complete a prompt with history
SELECT SNOWFLAKE.CORTEX.COMPLETE(
    'mistral-7b', -- the model you want to use
    [
        {'role': 'system', 
        'content': 'Analyze this Snowflake review and determine the overall sentiment. Answer with just \"Positive\", \"Negative\", or \"Neutral\"' },
        {'role': 'user',
        'content': 'I love Snowflake because it is so simple to use.'}
    ], -- the array with the prompt history, and your new prompt
    {} -- An empty object of options (we're not specify additional options here)
) AS response;

---> give Snowflake Cortex Complete a prompt with a lengthier history
SELECT SNOWFLAKE.CORTEX.COMPLETE(
    'mistral-7b',
    [
        {'role': 'system', 
        'content': 'Analyze this Snowflake review and determine the overall sentiment. Answer with just \"Positive\", \"Negative\", or \"Neutral\"' },
        {'role': 'user',
        'content': 'I love Snowflake because it is so simple to use.'},
        {'role': 'assistant',
        'content': 'Positive. The review expresses a positive sentiment towards Snowflake, specifically mentioning that it is \"so simple to use.\'"'},
        {'role': 'user',
        'content': 'Based on other information you know about Snowflake, explain why the reviewer might feel they way they do.'}
    ], -- the array with the prompt history, and your new prompt
    {} -- An empty object of options (we're not specify additional options here)
) AS response;

/*

I'd be happy to explain the roles of **system**, **assistant**, and **user** in the context of AI conversation frameworks like those used by language models.

## The Three Core Roles

**System Role**
The system role contains the foundational instructions and context that guide the AI's behavior throughout a conversation. This includes personality traits, ethical guidelines, knowledge about capabilities and limitations, and specific instructions about how to respond. Think of it as the "constitution" or "operating manual" for the AI - it establishes the ground rules and core identity that persist across all interactions.

**Assistant Role**
This represents the AI's responses and contributions to the conversation. When I respond to you right now, that's the assistant role in action. It encompasses all the AI's outputs - answers, questions, creative content, analysis, and any other form of communication the AI provides to the human.

**User Role**
This represents the human participant's inputs - their questions, requests, statements, and any other content they contribute to the conversation. It's everything coming from the person interacting with the AI system.

## How They Work Together

These roles create a structured dialogue framework where:
- The **system** provides consistent behavioral guidelines
- The **user** initiates requests and provides context
- The **assistant** responds appropriately based on both the user's input and the system's parameters

This role-based structure helps maintain coherent, purposeful conversations while ensuring the AI behaves consistently according to its designed parameters. It's essentially a way to organize and manage the different types of information flowing through an AI conversation system.

 */