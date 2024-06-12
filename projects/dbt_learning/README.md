### Data Build Tool (DBT)
DBT mainly uses 3 languages SQL to create models and tests, yaml for configguration files  and jinja to make yamly and sql code dynamic and reusable.

Preffered because it brings all the jennesequah of software engineering lke ci/cd, testing and dry into data transformation.

### Jinja
Anything starting and ending with {%%} is called a controll stamement eg if statements {%if amount <= 500:%}... {% endif %}
Anything that {{variable_a}} are calle expressions. They are usually evaluated and its output printed elsewhere.
Aything written without curly braces is called text eg SELECT/ FROM/ UNION.
{# Commentss #} - just for our understanding.

Install the dbt power user and python extensions
pip install dbt-bigquery
make a dbt folder in the user home directory ie mkdir $home\.dbt
initialise your first project using `dbt init` and follow the prompts.
dbt debug to confirm the connection

The macros folder contains sql queries used to perform transformaions on your data
To run various queries therefore you need to understand sql and ctes for dbt uses this mostly to create queries.
Once you are done creating your query in an sql file run `dbt run` on the terminal.

If the execution is successful you will see a view of the model created in the warehouse you are using. You can change the view to a table in the .yaml file if you want(ie the file containing all the dbt configuration)

**Modularity** breaking down a complex model with various lines of sql queries into smaller models then joining them to one. This increases reusability, maintainability, testing and readability. Save the small modules with a `_stg` to be able to differentiate them from the main one eg **customer_stg.sql**. Note we make the consious decision to materialise them as views since they contain minor transformation which are executed at runtime and do not occupy additional storage. Our main file however will still be saved as a table since it contains intensive transformations.
After creating the modules we reffer to them as `{{ref customer_stg.sql}} cu the same way we shall reffer another table that we are joining.

Note other macros that we are creating eg an **orders_fact.sql** which is totally not related to the customer but we need it for joining we still materialise it as a table and attach a fact to its name as shown. 

So all the above we can join them as a table called **customer_revenue.sql** Which will now become our dim.

We can manipulate this function by changing our .yaml models option

### Seeds 
csv files that can be easily loaded into dbt using the `dbt seeds ` command. They are particularly useful when working with static data that is relatively small. Create a seeds folder and paste the csv file in it thats it!! Now issue the `dbt seed` command and when it runs successfully check if the table is created in your warehouse. The seed file using the ref function and the file name without csv. NOte not the whole path just its name.

### Analysis folder
Allows us to create SQL files just like the models folder.
The files are however not materialised in the data warehouse.
They are used to run adhoc(on-demand) queries ontop of our data warehouse using dbt code. Think of it as the place you run your sql queries in your data warehouse but now on dbt. It allows us to uterlise dbt syntax for querying and analysing our data.
It is simply executed with the run button to view the results. 
The queries here or files are compiled which enables us to validate the code incase we choose to push the file to the models folder.
The `dbt compile` command will compile the sql file. Running the `dbt run` command will run the query but it will not be materialised in the data warehouse as a table or a view.

### Sources
Enables us to change the source table name of our existing data if need be. CChecking the models, each file refferences a spesific tablein its queries, to change that table we use sources and create a .yaml file in the models folder and add the sorce of your data. In the sql file you can now refference it using `{{ source('name', 'table_name')}}`


### Tests
Play a crutial role in mainatining data accuracy and integrity.
**Singular Tests:** Created within the queries and can onljy be used there ie where c1 <= c2
**Generic Tests:** Where a particular test is to be done multiple times in multiple queries. Due to DRY rule we parametarise the table and column_names and define a test query to be executed

```
{% test not_empty(model, col_name)%}
   select {{col_name}}
   from {{model}}
   where TRIM({{col_name}}) = ''
{% endtest %}
```

Dbt has 4 pre-built generic tests not_null, unique, relationships, accepted_values.
Create the test folder, then the sql file for the test, referencing the table as above then run `dbt test`
For generic tests, you can create a test file eg **test1.sql** file in the macros folder or just define it in the test -> generic folder then write a query like the one shown above.
In the models folder, create a yaml file then models, columns, tests. You can also configure built in generic tests here cause they are already defined.
These tests can also be applied to sources. Additionally, sources can also be checked or freshness. The `dbt source freshess` commad is uterlised to initiate this check. In a production environment it can be initialised to run prio to the main models to ensure that the models being run are up to date.

### Dbt Docs
It improves communication and makes your project more understandable.
Add descriptios to yaml files to increase the documentation process. Add a description to a model or a column.
Another way is to use doc blocks in the models folder eg `custdocs.md` 
Then write:

{% dosc StatusDoc %}

Whatever you want to document
{% enddocs %}

After defining it like this you can then call it in the .yaml file description as **description: "{{ doc('StatusDoc') }}"**

Once satisfied generate the docs using `dbt docs generate`
To make this documetation accessible to everyone, use the command `dbt docs serve`

### Dbt Macros
Used to perform spesific tasks just like functions in other well known languages.
They encapsulate the complexities of other tasks or logic / reusing a particular code block on multiple models.
Its like defining a function and when you want to use it in a model you just call it.
They are defined using the macro jinja tags as shown

```
{% macro add_amounts(old, new) %}
   ROUND(AVG({{old}} + {{new}}), 2)
{% endmacro %}

```
Our model  will now be:

```
SELECT Product,
       {{add_amounts('old', 'new')}} as avg_tt
FROM {{ref table_name}}
```

You can define multiple macros in the same file instead of creating a file for each every time then just call the one you need as shown above.

### Dbt Packages
Projects pre-built by someone to adress common use cases.
Visit `hub.getdbt.com` to view the packages. One popular one to use is `dbt_utils` and `dbt_expectations` 
Ensure to execute `dbt deps` after any package configuration changes.

### Dbt Materialisations
They includeways to include dbt models into a warehouse ie after running the `dbt run` command.
The common maerializations we know include, tables and views. There are three more **ephemeral(a cte), incremantal(just the updated data) and snapshot**
**Snapshot** just as the name suggests are used to capture historical changes in the data over time
Thier implementation differs from other materialisation:

```
{% snapshot snapshot_name %}
{{config(
    target_schema='schema_name',
    unique_key='col_name',
    strategy='timestamp' / 'check',
    updated_at='updated_at' / check_cols=['phone_no', 'email']
)}}
SQL query to run....
{% endsnapshot %}
```
Create a sql file inside a snapshot folder, encapsuate the code in the jinja syntax above , then run `dbt snapshot`
You can use the timestamp or check strategy 

You can change how a model is materialised using the  **{{ config(materialized='view') }}** before writting the query or in the .yaml file , models parameter as shown before.

To run a spesific model `dbt run --select model`

In the case of the incremental approach we have to define a macro that checks for new records. ie right below the query
{% if is_incremental() %}
where ... eg where col_name > date_add(day, - 7, current_date) {# past week #}
{% endif %}

the **is_incremental** macro returns true if the model is materialised as incremental, dbt is not running on full refresh and the table already exists.

Reffer to video for more details

### Productionise using Airflow



