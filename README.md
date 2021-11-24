# INCUBYTE

Technolgis uses for This Project is Snowflake WareHouse and Azure Data Lake.
in Snowflake warehouse is destination and Azure data Lake is source .

Script and Description Of INCUBYTE Assessment.

STEP 1:-
Load test files to azure container,and that container link with Snowflake Stages.
STAGE:-STAGE_TEST
FILE_FORMAT:-TEST

STEP 2:-
Create Config Table of COUNTRY_TABLE.
in this table store details about country name and code.

STEP 3:-
Create three tables of INCUBYTE_TEST which intermediate table,where stores all the datas of file and bifarcate to child talbes.
TEMP_INCUBYTE_TEST,STG_INCUBYTE_TEST Tale uses for inner operations ,data checking,removing duplicate,handle incremental load.
Create view for removing duplicates.

STEP 4:-
create target tables(country),where data is stores according to business rules.
like TABLE_AU,TABLE_IND,TABLE_USA etc.

STEP 5:-
Run procedure SP_LOAD_INCUBYTE_TEST() , Which load the data from the containers to target tables.

