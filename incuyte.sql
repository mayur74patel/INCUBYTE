
create or replace TABLE COUNTRY_TABLE (
	TABLENAME VARCHAR(16777216),
	CONTRY_CODE VARCHAR(16777216),
	CONTRY_NAME VARCHAR(16777216)
);
INSERT INTO COUNTRY_TABLE VALUES('TABLE_USA ','USA','USA');
INSERT INTO COUNTRY_TABLE VALUES('TABLE_IND','IND','INDIA');
INSERT INTO COUNTRY_TABLE VALUES('TABLE_PHIL','PHIL','PHILIPINES');
INSERT INTO COUNTRY_TABLE VALUES('TABLE_NYC','NYC','NEW YORK');
INSERT INTO COUNTRY_TABLE VALUES('TABLE_AU','AU','AUSTRALIA');

create or replace TABLE INCUBYTE_TEST (
	CUSTOMER_NAME VARCHAR(255),
	CUSTOMER_ID VARCHAR(18) NOT NULL,
	OPEN_DATE VARCHAR(16777216),
	LAST_CONSULTED_DATE VARCHAR(16777216),
	VACCINATION_ID VARCHAR(5),
	DR_NAME VARCHAR(255),
	STATE VARCHAR(5),
	COUNTRY VARCHAR(5),
	DOB VARCHAR(16777216),
	IS_ACTIVE VARCHAR(1),
	INSERTED_DATE TIMESTAMP_LTZ(9),
	primary key (CUSTOMER_ID)
);

create or replace TABLE TEMP_INCUBYTE_TEST (
	CUSTOMER_NAME VARCHAR(255),
	CUSTOMER_ID VARCHAR(18) NOT NULL,
	OPEN_DATE VARCHAR(16777216),
	LAST_CONSULTED_DATE VARCHAR(16777216),
	VACCINATION_ID VARCHAR(5),
	DR_NAME VARCHAR(255),
	STATE VARCHAR(5),
	COUNTRY VARCHAR(5),
	DOB VARCHAR(16777216),
	IS_ACTIVE VARCHAR(1),
	INSERTED_DATE TIMESTAMP_LTZ(9),
	primary key (CUSTOMER_ID)
);


create or replace TABLE STG_INCUBYTE_TEST (
	CUSTOMER_NAME VARCHAR(255),
	CUSTOMER_ID VARCHAR(18) NOT NULL,
	OPEN_DATE VARCHAR(16777216),
	LAST_CONSULTED_DATE VARCHAR(16777216),
	VACCINATION_ID VARCHAR(5),
	DR_NAME VARCHAR(255),
	STATE VARCHAR(5),
	COUNTRY VARCHAR(5),
	DOB VARCHAR(16777216),
	IS_ACTIVE VARCHAR(1),
	INSERTED_DATE TIMESTAMP_LTZ(9),
	primary key (CUSTOMER_ID)
);

create or replace view vw_incute_test as
select * from (select *,row_number()  over (partition by customer_id order by inserted_date )  as row_number from INCUYTE_TEST)  where row_number=1;


create or replace TABLE TABLE_USA (
	CUSTOMER_NAME VARCHAR(255),
	CUSTOMER_ID VARCHAR(18) NOT NULL,
	OPEN_DATE DATE,
	LAST_CONSULTED_DATE DATE,
	VACCINATION_ID VARCHAR(5),
	DR_NAME VARCHAR(255),
	STATE VARCHAR(5),
	COUNTRY VARCHAR(5),
	DOB DATE,
	IS_ACTIVE VARCHAR(1),
	INSERTED_DATE TIMESTAMP_LTZ(9),
	primary key (CUSTOMER_ID)
);
create or replace view VW_TABLE_USA as
select * from (select *,row_number()  over (partition by customer_id order by inserted_date )  as row_number from TABLE_USA)  where row_number=1;

create or replace TABLE TABLE_IND (
	CUSTOMER_NAME VARCHAR(255),
	CUSTOMER_ID VARCHAR(18) NOT NULL,
	OPEN_DATE DATE,
	LAST_CONSULTED_DATE DATE,
	VACCINATION_ID VARCHAR(5),
	DR_NAME VARCHAR(255),
	STATE VARCHAR(5),
	COUNTRY VARCHAR(5),
	DOB DATE,
	IS_ACTIVE VARCHAR(1),
	INSERTED_DATE TIMESTAMP_LTZ(9),
	primary key (CUSTOMER_ID)
);

create or replace view VW_TABLE_IND as
select * from (select *,row_number()  over (partition by customer_id order by inserted_date )  as row_number from TABLE_IND)  where row_number=1;

create or replace TABLE TABLE_PHIL (
	CUSTOMER_NAME VARCHAR(255),
	CUSTOMER_ID VARCHAR(18) NOT NULL,
	OPEN_DATE DATE,
	LAST_CONSULTED_DATE DATE,
	VACCINATION_ID VARCHAR(5),
	DR_NAME VARCHAR(255),
	STATE VARCHAR(5),
	COUNTRY VARCHAR(5),
	DOB DATE,
	IS_ACTIVE VARCHAR(1),
	INSERTED_DATE TIMESTAMP_LTZ(9),
	primary key (CUSTOMER_ID)
);

create or replace view VW_TABLE_PHIL as
select * from (select *,row_number()  over (partition by customer_id order by inserted_date )  as row_number from TABLE_PHIL)  where row_number=1;


create or replace TABLE TABLE_NYC (
	CUSTOMER_NAME VARCHAR(255),
	CUSTOMER_ID VARCHAR(18) NOT NULL,
	OPEN_DATE DATE,
	LAST_CONSULTED_DATE DATE,
	VACCINATION_ID VARCHAR(5),
	DR_NAME VARCHAR(255),
	STATE VARCHAR(5),
	COUNTRY VARCHAR(5),
	DOB DATE,
	IS_ACTIVE VARCHAR(1),
	INSERTED_DATE TIMESTAMP_LTZ(9),
	primary key (CUSTOMER_ID)
);


create or replace view VW_TABLE_NYC as
select * from (select *,row_number()  over (partition by customer_id order by inserted_date )  as row_number from TABLE_NYC)  where row_number=1;

create or replace TABLE TABLE_AU (
	CUSTOMER_NAME VARCHAR(255),
	CUSTOMER_ID VARCHAR(18) NOT NULL,
	OPEN_DATE DATE,
	LAST_CONSULTED_DATE DATE,
	VACCINATION_ID VARCHAR(5),
	DR_NAME VARCHAR(255),
	STATE VARCHAR(5),
	COUNTRY VARCHAR(5),
	DOB DATE,
	IS_ACTIVE VARCHAR(1),
	INSERTED_DATE TIMESTAMP_LTZ(9),
	primary key (CUSTOMER_ID)
);


create or replace view VW_TABLE_AU as
select * from (select *,row_number()  over (partition by customer_id order by inserted_date )  as row_number from TABLE_AU)  where row_number=1;



create or replace procedure sp_load_incubyte_test()
returns varchar
language javascript
execute as owner
 as $$
 try{

	var my_sql_command1_0='DELETE FROM STG_INCUBYTE_TEST';
	var statement1_0=snowflake.createStatement({sqlText: my_sql_command1_0});
	statement1_0.execute();

    var my_sql_command1=`copy into STG_INCUBYTE_TEST (CUSTOMER_NAME, CUSTOMER_ID, OPEN_DATE, LAST_CONSULTED_DATE, VACCINATION_ID, DR_NAME, STATE, COUNTRY, DOB, IS_ACTIVE, INSERTED_DATE) from 
	(select T.$3, T.$4, T.$5,T.$6, T.$7, T.$8, T.$9, T.$10, T.$11, T.$12,current_timestamp() from @STAGE_TEST T) 
	FILE_FORMAT=(FORMAT_NAME='TEST')`;
	var statement1=snowflake.createStatement({sqlText: my_sql_command1});
	statement1.execute();

	var my_sql_command2_0='TRUNCATE TABLE TEMP_INCUBYTE_TEST';
	var statement2_0=snowflake.createStatement({sqlText: my_sql_command2_0});
	statement2_0.execute();

    var my_sql_command2=`create or replace table TEMP_INCUBYTE_TEST as  (
    SELECT CUSTOMER_NAME, CUSTOMER_ID, OPEN_DATE, LAST_CONSULTED_DATE, VACCINATION_ID, DR_NAME, STATE, COUNTRY, DOB, IS_ACTIVE,CURRENT_TIMESTAMP()  AS INSERTED_DATE
    FROM (
    SELECT CUSTOMER_NAME, CUSTOMER_ID, OPEN_DATE, LAST_CONSULTED_DATE, VACCINATION_ID, DR_NAME, STATE, COUNTRY, DOB, IS_ACTIVE FROM STG_INCUBYTE_TEST
    MINUS
    SELECT CUSTOMER_NAME, CUSTOMER_ID, OPEN_DATE, LAST_CONSULTED_DATE, VACCINATION_ID, DR_NAME, STATE, COUNTRY, DOB, IS_ACTIVE FROM INCUBYTE_TEST)
    )`;
	var statement2=snowflake.createStatement({sqlText: my_sql_command2});
	statement2.execute();


    var my_sql_command3=`insert into INCUBYTE_TEST 
    select CUSTOMER_NAME, CUSTOMER_ID, OPEN_DATE, LAST_CONSULTED_DATE, VACCINATION_ID, DR_NAME, STATE, COUNTRY, DOB, IS_ACTIVE,CURRENT_TIMESTAMP() AS INSERTED_DATE FROM TEMP_INCUBYTE_TEST`;
    var statement3=snowflake.createStatement({sqlText: my_sql_command3});
    statement3.execute();

	var my_sql_command4=`select CUSTOMER_NAME, CUSTOMER_ID, OPEN_DATE, LAST_CONSULTED_DATE, VACCINATION_ID, DR_NAME, STATE, COUNTRY, DOB, IS_ACTIVE from TEMP_INCUBYTE_TEST`;
	var statement4=snowflake.createStatement({sqlText: my_sql_command4});
	result1=statement4.execute();

	while(result1.next()){
		var	 CUSTOMER_NAME =	result1.getColumnValue(1);
		var	 CUSTOMER_ID =	result1.getColumnValue(2);
		var	 OPEN_DATE =	result1.getColumnValue(3);
		var	 LAST_CONSULTED_DATE =	result1.getColumnValue(4);
		var	 VACCINATION_ID =	result1.getColumnValue(5);
		var	 DR_NAME =	result1.getColumnValue(6);
		var	 STATE =	result1.getColumnValue(7);
		var	 COUNTRY =	result1.getColumnValue(8);
		var	 DOB =	result1.getColumnValue(9);	
		var	 IS_ACTIVE =	result1.getColumnValue(10);

	var my_sql_command5=`select TABLENAME from COUNTRY_TABLE WHERE CONTRY_CODE='`+COUNTRY+`';`;
	var statement5=snowflake.createStatement({sqlText: my_sql_command5});
	result2=statement5.execute();
	result2.next();
	var TABLENAME=result2.getColumnValue(1);

	var my_sql_command6=`insert into `+TABLENAME+` select '`+CUSTOMER_NAME+`','`+CUSTOMER_ID+`',to_date('`+OPEN_DATE+`\',\'YYYYMMDD\'),`+`to_date('`+LAST_CONSULTED_DATE+`\',\'YYYYMMDD\'),'`+VACCINATION_ID+`','`+DR_NAME+`','`+STATE+`','`+COUNTRY+`',`+`to_date('`+DOB+`\',\'DDMMYYYY\')`+`,'`+IS_ACTIVE+`',current_timestamp() `;
	var statement6=snowflake.createStatement({sqlText: my_sql_command6});
	result2=statement6.execute();
	}
	return 'Success';
} catch(err){
return(err)
}
 $$;



select * from VW_INCUBYTE_TEST;
select * from VW_TABLE_AU;
select * from VW_TABLE_IND;
select * from VW_TABLE_NYC;
select * from VW_TABLE_PHIL;
select * from VW_TABLE_USA;