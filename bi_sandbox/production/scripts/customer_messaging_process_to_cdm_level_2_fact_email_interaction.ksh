#!/bin/ksh
#######################################################################
# Script :      customer_messaging_process_to_cdm_level_2_fact_email_interaction.ksh 
# Description : This script takes data from the following RDW table:
#					RDW..CUSTOMER_MESSAGING_FAIL, RDW..CUSTOMER_MESSAGING_SENT, RDW..CUSTOMER_MESSAGING_SKIPPED, RDW..CUSTOMER_MESSAGING_BOUNCE,
#					RDW..CUSTOMER_MESSAGING_OPEN, RDW..CUSTOMER_MESSAGING_CLICK, RDW..CUSTOMER_MESSAGING_CONVERT, RDW..CUSTOMER_MESSAGING_COMPLAINT,
#					RDW..CUSTOMER_MESSAGING_OPT_IN, RDW..CUSTOMER_MESSAGING_OPT_OUT, RDW..CUSTOMER_MESSAGING_BOUNCELOG, RDW..CUSTOMER_MESSAGING_REALTIMEMSGLOG,
#					RDW..CUSTOMER_MESSAGING_SPAMCOMPLAINTLOG, RDW..CUSTOMER_MESSAGING_UNSUBSCRIBEDLOG
#				and loads to cdc, transform, and cdm tables:
#					STGCDM..CM_EMAIL_INTERACTION_FACT_CDC, STGCDM..CM_EMAIL_INTERACTION_FACT_TRANSFORM, CDM..CM_EMAIL_INTERACTION_FACT_TRANSFORM
#
#
# Modifications :
# 1.0  2013-Mar-10	REI	ichon	 : Creation
# 2.0  2015-Jun-25   	smohamm : Replaced DELETE statement with TRUNCATE statement for CDM tables
# 3.0  2015-Jul-28		smohamm : Changed the script to load the presentation table from CDC rather than TRANSFORM and the email key update is done on the presentation table. Converted
#                                    Full load into incremental load strategy. Added a new variable that accepts the JUB_RUN_ID of the RDW wrapper scripts.   
#4.0   2016-Mar-03   Juganu         : Changes as per eReceipt project. Added three new columns order_header_key, order_id, and surrogate for table. 
#5.0   2016-Mar-16   Juganu         : Changes as per QA result. 
#6.0   2016-Mar-23   Juganu         : Late Arrival Transaction Order_header_key update 
#######################################################################

export SCRIPT_NAME=$(basename  $0)
export SCRIPT_VERSION="1.0"

. $SCRIPTS_DIR/load_parameters.ksh
. $SCRIPTS_DIR/load_library.ksh
. $SCRIPTS_DIR/load_datastage_params.ksh


export return_code_fail=3

export cm_platform_key_contact=1  


  
print_msg "0001 Processing started for script $SCRIPT_NAME Version $SCRIPT_VERSION"
	if [[ $verbose_logging = 1 ]]; then
		print_msg "  METAMART database name = $NZ_METAMART_DB"		
		print_msg "  RDW database name = $NZ_RDW_DB"
		print_msg "  STGCDM database name = $NZ_STG_DB"
		print_msg "  CDM database name = $NZ_PRSNT_DB"
	fi



print_msg "0002 Accepting parameters passed into this script file"
	while getopts ":j:i:x" arg  
	do 
		case $arg in
			j ) job_run_id=$OPTARG;;
			i ) PRED_JOB_IDS=$OPTARG;;
			\?) print '$SCRIPT_NAME: -j job_run_id -i PRED_JOB_IDS'
		esac
	done

	if [[ $verbose_logging = 1 ]]; then
		print_msg "  Current Job_Run_ID is $job_run_id"
		print_msg "  All Pred_Job_Run_ID's are  $PRED_JOB_IDS"
	fi




print_msg "0010 Starting to truncate all records in level 2 cdc email interaction CDC table in $NZ_STG_DB"	
	export CLEAR_CDC_TABLES_SQL="
		TRUNCATE TABLE CM_EMAIL_INTERACTION_FACT_CDC;"

			
	if [[ $verbose_logging = 1 ]]; then
		print_msg "$CLEAR_CDC_TABLES_SQL"
	fi
	
	run_query -d $NZ_STG_DB -q "$CLEAR_CDC_TABLES_SQL" -m "0011 Failed while truncating records in level 2 cdc table"
	if [[ $? != 0 ]]
	then 
		perl endJobRun.pl "$job_run_id" "FAILURE"
		print_msg $return_code_fail -m "0012 *****ERROR***** Failed while truncating records in level 2 cdc table in $NZ_STG_DB.  $SCRIPT_NAME finished UNSUCCESSFULLY."
		exit $return_code_fail
	else
		print_msg "0013 All records in level 2 cdc email table truncated"
	fi

	

print_msg "0020 Populating Email Interaction CDC table with good rows from Fail RDW"
	export CDC_EMAIL_INTERACTION_SQL2="
		INSERT INTO CM_EMAIL_INTERACTION_FACT_CDC
		(	ORDER_ID, ORDER_HEADER_KEY, EVENT_DATE_KEY, CM_PLATFORM_KEY, RIID, EMAIL_KEY, CM_EMAIL_ADDRESS_KEY,
			CM_LIST_KEY, LIST_ID, CM_CAMPAIGN_KEY, CAMPAIGN_ID, CM_LAUNCH_KEY, LAUNCH_ID, SEGMENT_INFO,
			FAIL_COUNT, SENT_COUNT, SKIPPED_COUNT, BOUNCE_COUNT, OPEN_COUNT, 
			CLICK_COUNT, CONVERT_COUNT, COMPLAINT_COUNT, OPT_IN_COUNT, OPT_OUT_COUNT, RESPOND_COUNT,
			JOB_RUN_ID, M_INSERT_DATETIME	)
		SELECT CASE 
                      WHEN R.TRANSACTIONKEY = '-2' THEN NULL
                      WHEN R.SOURCESYSTEM = 'POS' THEN TRIM(TRIM(R.TRANSACTIONKEY)||'-'||LPAD(1,5,'0'))
			 ELSE R.TRANSACTIONKEY
			END AS ORDER_ID
			 , COALESCE(OH.ORDER_HEADER_KEY,-2) AS ORDER_HEADER_KEY,
			   R.EVENT_CAPTURED_DATE_KEY AS EVENT_DATE_KEY,
			   R.CM_PLATFORM_KEY,
			   R.RIID,
			   CASE WHEN CAST(R.EMAIL_KEY AS INTEGER) <> -2 THEN CAST(R.EMAIL_KEY AS INTEGER)
					WHEN R.EMAIL_ADDR_KEY <> -2 THEN R.EMAIL_ADDR_KEY
					WHEN R.EMAIL_ADDRESS_KEY <> -2 THEN R.EMAIL_ADDRESS_KEY
					WHEN R.CONTACT_INFO_KEY <> -2 THEN R.CONTACT_INFO_KEY 
					ELSE -2 END AS EMAIL_KEY,
			   CASE WHEN R.CM_EMAIL_ADDR_KEY <> -2 THEN R.CM_EMAIL_ADDR_KEY
					WHEN R.CM_EMAIL_ADDRESS_KEY <> -2 THEN R.CM_EMAIL_ADDRESS_KEY
					WHEN R.CM_CONTACT_INFO_KEY <> -2 THEN R.CM_CONTACT_INFO_KEY 
					ELSE -2 END AS CM_EMAIL_ADDRESS_KEY,
			   R.CM_LIST_KEY, 
			   R.LIST_ID,
			   R.CM_CAMPAIGN_KEY, 
			   R.CAMPAIGN_ID,
			   R.CM_LAUNCH_KEY, 
			   R.LAUNCH_ID,
			   '-2' AS SEGMENT_INFO,
			   1 AS FAIL_COUNT, 
			   0 AS SENT_COUNT, 
			   0 AS SKIPPED_COUNT, 
			   0 AS BOUNCE_COUNT, 
			   0 AS OPEN_COUNT,
			   0 AS CLICK_COUNT, 
			   0 AS CONVERT_COUNT, 
			   0 AS COMPLAINT_COUNT, 
			   0 AS OPT_IN_COUNT, 
			   0 AS OPT_OUT_COUNT,
			   0 AS RESPOND_COUNT,
			   $job_run_id AS JOB_RUN_ID,
			   CURRENT_TIMESTAMP AS M_INSERT_DATETIME 
		FROM $NZ_RDW_DB..CUSTOMER_MESSAGING_FAIL R LEFT JOIN $NZ_PRSNT_DB.$NZ_PRSNT_SCHEMA.ORDER_HEADER OH 
		                                                  ON CASE
                                                              WHEN R.SOURCESYSTEM = 'POS' 
                                                              THEN TRIM(TRIM(R.TRANSACTIONKEY)||'-'||LPAD(1,5,'0')) 
															  ELSE R.TRANSACTIONKEY
															  END = OH.ORDER_ID
			WHERE  R.JOB_RUN_ID IN $PRED_JOB_IDS;
		"
echo ''
echo $CDC_EMAIL_INTERACTION_SQL2
echo ''

	if [[ $verbose_logging = 1 ]]; then
		print_msg "$CDC_EMAIL_INTERACTION_SQL2"
	fi

	run_query -d $NZ_STG_DB -q "$CDC_EMAIL_INTERACTION_SQL2" -m "0021 Failed while inserting fail data into cdc table"
	if [[ $? != 0 ]]
	then 
		perl endJobRun.pl "$job_run_id" "FAILURE"
		print_msg "0022 *****ERROR***** Failed while inserting fail data into cdc table in $NZ_STG_DB.  $SCRIPT_NAME finished UNSUCCESSFULLY."
		exit $return_code_fail
	fi

	
print_msg "0030 Populating Email Interaction CDC table with good rows from Sent RDW"
	export CDC_EMAIL_INTERACTION_SQL3="
		INSERT INTO CM_EMAIL_INTERACTION_FACT_CDC
		(	ORDER_ID, ORDER_HEADER_KEY, EVENT_DATE_KEY, CM_PLATFORM_KEY, RIID, EMAIL_KEY, CM_EMAIL_ADDRESS_KEY,
			CM_LIST_KEY, LIST_ID, CM_CAMPAIGN_KEY, CAMPAIGN_ID, CM_LAUNCH_KEY, LAUNCH_ID, SEGMENT_INFO,
			FAIL_COUNT, SENT_COUNT, SKIPPED_COUNT, BOUNCE_COUNT, OPEN_COUNT, 
			CLICK_COUNT, CONVERT_COUNT, COMPLAINT_COUNT, OPT_IN_COUNT, OPT_OUT_COUNT, RESPOND_COUNT,
			JOB_RUN_ID, M_INSERT_DATETIME	)
		SELECT CASE 
                      WHEN R.TRANSACTIONKEY = '-2' THEN NULL
                      WHEN R.SOURCESYSTEM = 'POS' THEN TRIM(TRIM(R.TRANSACTIONKEY)||'-'||LPAD(1,5,'0'))
			 ELSE R.TRANSACTIONKEY
			END AS ORDER_ID
			, COALESCE(OH.ORDER_HEADER_KEY,-2) ORDER_HEADER_KEY,
			   R.EVENT_CAPTURED_DATE_KEY AS EVENT_DATE_KEY,
			   R.CM_PLATFORM_KEY,
			   R.RIID,
			   CASE WHEN CAST(R.EMAIL_KEY AS INTEGER) <> -2 THEN CAST(R.EMAIL_KEY AS INTEGER)
					WHEN R.EMAIL_ADDR_KEY <> -2 THEN R.EMAIL_ADDR_KEY
					WHEN R.EMAIL_ADDRESS_KEY <> -2 THEN R.EMAIL_ADDRESS_KEY
					WHEN R.CONTACT_INFO_KEY <> -2 THEN R.CONTACT_INFO_KEY 
					ELSE -2 END AS EMAIL_KEY,
			   CASE WHEN R.CM_EMAIL_ADDR_KEY <> -2 THEN R.CM_EMAIL_ADDR_KEY
					WHEN R.CM_EMAIL_ADDRESS_KEY <> -2 THEN R.CM_EMAIL_ADDRESS_KEY
					WHEN R.CM_CONTACT_INFO_KEY <> -2 THEN R.CM_CONTACT_INFO_KEY 
					ELSE -2 END AS CM_EMAIL_ADDRESS_KEY,
			   R.CM_LIST_KEY,
			   R.LIST_ID,
			   R.CM_CAMPAIGN_KEY,
			   R.CAMPAIGN_ID,
			   R.CM_LAUNCH_KEY,
			   R.LAUNCH_ID,
			   R.SEGMENT_INFO,
			   0 AS FAIL_COUNT, 
			   1 AS SENT_COUNT, 
			   0 AS SKIPPED_COUNT, 
			   0 AS BOUNCE_COUNT, 
			   0 AS OPEN_COUNT,
			   0 AS CLICK_COUNT, 
			   0 AS CONVERT_COUNT, 
			   0 AS COMPLAINT_COUNT, 
			   0 AS OPT_IN_COUNT, 
			   0 AS OPT_OUT_COUNT,
			   0 AS RESPOND_COUNT,
			   $job_run_id AS JOB_RUN_ID,
			   CURRENT_TIMESTAMP AS M_INSERT_DATETIME
		FROM $NZ_RDW_DB..CUSTOMER_MESSAGING_SENT R LEFT JOIN $NZ_PRSNT_DB.$NZ_PRSNT_SCHEMA.ORDER_HEADER OH 
		                                                  ON CASE
                                                              WHEN R.SOURCESYSTEM = 'POS' 
                                                              THEN TRIM(TRIM(R.TRANSACTIONKEY)||'-'||LPAD(1,5,'0')) 
															  ELSE R.TRANSACTIONKEY
															  END = OH.ORDER_ID
			WHERE  R.JOB_RUN_ID IN $PRED_JOB_IDS;
"

echo ''		
echo $CDC_EMAIL_INTERACTION_SQL3;
echo ''

	if [[ $verbose_logging = 1 ]]; then
		print_msg "$CDC_EMAIL_INTERACTION_SQL3"
	fi

	run_query -d $NZ_STG_DB -q "$CDC_EMAIL_INTERACTION_SQL3" -m "0031 Failed while inserting sent data into cdc table"
	if [[ $? != 0 ]]
	then 
		perl endJobRun.pl "$job_run_id" "FAILURE"
		print_msg "0032 *****ERROR***** Failed while inserting sent data into cdc table in $NZ_STG_DB.  $SCRIPT_NAME finished UNSUCCESSFULLY."
		exit $return_code_fail
	fi


print_msg "0040 Populating Email Interaction CDC table with good rows from Skipped RDW"
	export CDC_EMAIL_INTERACTION_SQL4="
		INSERT INTO CM_EMAIL_INTERACTION_FACT_CDC
		(	ORDER_ID, ORDER_HEADER_KEY, EVENT_DATE_KEY, CM_PLATFORM_KEY, RIID, EMAIL_KEY, CM_EMAIL_ADDRESS_KEY,
			CM_LIST_KEY, LIST_ID, CM_CAMPAIGN_KEY, CAMPAIGN_ID, CM_LAUNCH_KEY, LAUNCH_ID, SEGMENT_INFO,
			FAIL_COUNT, SENT_COUNT, SKIPPED_COUNT, BOUNCE_COUNT, OPEN_COUNT, 
			CLICK_COUNT, CONVERT_COUNT, COMPLAINT_COUNT, OPT_IN_COUNT, OPT_OUT_COUNT, RESPOND_COUNT,
			JOB_RUN_ID, M_INSERT_DATETIME	)
		SELECT CASE 
                      WHEN R.TRANSACTIONKEY = '-2' THEN NULL
                      WHEN R.SOURCESYSTEM = 'POS' THEN TRIM(TRIM(R.TRANSACTIONKEY)||'-'||LPAD(1,5,'0'))
			 ELSE R.TRANSACTIONKEY
			END AS ORDER_ID
			 , COALESCE(OH.ORDER_HEADER_KEY,-2) AS ORDER_HEADER_KEY,
			   R.EVENT_CAPTURED_DATE_KEY AS EVENT_DATE_KEY,
			   R.CM_PLATFORM_KEY,
			   R.RIID,
			   CASE WHEN CAST(R.EMAIL_KEY AS INTEGER) <> -2 THEN CAST(R.EMAIL_KEY AS INTEGER)
					WHEN R.EMAIL_ADDR_KEY <> -2 THEN R.EMAIL_ADDR_KEY
					WHEN R.EMAIL_ADDRESS_KEY <> -2 THEN R.EMAIL_ADDRESS_KEY
					WHEN R.CONTACT_INFO_KEY <> -2 THEN R.CONTACT_INFO_KEY 
					ELSE -2 END AS EMAIL_KEY,
			   CASE WHEN R.CM_EMAIL_ADDR_KEY <> -2 THEN R.CM_EMAIL_ADDR_KEY
					WHEN R.CM_EMAIL_ADDRESS_KEY <> -2 THEN R.CM_EMAIL_ADDRESS_KEY
					WHEN R.CM_CONTACT_INFO_KEY <> -2 THEN R.CM_CONTACT_INFO_KEY 
					ELSE -2 END AS CM_EMAIL_ADDRESS_KEY,
			   R.CM_LIST_KEY,
			   R.LIST_ID,
			   R.CM_CAMPAIGN_KEY,
			   R.CAMPAIGN_ID,
			   R.CM_LAUNCH_KEY,
			   R.LAUNCH_ID,
			   '-2' AS SEGMENT_INFO,
			   0 AS FAIL_COUNT, 
			   0 AS SENT_COUNT, 
			   1 AS SKIPPED_COUNT, 
			   0 AS BOUNCE_COUNT, 
			   0 AS OPEN_COUNT,
			   0 AS CLICK_COUNT, 
			   0 AS CONVERT_COUNT, 
			   0 AS COMPLAINT_COUNT, 
			   0 AS OPT_IN_COUNT, 
			   0 AS OPT_OUT_COUNT,
			   0 AS RESPOND_COUNT,
			   $job_run_id AS JOB_RUN_ID,
			   CURRENT_TIMESTAMP AS M_INSERT_DATETIME
		FROM $NZ_RDW_DB..CUSTOMER_MESSAGING_SKIPPED R LEFT JOIN $NZ_PRSNT_DB.$NZ_PRSNT_SCHEMA.ORDER_HEADER OH 
		                                                  ON CASE
                                                              WHEN R.SOURCESYSTEM = 'POS' 
                                                              THEN TRIM(TRIM(R.TRANSACTIONKEY)||'-'||LPAD(1,5,'0')) 
															  ELSE R.TRANSACTIONKEY
															  END = OH.ORDER_ID
			WHERE  R.JOB_RUN_ID IN $PRED_JOB_IDS;
		"
echo ''		
echo $CDC_EMAIL_INTERACTION_SQL4;
echo ''

	if [[ $verbose_logging = 1 ]]; then
		print_msg "$CDC_EMAIL_INTERACTION_SQL4"
	fi

	run_query -d $NZ_STG_DB -q "$CDC_EMAIL_INTERACTION_SQL4" -m "0041 Failed while inserting skipped data into cdc table"
	if [[ $? != 0 ]]
	then 
		perl endJobRun.pl "$job_run_id" "FAILURE"
		print_msg "0042 *****ERROR***** Failed while inserting skipped data into cdc table in $NZ_STG_DB.  $SCRIPT_NAME finished UNSUCCESSFULLY."
		exit $return_code_fail
	fi

	
print_msg "0050 Populating Email Interaction CDC table with good rows from Bounce RDW"
	export CDC_EMAIL_INTERACTION_SQL5="
		INSERT INTO CM_EMAIL_INTERACTION_FACT_CDC
		(	ORDER_ID, ORDER_HEADER_KEY, EVENT_DATE_KEY, CM_PLATFORM_KEY, RIID, EMAIL_KEY, CM_EMAIL_ADDRESS_KEY,
			CM_LIST_KEY, LIST_ID, CM_CAMPAIGN_KEY, CAMPAIGN_ID, CM_LAUNCH_KEY, LAUNCH_ID, SEGMENT_INFO,
			FAIL_COUNT, SENT_COUNT, SKIPPED_COUNT, BOUNCE_COUNT, OPEN_COUNT, 
			CLICK_COUNT, CONVERT_COUNT, COMPLAINT_COUNT, OPT_IN_COUNT, OPT_OUT_COUNT, RESPOND_COUNT,
			JOB_RUN_ID, M_INSERT_DATETIME	)
		SELECT CASE 
                      WHEN R.TRANSACTIONKEY = '-2' THEN NULL
                      WHEN R.SOURCESYSTEM = 'POS' THEN TRIM(TRIM(R.TRANSACTIONKEY)||'-'||LPAD(1,5,'0'))
			 ELSE R.TRANSACTIONKEY
			END AS ORDER_ID
			 , COALESCE(OH.ORDER_HEADER_KEY,-2) AS ORDER_HEADER_KEY,
			   R.EVENT_CAPTURED_DATE_KEY AS EVENT_DATE_KEY,
			   R.CM_PLATFORM_KEY,
			   R.RIID,
			   CASE WHEN CAST(R.EMAIL_KEY AS INTEGER) <> -2 THEN CAST(R.EMAIL_KEY AS INTEGER)
					WHEN R.EMAIL_ADDR_KEY <> -2 THEN R.EMAIL_ADDR_KEY
					WHEN R.EMAIL_ADDRESS_KEY <> -2 THEN R.EMAIL_ADDRESS_KEY
					WHEN R.CONTACT_INFO_KEY <> -2 THEN R.CONTACT_INFO_KEY 
					ELSE -2 END AS EMAIL_KEY,
			   CASE WHEN R.CM_EMAIL_ADDR_KEY <> -2 THEN R.CM_EMAIL_ADDR_KEY
					WHEN R.CM_EMAIL_ADDRESS_KEY <> -2 THEN R.CM_EMAIL_ADDRESS_KEY
					WHEN R.CM_CONTACT_INFO_KEY <> -2 THEN R.CM_CONTACT_INFO_KEY 
					ELSE -2 END AS CM_EMAIL_ADDRESS_KEY,
			   R.CM_LIST_KEY,
			   R.LIST_ID,
			   R.CM_CAMPAIGN_KEY,
			   R.CAMPAIGN_ID,
			   R.CM_LAUNCH_KEY,
			   R.LAUNCH_ID,
			   '-2' AS SEGMENT_INFO,
			   0 AS FAIL_COUNT, 
			   0 AS SENT_COUNT, 
			   0 AS SKIPPED_COUNT, 
			   1 AS BOUNCE_COUNT, 
			   0 AS OPEN_COUNT,
			   0 AS CLICK_COUNT, 
			   0 AS CONVERT_COUNT, 
			   0 AS COMPLAINT_COUNT, 
			   0 AS OPT_IN_COUNT, 
			   0 AS OPT_OUT_COUNT,
			   0 AS RESPOND_COUNT,
			   $job_run_id AS JOB_RUN_ID,
			   CURRENT_TIMESTAMP AS M_INSERT_DATETIME
		FROM $NZ_RDW_DB..CUSTOMER_MESSAGING_BOUNCE R LEFT JOIN $NZ_PRSNT_DB.$NZ_PRSNT_SCHEMA.ORDER_HEADER OH 
		                                                  ON CASE
                                                              WHEN R.SOURCESYSTEM = 'POS' 
                                                              THEN TRIM(TRIM(R.TRANSACTIONKEY)||'-'||LPAD(1,5,'0')) 
															  ELSE R.TRANSACTIONKEY
															  END = OH.ORDER_ID
			WHERE  R.JOB_RUN_ID IN $PRED_JOB_IDS;
		"
echo ''		
echo $CDC_EMAIL_INTERACTION_SQL5;
echo ''

	if [[ $verbose_logging = 1 ]]; then
		print_msg "$CDC_EMAIL_INTERACTION_SQL5"
	fi

	run_query -d $NZ_STG_DB -q "$CDC_EMAIL_INTERACTION_SQL5" -m "0051 Failed while inserting bounce data into cdc table"
	if [[ $? != 0 ]]
	then 
		perl endJobRun.pl "$job_run_id" "FAILURE"
		print_msg "0052 *****ERROR***** Failed while inserting bounce data into cdc table in $NZ_STG_DB.  $SCRIPT_NAME finished UNSUCCESSFULLY."
		exit $return_code_fail
	fi


print_msg "0060 Populating Email Interaction CDC table with good rows from Open RDW"
	export CDC_EMAIL_INTERACTION_SQL6="
		INSERT INTO CM_EMAIL_INTERACTION_FACT_CDC
		(	ORDER_ID, ORDER_HEADER_KEY, EVENT_DATE_KEY, CM_PLATFORM_KEY, RIID, EMAIL_KEY, CM_EMAIL_ADDRESS_KEY,
			CM_LIST_KEY, LIST_ID, CM_CAMPAIGN_KEY, CAMPAIGN_ID, CM_LAUNCH_KEY, LAUNCH_ID, SEGMENT_INFO,
			FAIL_COUNT, SENT_COUNT, SKIPPED_COUNT, BOUNCE_COUNT, OPEN_COUNT, 
			CLICK_COUNT, CONVERT_COUNT, COMPLAINT_COUNT, OPT_IN_COUNT, OPT_OUT_COUNT, RESPOND_COUNT,
			JOB_RUN_ID, M_INSERT_DATETIME	)
		SELECT CASE 
                      WHEN R.TRANSACTIONKEY = '-2' THEN NULL
                      WHEN R.SOURCESYSTEM = 'POS' THEN TRIM(TRIM(R.TRANSACTIONKEY)||'-'||LPAD(1,5,'0'))
			 ELSE R.TRANSACTIONKEY
			END AS ORDER_ID
			 , COALESCE(OH.ORDER_HEADER_KEY,-2) AS ORDER_HEADER_KEY,
			   R.EVENT_CAPTURED_DATE_KEY AS EVENT_DATE_KEY,
			   R.CM_PLATFORM_KEY,
			   R.RIID,
			   CASE WHEN CAST(R.EMAIL_KEY AS INTEGER) <> -2 THEN CAST(R.EMAIL_KEY AS INTEGER)
					WHEN R.EMAIL_ADDRESS_KEY <> -2 THEN R.EMAIL_ADDRESS_KEY
					ELSE -2 END AS EMAIL_KEY,
			   CAse WHEN R.CM_EMAIL_ADDRESS_KEY <> -2 THEN R.CM_EMAIL_ADDRESS_KEY
					ELSE -2 END AS CM_EMAIL_ADDRESS_KEY,
			   R.CM_LIST_KEY,
			   R.LIST_ID,
			   R.CM_CAMPAIGN_KEY,
			   R.CAMPAIGN_ID,
			   R.CM_LAUNCH_KEY,
			   R.LAUNCH_ID,
			   '-2' AS SEGMENT_INFO,
			   0 AS FAIL_COUNT, 
			   0 AS SENT_COUNT, 
			   0 AS SKIPPED_COUNT, 
			   0 AS BOUNCE_COUNT, 
			   1 AS OPEN_COUNT,
			   0 AS CLICK_COUNT, 
			   0 AS CONVERT_COUNT, 
			   0 AS COMPLAINT_COUNT, 
			   0 AS OPT_IN_COUNT, 
			   0 AS OPT_OUT_COUNT,
			   0 AS RESPOND_COUNT,
			   $job_run_id AS JOB_RUN_ID,
			   CURRENT_TIMESTAMP AS M_INSERT_DATETIME 
		FROM $NZ_RDW_DB..CUSTOMER_MESSAGING_OPEN R LEFT JOIN $NZ_PRSNT_DB.$NZ_PRSNT_SCHEMA.ORDER_HEADER OH 
		                                                  ON CASE
                                                              WHEN R.SOURCESYSTEM = 'POS' 
                                                              THEN TRIM(TRIM(R.TRANSACTIONKEY)||'-'||LPAD(1,5,'0')) 
															  ELSE R.TRANSACTIONKEY
															  END = OH.ORDER_ID
			WHERE  R.JOB_RUN_ID IN $PRED_JOB_IDS;
		"

echo ''		
echo $CDC_EMAIL_INTERACTION_SQL6;
echo ''

	if [[ $verbose_logging = 1 ]]; then
		print_msg "$CDC_EMAIL_INTERACTION_SQL6"
	fi

	run_query -d $NZ_STG_DB -q "$CDC_EMAIL_INTERACTION_SQL6" -m "0061 Failed while inserting open data into cdc table"
	if [[ $? != 0 ]]
	then 
		perl endJobRun.pl "$job_run_id" "FAILURE"
		print_msg "0062 *****ERROR***** Failed while inserting open data into cdc table in $NZ_STG_DB.  $SCRIPT_NAME finished UNSUCCESSFULLY."
		exit $return_code_fail
	fi

	
print_msg "0070 Populating Email Interaction CDC table with good rows from Click RDW"
	export CDC_EMAIL_INTERACTION_SQL7="
		INSERT INTO CM_EMAIL_INTERACTION_FACT_CDC
		(	ORDER_ID, ORDER_HEADER_KEY, EVENT_DATE_KEY, CM_PLATFORM_KEY, RIID, EMAIL_KEY, CM_EMAIL_ADDRESS_KEY,
			CM_LIST_KEY, LIST_ID, CM_CAMPAIGN_KEY, CAMPAIGN_ID, CM_LAUNCH_KEY, LAUNCH_ID, SEGMENT_INFO,
			FAIL_COUNT, SENT_COUNT, SKIPPED_COUNT, BOUNCE_COUNT, OPEN_COUNT, 
			CLICK_COUNT, CONVERT_COUNT, COMPLAINT_COUNT, OPT_IN_COUNT, OPT_OUT_COUNT, RESPOND_COUNT,
			JOB_RUN_ID, M_INSERT_DATETIME	)
		SELECT CASE 
                      WHEN R.TRANSACTIONKEY = '-2' THEN NULL
                      WHEN R.SOURCESYSTEM = 'POS' THEN TRIM(TRIM(R.TRANSACTIONKEY)||'-'||LPAD(1,5,'0'))
			 ELSE R.TRANSACTIONKEY
			END AS ORDER_ID
			 , COALESCE(OH.ORDER_HEADER_KEY,-2) AS ORDER_HEADER_KEY,
			   R.EVENT_CAPTURED_DATE_KEY AS EVENT_DATE_KEY,
			   R.CM_PLATFORM_KEY,
			   R.RIID,
			   CASE WHEN CAST(R.EMAIL_KEY AS INTEGER) <> -2 THEN CAST(R.EMAIL_KEY AS INTEGER)
					WHEN R.EMAIL_ADDRESS_KEY <> -2 THEN R.EMAIL_ADDRESS_KEY
					ELSE -2 END AS EMAIL_KEY,
			   CAse WHEN R.CM_EMAIL_ADDRESS_KEY <> -2 THEN R.CM_EMAIL_ADDRESS_KEY
					ELSE -2 END AS CM_EMAIL_ADDRESS_KEY,
			   R.CM_LIST_KEY,
			   R.LIST_ID,
			   R.CM_CAMPAIGN_KEY,
			   R.CAMPAIGN_ID,
			   R.CM_LAUNCH_KEY,
			   R.LAUNCH_ID,
			   '-2' AS SEGMENT_INFO,
			   0 AS FAIL_COUNT, 
			   0 AS SENT_COUNT, 
			   0 AS SKIPPED_COUNT, 
			   0 AS BOUNCE_COUNT, 
			   0 AS OPEN_COUNT,
			   1 AS CLICK_COUNT, 
			   0 AS CONVERT_COUNT, 
			   0 AS COMPLAINT_COUNT, 
			   0 AS OPT_IN_COUNT, 
			   0 AS OPT_OUT_COUNT,
			   0 AS RESPOND_COUNT,
			   $job_run_id AS JOB_RUN_ID,
			   CURRENT_TIMESTAMP AS M_INSERT_DATETIME
		FROM $NZ_RDW_DB..CUSTOMER_MESSAGING_CLICK R LEFT JOIN $NZ_PRSNT_DB.$NZ_PRSNT_SCHEMA.ORDER_HEADER OH 
		                                                  ON CASE
                                                              WHEN R.SOURCESYSTEM = 'POS' 
                                                              THEN TRIM(TRIM(R.TRANSACTIONKEY)||'-'||LPAD(1,5,'0')) 
															  ELSE R.TRANSACTIONKEY
															  END = OH.ORDER_ID
			WHERE  R.JOB_RUN_ID IN $PRED_JOB_IDS;
		"
echo ''		
echo $CDC_EMAIL_INTERACTION_SQL7;
echo ''

	if [[ $verbose_logging = 1 ]]; then
		print_msg "$CDC_EMAIL_INTERACTION_SQL7"
	fi

	run_query -d $NZ_STG_DB -q "$CDC_EMAIL_INTERACTION_SQL7" -m "0071 Failed while inserting click data into cdc table"
	if [[ $? != 0 ]]
	then 
		perl endJobRun.pl "$job_run_id" "FAILURE"
		print_msg "0072 *****ERROR***** Failed while inserting click data into cdc table in $NZ_STG_DB.  $SCRIPT_NAME finished UNSUCCESSFULLY."
		exit $return_code_fail
	fi

	
print_msg "0080 Populating Email Interaction CDC table with good rows from Convert RDW"
	export CDC_EMAIL_INTERACTION_SQL8="
		INSERT INTO CM_EMAIL_INTERACTION_FACT_CDC
		(	ORDER_ID, ORDER_HEADER_KEY, EVENT_DATE_KEY, CM_PLATFORM_KEY, RIID, EMAIL_KEY, CM_EMAIL_ADDRESS_KEY,
			CM_LIST_KEY, LIST_ID, CM_CAMPAIGN_KEY, CAMPAIGN_ID, CM_LAUNCH_KEY, LAUNCH_ID, SEGMENT_INFO,
			FAIL_COUNT, SENT_COUNT, SKIPPED_COUNT, BOUNCE_COUNT, OPEN_COUNT, 
			CLICK_COUNT, CONVERT_COUNT, COMPLAINT_COUNT, OPT_IN_COUNT, OPT_OUT_COUNT, RESPOND_COUNT,
			JOB_RUN_ID, M_INSERT_DATETIME	)
		SELECT CASE 
                      WHEN R.TRANSACTIONKEY = '-2' THEN NULL
                      WHEN R.SOURCESYSTEM = 'POS' THEN TRIM(TRIM(R.TRANSACTIONKEY)||'-'||LPAD(1,5,'0'))
			 ELSE R.TRANSACTIONKEY
			END AS ORDER_ID
			 , COALESCE(OH.ORDER_HEADER_KEY,-2) AS ORDER_HEADER_KEY,
			   R.EVENT_CAPTURED_DATE_KEY AS EVENT_DATE_KEY,
			   R.CM_PLATFORM_KEY,
			   R.RIID,
			   CASE WHEN CAST(R.EMAIL_KEY AS INTEGER) <> -2 THEN CAST(R.EMAIL_KEY AS INTEGER)
					WHEN R.EMAIL_ADDRESS_KEY <> -2 THEN R.EMAIL_ADDRESS_KEY
					ELSE -2 END AS EMAIL_KEY,
			   CAse WHEN R.CM_EMAIL_ADDRESS_KEY <> -2 THEN R.CM_EMAIL_ADDRESS_KEY
					ELSE -2 END AS CM_EMAIL_ADDRESS_KEY,
			   R.CM_LIST_KEY,
			   R.LIST_ID,
			   R.CM_CAMPAIGN_KEY,
			   R.CAMPAIGN_ID,
			   R.CM_LAUNCH_KEY,
			   R.LAUNCH_ID,
			   '-2' AS SEGMENT_INFO,
			   0 AS FAIL_COUNT, 
			   0 AS SENT_COUNT, 
			   0 AS SKIPPED_COUNT, 
			   0 AS BOUNCE_COUNT, 
			   0 AS OPEN_COUNT,
			   0 AS CLICK_COUNT, 
			   1 AS CONVERT_COUNT, 
			   0 AS COMPLAINT_COUNT, 
			   0 AS OPT_IN_COUNT, 
			   0 AS OPT_OUT_COUNT,
			   0 AS RESPOND_COUNT,
			   $job_run_id AS JOB_RUN_ID,
			   CURRENT_TIMESTAMP AS M_INSERT_DATETIME
		FROM $NZ_RDW_DB..CUSTOMER_MESSAGING_CONVERT R LEFT JOIN $NZ_PRSNT_DB.$NZ_PRSNT_SCHEMA.ORDER_HEADER OH 
		                                                  ON CASE
                                                              WHEN R.SOURCESYSTEM = 'POS' 
                                                              THEN TRIM(TRIM(R.TRANSACTIONKEY)||'-'||LPAD(1,5,'0')) 
															  ELSE R.TRANSACTIONKEY
															  END = OH.ORDER_ID
			WHERE  R.JOB_RUN_ID IN $PRED_JOB_IDS;
		"
echo ''		
echo $CDC_EMAIL_INTERACTION_SQL8;
echo ''

	if [[ $verbose_logging = 1 ]]; then
		print_msg "$CDC_EMAIL_INTERACTION_SQL8"
	fi

	run_query -d $NZ_STG_DB -q "$CDC_EMAIL_INTERACTION_SQL8" -m "0081 Failed while inserting convert data into cdc table"
	if [[ $? != 0 ]]
	then 
		perl endJobRun.pl "$job_run_id" "FAILURE"
		print_msg "0082 *****ERROR***** Failed while inserting convert data into cdc table in $NZ_STG_DB.  $SCRIPT_NAME finished UNSUCCESSFULLY."
		exit $return_code_fail
	fi

		
print_msg "0090 Populating Email Interaction CDC table with good rows from Complaint RDW"
	export CDC_EMAIL_INTERACTION_SQL9="
		INSERT INTO CM_EMAIL_INTERACTION_FACT_CDC
		(	ORDER_ID, ORDER_HEADER_KEY, EVENT_DATE_KEY, CM_PLATFORM_KEY, RIID, EMAIL_KEY, CM_EMAIL_ADDRESS_KEY,
			CM_LIST_KEY, LIST_ID, CM_CAMPAIGN_KEY, CAMPAIGN_ID, CM_LAUNCH_KEY, LAUNCH_ID, SEGMENT_INFO,
			FAIL_COUNT, SENT_COUNT, SKIPPED_COUNT, BOUNCE_COUNT, OPEN_COUNT, 
			CLICK_COUNT, CONVERT_COUNT, COMPLAINT_COUNT, OPT_IN_COUNT, OPT_OUT_COUNT, RESPOND_COUNT,
			JOB_RUN_ID, M_INSERT_DATETIME	)
		SELECT CASE 
                      WHEN R.TRANSACTIONKEY = '-2' THEN NULL
                      WHEN R.SOURCESYSTEM = 'POS' THEN TRIM(TRIM(R.TRANSACTIONKEY)||'-'||LPAD(1,5,'0'))
			 ELSE R.TRANSACTIONKEY
			END AS ORDER_ID
			 , COALESCE(OH.ORDER_HEADER_KEY,-2) AS ORDER_HEADER_KEY,
			   R.EVENT_CAPTURED_DATE_KEY AS EVENT_DATE_KEY,
			   R.CM_PLATFORM_KEY,
			   R.RIID,
			  CASE WHEN CAST(R.EMAIL_KEY AS INTEGER) <> -2 THEN CAST(R.EMAIL_KEY AS INTEGER)
					WHEN R.EMAIL_ADDR_KEY <> -2 THEN R.EMAIL_ADDR_KEY
					WHEN R.EMAIL_ADDRESS_KEY <> -2 THEN R.EMAIL_ADDRESS_KEY
					WHEN R.CONTACT_INFO_KEY <> -2 THEN R.CONTACT_INFO_KEY 
					ELSE -2 END AS EMAIL_KEY,
			   CASE WHEN R.CM_EMAIL_ADDR_KEY <> -2 THEN R.CM_EMAIL_ADDR_KEY
					WHEN R.CM_EMAIL_ADDRESS_KEY <> -2 THEN R.CM_EMAIL_ADDRESS_KEY
					WHEN R.CM_CONTACT_INFO_KEY <> -2 THEN R.CM_CONTACT_INFO_KEY 
					ELSE -2 END AS CM_EMAIL_ADDRESS_KEY,
			   R.CM_LIST_KEY,
			   R.LIST_ID,
			   R.CM_CAMPAIGN_KEY,
			   R.CAMPAIGN_ID,
			   R.CM_LAUNCH_KEY,
			   R.LAUNCH_ID,
			   '-2' AS SEGMENT_INFO,
			   0 AS FAIL_COUNT, 
			   0 AS SENT_COUNT, 
			   0 AS SKIPPED_COUNT, 
			   0 AS BOUNCE_COUNT, 
			   0 AS OPEN_COUNT,
			   0 AS CLICK_COUNT, 
			   0 AS CONVERT_COUNT, 
			   1 AS COMPLAINT_COUNT, 
			   0 AS OPT_IN_COUNT, 
			   0 AS OPT_OUT_COUNT,
			   0 AS RESPOND_COUNT,
			   $job_run_id AS JOB_RUN_ID,
			   CURRENT_TIMESTAMP AS M_INSERT_DATETIME
		FROM $NZ_RDW_DB..CUSTOMER_MESSAGING_COMPLAINT R LEFT JOIN $NZ_PRSNT_DB.$NZ_PRSNT_SCHEMA.ORDER_HEADER OH 
		                                                  ON CASE
                                                              WHEN R.SOURCESYSTEM = 'POS' 
                                                              THEN TRIM(TRIM(R.TRANSACTIONKEY)||'-'||LPAD(1,5,'0')) 
															  ELSE R.TRANSACTIONKEY
															  END = OH.ORDER_ID
			WHERE  R.JOB_RUN_ID IN $PRED_JOB_IDS;
		"

echo ''		
echo $CDC_EMAIL_INTERACTION_SQL9;
echo ''

	if [[ $verbose_logging = 1 ]]; then
		print_msg "$CDC_EMAIL_INTERACTION_SQL9"
	fi

	run_query -d $NZ_STG_DB -q "$CDC_EMAIL_INTERACTION_SQL9" -m "0091 Failed while inserting complaint data into cdc table"
	if [[ $? != 0 ]]
	then 
		perl endJobRun.pl "$job_run_id" "FAILURE"
		print_msg "0092 *****ERROR***** Failed while inserting complaint data into cdc table in $NZ_STG_DB.  $SCRIPT_NAME finished UNSUCCESSFULLY."
		exit $return_code_fail
	fi


print_msg "0100 Populating Email Interaction CDC table with good rows from Opt In RDW"
	export CDC_EMAIL_INTERACTION_SQL10="
		INSERT INTO CM_EMAIL_INTERACTION_FACT_CDC
		(	ORDER_ID, ORDER_HEADER_KEY, EVENT_DATE_KEY, CM_PLATFORM_KEY, RIID, EMAIL_KEY, CM_EMAIL_ADDRESS_KEY,
			CM_LIST_KEY, LIST_ID, CM_CAMPAIGN_KEY, CAMPAIGN_ID, CM_LAUNCH_KEY, LAUNCH_ID, SEGMENT_INFO,
			FAIL_COUNT, SENT_COUNT, SKIPPED_COUNT, BOUNCE_COUNT, OPEN_COUNT, 
			CLICK_COUNT, CONVERT_COUNT, COMPLAINT_COUNT, OPT_IN_COUNT, OPT_OUT_COUNT, RESPOND_COUNT,
			JOB_RUN_ID, M_INSERT_DATETIME	)
		SELECT CASE 
                      WHEN R.TRANSACTIONKEY = '-2' THEN NULL
                      WHEN R.SOURCESYSTEM = 'POS' THEN TRIM(TRIM(R.TRANSACTIONKEY)||'-'||LPAD(1,5,'0'))
			 ELSE R.TRANSACTIONKEY
			END AS ORDER_ID
			 , COALESCE(OH.ORDER_HEADER_KEY,-2) AS ORDER_HEADER_KEY,
			   R.EVENT_CAPTURED_DATE_KEY AS EVENT_DATE_KEY,
			   R.CM_PLATFORM_KEY,
			   R.RIID,
			   CASE WHEN CAST(R.EMAIL_KEY AS INTEGER) <> -2 THEN CAST(R.EMAIL_KEY AS INTEGER)
					WHEN R.EMAIL_ADDR_KEY <> -2 THEN R.EMAIL_ADDR_KEY
					WHEN R.EMAIL_ADDRESS_KEY <> -2 THEN R.EMAIL_ADDRESS_KEY
					ELSE -2 END AS EMAIL_KEY,
			   CASE WHEN R.CM_EMAIL_ADDR_KEY <> -2 THEN R.CM_EMAIL_ADDR_KEY
					WHEN R.CM_EMAIL_ADDRESS_KEY <> -2 THEN R.CM_EMAIL_ADDRESS_KEY
					ELSE -2 END AS CM_EMAIL_ADDRESS_KEY,
			   R.CM_LIST_KEY,
			   R.LIST_ID,
			   R.CM_CAMPAIGN_KEY,
			   R.CAMPAIGN_ID,
			   R.CM_LAUNCH_KEY,
			   R.LAUNCH_ID,
			   '-2' AS SEGMENT_INFO,
			   0 AS FAIL_COUNT, 
			   0 AS SENT_COUNT, 
			   0 AS SKIPPED_COUNT, 
			   0 AS BOUNCE_COUNT, 
			   0 AS OPEN_COUNT,
			   0 AS CLICK_COUNT, 
			   0 AS CONVERT_COUNT, 
			   0 AS COMPLAINT_COUNT, 
			   1 AS OPT_IN_COUNT, 
			   0 AS OPT_OUT_COUNT,
			   0 AS RESPOND_COUNT,
			   $job_run_id AS JOB_RUN_ID,
			   CURRENT_TIMESTAMP AS M_INSERT_DATETIME
		FROM $NZ_RDW_DB..CUSTOMER_MESSAGING_OPT_IN R LEFT JOIN $NZ_PRSNT_DB.$NZ_PRSNT_SCHEMA.ORDER_HEADER OH 
		                                                  ON CASE
                                                              WHEN R.SOURCESYSTEM = 'POS' 
                                                              THEN TRIM(TRIM(R.TRANSACTIONKEY)||'-'||LPAD(1,5,'0')) 
															  ELSE R.TRANSACTIONKEY
															  END = OH.ORDER_ID
			WHERE  R.JOB_RUN_ID IN $PRED_JOB_IDS;
		"
echo ''		
echo $CDC_EMAIL_INTERACTION_SQL10;
echo ''

	if [[ $verbose_logging = 1 ]]; then
		print_msg "$CDC_EMAIL_INTERACTION_SQL10"
	fi

	run_query -d $NZ_STG_DB -q "$CDC_EMAIL_INTERACTION_SQL10" -m "0101 Failed while inserting opt in data into cdc table"
	if [[ $? != 0 ]]
	then 
		perl endJobRun.pl "$job_run_id" "FAILURE"
		print_msg "0102 *****ERROR***** Failed while inserting opt in data into cdc table in $NZ_STG_DB.  $SCRIPT_NAME finished UNSUCCESSFULLY."
		exit $return_code_fail
	fi


print_msg "0110 Populating Email Interaction CDC table with good rows from Opt Out RDW"
	export CDC_EMAIL_INTERACTION_SQL11="
		INSERT INTO CM_EMAIL_INTERACTION_FACT_CDC
		(	ORDER_ID, ORDER_HEADER_KEY, EVENT_DATE_KEY, CM_PLATFORM_KEY, RIID, EMAIL_KEY, CM_EMAIL_ADDRESS_KEY,
			CM_LIST_KEY, LIST_ID, CM_CAMPAIGN_KEY, CAMPAIGN_ID, CM_LAUNCH_KEY, LAUNCH_ID, SEGMENT_INFO,
			FAIL_COUNT, SENT_COUNT, SKIPPED_COUNT, BOUNCE_COUNT, OPEN_COUNT, 
			CLICK_COUNT, CONVERT_COUNT, COMPLAINT_COUNT, OPT_IN_COUNT, OPT_OUT_COUNT, RESPOND_COUNT,
			JOB_RUN_ID, M_INSERT_DATETIME	)
		SELECT CASE 
                      WHEN R.TRANSACTIONKEY = '-2' THEN NULL
                      WHEN R.SOURCESYSTEM = 'POS' THEN TRIM(TRIM(R.TRANSACTIONKEY)||'-'||LPAD(1,5,'0'))
			 ELSE R.TRANSACTIONKEY
			END AS ORDER_ID
			 , COALESCE(OH.ORDER_HEADER_KEY,-2) AS ORDER_HEADER_KEY,
			   R.EVENT_CAPTURED_DATE_KEY AS EVENT_DATE_KEY,
			   R.CM_PLATFORM_KEY,
			   R.RIID,
			   CASE WHEN CAST(R.EMAIL_KEY AS INTEGER) <> -2 THEN CAST(R.EMAIL_KEY AS INTEGER)
					WHEN R.EMAIL_ADDR_KEY <> -2 THEN R.EMAIL_ADDR_KEY
					WHEN R.EMAIL_ADDRESS_KEY <> -2 THEN R.EMAIL_ADDRESS_KEY
					WHEN R.CONTACT_INFO_KEY <> -2 THEN R.CONTACT_INFO_KEY 
					ELSE -2 END AS EMAIL_KEY,
			   CASE WHEN R.CM_EMAIL_ADDR_KEY <> -2 THEN R.CM_EMAIL_ADDR_KEY
					WHEN R.CM_EMAIL_ADDRESS_KEY <> -2 THEN R.CM_EMAIL_ADDRESS_KEY
					WHEN R.CM_CONTACT_INFO_KEY <> -2 THEN R.CM_CONTACT_INFO_KEY 
					ELSE -2 END AS CM_EMAIL_ADDRESS_KEY,
			   R.CM_LIST_KEY,
			   R.LIST_ID,
			   R.CM_CAMPAIGN_KEY,
			   R.CAMPAIGN_ID,
			   R.CM_LAUNCH_KEY,
			   R.LAUNCH_ID,
			   '-2' AS SEGMENT_INFO,
			   0 AS FAIL_COUNT, 
			   0 AS SENT_COUNT, 
			   0 AS SKIPPED_COUNT, 
			   0 AS BOUNCE_COUNT, 
			   0 AS OPEN_COUNT,
			   0 AS CLICK_COUNT, 
			   0 AS CONVERT_COUNT, 
			   0 AS COMPLAINT_COUNT, 
			   0 AS OPT_IN_COUNT, 
			   1 AS OPT_OUT_COUNT,
			   0 AS RESPOND_COUNT,
			   $job_run_id AS JOB_RUN_ID,
			   CURRENT_TIMESTAMP AS M_INSERT_DATETIME 
		FROM $NZ_RDW_DB..CUSTOMER_MESSAGING_OPT_OUT R LEFT JOIN $NZ_PRSNT_DB.$NZ_PRSNT_SCHEMA.ORDER_HEADER OH 
		                                                  ON CASE
                                                              WHEN R.SOURCESYSTEM = 'POS' 
                                                              THEN TRIM(TRIM(R.TRANSACTIONKEY)||'-'||LPAD(1,5,'0')) 
															  ELSE R.TRANSACTIONKEY
															  END = OH.ORDER_ID
			WHERE  R.JOB_RUN_ID IN $PRED_JOB_IDS;
		"
echo ''		
echo $CDC_EMAIL_INTERACTION_SQL11;
echo ''

	if [[ $verbose_logging = 1 ]]; then
		print_msg "$CDC_EMAIL_INTERACTION_SQL11"
	fi

	run_query -d $NZ_STG_DB -q "$CDC_EMAIL_INTERACTION_SQL11" -m "0111 Failed while inserting opt out data into cdc table"
	if [[ $? != 0 ]]
	then 
		perl endJobRun.pl "$job_run_id" "FAILURE"
		print_msg "0112 *****ERROR***** Failed while inserting opt out data into cdc table in $NZ_STG_DB.  $SCRIPT_NAME finished UNSUCCESSFULLY."
		exit $return_code_fail
	fi


	
print_msg "0120 Populating Email Interaction CDC table with good rows from Bounce Log RDW"
	export CDC_EMAIL_INTERACTION_SQL12="
		INSERT INTO CM_EMAIL_INTERACTION_FACT_CDC
		(	ORDER_ID, ORDER_HEADER_KEY, EVENT_DATE_KEY, CM_PLATFORM_KEY, RIID, EMAIL_KEY, CM_EMAIL_ADDRESS_KEY,
			CM_LIST_KEY, LIST_ID, CM_CAMPAIGN_KEY, CAMPAIGN_ID, CM_LAUNCH_KEY, LAUNCH_ID, SEGMENT_INFO,
			FAIL_COUNT, SENT_COUNT, SKIPPED_COUNT, BOUNCE_COUNT, OPEN_COUNT, 
			CLICK_COUNT, CONVERT_COUNT, COMPLAINT_COUNT, OPT_IN_COUNT, OPT_OUT_COUNT, RESPOND_COUNT,
			JOB_RUN_ID, M_INSERT_DATETIME	)
		SELECT NULL AS ORDER_ID , 
			  -2 AS ORDER_HEADER_KEY ,
			   R.EVENT_CAPTURED_DATE_KEY AS EVENT_DATE_KEY,
			   R.CM_PLATFORM_KEY,
			   -2 AS RIID,
			   R.EMAIL_ADDR_KEY AS EMAIL_KEY,
			   R.CM_EMAIL_ADDR_KEY AS CM_EMAIL_ADDRESS_KEY,
			   -2 AS CM_LIST_KEY,
			   -2 AS LIST_ID,
			   R.CM_CAMPAIGN_KEY,
			   R.CAMPAIGN_NAME AS CAMPAIGN_ID,
			   R.CM_LAUNCH_KEY,
			   R.LAUNCH_ID,
			   '-2' AS SEGMENT_INFO,
			   0 AS FAIL_COUNT, 
			   0 AS SENT_COUNT, 
			   0 AS SKIPPED_COUNT, 
			   1 AS BOUNCE_COUNT, 
			   0 AS OPEN_COUNT,
			   0 AS CLICK_COUNT, 
			   0 AS CONVERT_COUNT, 
			   0 AS COMPLAINT_COUNT, 
			   0 AS OPT_IN_COUNT, 
			   0 AS OPT_OUT_COUNT,
			   0 AS RESPOND_COUNT,
			   $job_run_id AS JOB_RUN_ID,
			   CURRENT_TIMESTAMP AS M_INSERT_DATETIME 
		FROM $NZ_RDW_DB..CUSTOMER_MESSAGING_BOUNCELOG R
			WHERE  R.JOB_RUN_ID IN $PRED_JOB_IDS;
		"

echo ''		
echo $CDC_EMAIL_INTERACTION_SQL12;
echo ''

	if [[ $verbose_logging = 1 ]]; then
		print_msg "$CDC_EMAIL_INTERACTION_SQL12"
	fi

	run_query -d $NZ_STG_DB -q "$CDC_EMAIL_INTERACTION_SQL12" -m "0121 Failed while inserting bounce log data into cdc table"
	if [[ $? != 0 ]]
	then 
		perl endJobRun.pl "$job_run_id" "FAILURE"
		print_msg "0122 *****ERROR***** Failed while inserting bounce log data into cdc table in $NZ_STG_DB.  $SCRIPT_NAME finished UNSUCCESSFULLY."
		exit $return_code_fail
	fi


print_msg "0130 Populating Email Interaction CDC table with good rows from Real Time Msg Log (_Sent_) RDW"
	export CDC_EMAIL_INTERACTION_SQL13="
		INSERT INTO CM_EMAIL_INTERACTION_FACT_CDC
		(	ORDER_ID, ORDER_HEADER_KEY, EVENT_DATE_KEY, CM_PLATFORM_KEY, RIID, EMAIL_KEY, CM_EMAIL_ADDRESS_KEY,
			CM_LIST_KEY, LIST_ID, CM_CAMPAIGN_KEY, CAMPAIGN_ID, CM_LAUNCH_KEY, LAUNCH_ID, SEGMENT_INFO,
			FAIL_COUNT, SENT_COUNT, SKIPPED_COUNT, BOUNCE_COUNT, OPEN_COUNT, 
			CLICK_COUNT, CONVERT_COUNT, COMPLAINT_COUNT, OPT_IN_COUNT, OPT_OUT_COUNT, RESPOND_COUNT,
			JOB_RUN_ID, M_INSERT_DATETIME	)
		SELECT NULL AS ORDER_ID , 
			  -2 AS ORDER_HEADER_KEY ,
			   R.EVENT_CAPTURED_DATE_KEY AS EVENT_DATE_KEY,
			   R.CM_PLATFORM_KEY,
			   -2 AS RIID,
			   R.EMAIL_ADDR_KEY AS EMAIL_KEY,
			   R.CM_EMAIL_ADDR_KEY AS CM_EMAIL_ADDRESS_KEY,
			   -2 AS CM_LIST_KEY,
			   -2 AS LIST_ID,
			   R.CM_CAMPAIGN_KEY,
			   R.CAMPAIGN_NAME AS CAMPAIGN_ID,
			   R.CM_LAUNCH_KEY,
			   R.LAUNCH_ID,
			   '-2' AS SEGMENT_INFO,
			   0 AS FAIL_COUNT, 
			   CASE WHEN ACTION_EVENT = '_Sent_' THEN 1 ELSE 0 END AS SENT_COUNT, 
			   CASE WHEN ACTION_EVENT = '_Skipped_' THEN 1 ELSE 0 END AS SKIPPED_COUNT, 
			   0 AS BOUNCE_COUNT, 
			   CASE WHEN ACTION_EVENT = '_Opened_' THEN 1 ELSE 0 END AS OPEN_COUNT,
			   CASE WHEN ACTION_EVENT = '_Clicked_' THEN 1 ELSE 0 END AS CLICK_COUNT, 
			   CASE WHEN ACTION_EVENT = '_Conversion_' THEN 1 ELSE 0 END AS CONVERT_COUNT, 
			   0 AS COMPLAINT_COUNT, 
			   0 AS OPT_IN_COUNT, 
			   0 AS OPT_OUT_COUNT,
			   CASE WHEN ACTION_EVENT = '_Responded_' THEN 1 ELSE 0 END AS RESPOND_COUNT,
			   $job_run_id AS JOB_RUN_ID,
			   CURRENT_TIMESTAMP AS M_INSERT_DATETIME  
		FROM $NZ_RDW_DB..CUSTOMER_MESSAGING_REALTIMEMSGLOG R 
			WHERE  R.JOB_RUN_ID IN $PRED_JOB_IDS;
		"
echo ''		
echo $CDC_EMAIL_INTERACTION_SQL13;
echo ''

	if [[ $verbose_logging = 1 ]]; then
		print_msg "$CDC_EMAIL_INTERACTION_SQL13"
	fi

	run_query -d $NZ_STG_DB -q "$CDC_EMAIL_INTERACTION_SQL13" -m "0131 Failed while inserting real time msg log (sent) data into cdc table"
	if [[ $? != 0 ]]
	then 
		perl endJobRun.pl "$job_run_id" "FAILURE"
		print_msg "0132 *****ERROR***** Failed while inserting real time msg log (sent) data into cdc table in $NZ_STG_DB.  $SCRIPT_NAME finished UNSUCCESSFULLY."
		exit $return_code_fail
	fi


	
print_msg "0140 Populating Email Interaction CDC table with good rows from Spam Complaint Log RDW"
	export CDC_EMAIL_INTERACTION_SQL14="
		INSERT INTO CM_EMAIL_INTERACTION_FACT_CDC
		(	ORDER_ID, ORDER_HEADER_KEY, EVENT_DATE_KEY, CM_PLATFORM_KEY, RIID, EMAIL_KEY, CM_EMAIL_ADDRESS_KEY,
			CM_LIST_KEY, LIST_ID, CM_CAMPAIGN_KEY, CAMPAIGN_ID, CM_LAUNCH_KEY, LAUNCH_ID, SEGMENT_INFO,
			FAIL_COUNT, SENT_COUNT, SKIPPED_COUNT, BOUNCE_COUNT, OPEN_COUNT, 
			CLICK_COUNT, CONVERT_COUNT, COMPLAINT_COUNT, OPT_IN_COUNT, OPT_OUT_COUNT, RESPOND_COUNT,
			JOB_RUN_ID, M_INSERT_DATETIME	)
		SELECT NULL AS ORDER_ID , 
			  -2 AS ORDER_HEADER_KEY ,
			   R.EVENT_CAPTURED_DATE_KEY AS EVENT_DATE_KEY,
			   R.CM_PLATFORM_KEY,
			   -2 AS RIID,
			   R.EMAIL_ADDR_KEY AS EMAIL_KEY,
			   R.CM_EMAIL_ADDR_KEY AS CM_EMAIL_ADDRESS_KEY,
			   -2 AS CM_LIST_KEY,
			   -2 AS LIST_ID,
			   R.CM_CAMPAIGN_KEY,
			   R.CAMPAIGN_NAME AS CAMPAIGN_ID,
			   R.CM_LAUNCH_KEY,
			   R.LAUNCH_ID,
			   '-2' AS SEGMENT_INFO,
			   0 AS FAIL_COUNT, 
			   0 AS SENT_COUNT, 
			   0 AS SKIPPED_COUNT, 
			   0 AS BOUNCE_COUNT, 
			   0 AS OPEN_COUNT,
			   0 AS CLICK_COUNT, 
			   0 AS CONVERT_COUNT, 
			   1 AS COMPLAINT_COUNT, 
			   0 AS OPT_IN_COUNT, 
			   0 AS OPT_OUT_COUNT,
			   0 AS RESPOND_COUNT,
			   $job_run_id AS JOB_RUN_ID,
			   CURRENT_TIMESTAMP AS M_INSERT_DATETIME  
		FROM $NZ_RDW_DB..CUSTOMER_MESSAGING_SPAMCOMPLAINTLOG R 
			WHERE  R.JOB_RUN_ID IN $PRED_JOB_IDS;
		"

echo ''		
echo $CDC_EMAIL_INTERACTION_SQL14;
echo ''

	if [[ $verbose_logging = 1 ]]; then
		print_msg "$CDC_EMAIL_INTERACTION_SQL14"
	fi

	run_query -d $NZ_STG_DB -q "$CDC_EMAIL_INTERACTION_SQL14" -m "0141 Failed while inserting spam complaint log data into cdc table"
	if [[ $? != 0 ]]
	then 
		perl endJobRun.pl "$job_run_id" "FAILURE"
		print_msg "0142 *****ERROR***** Failed while inserting spam complaint data into cdc table in $NZ_STG_DB.  $SCRIPT_NAME finished UNSUCCESSFULLY."
		exit $return_code_fail
	fi


print_msg "0150 Populating Email Interaction CDC table with good rows from Unsubscribed Log RDW"
	export CDC_EMAIL_INTERACTION_SQL15="
		INSERT INTO CM_EMAIL_INTERACTION_FACT_CDC
		(	ORDER_ID, ORDER_HEADER_KEY, EVENT_DATE_KEY, CM_PLATFORM_KEY, RIID, EMAIL_KEY, CM_EMAIL_ADDRESS_KEY,
			CM_LIST_KEY, LIST_ID, CM_CAMPAIGN_KEY, CAMPAIGN_ID, CM_LAUNCH_KEY, LAUNCH_ID, SEGMENT_INFO,
			FAIL_COUNT, SENT_COUNT, SKIPPED_COUNT, BOUNCE_COUNT, OPEN_COUNT, 
			CLICK_COUNT, CONVERT_COUNT, COMPLAINT_COUNT, OPT_IN_COUNT, OPT_OUT_COUNT, RESPOND_COUNT,
			JOB_RUN_ID, M_INSERT_DATETIME	)
		SELECT NULL AS ORDER_ID , 
			  -2 AS ORDER_HEADER_KEY ,
			   R.EVENT_CAPTURED_DATE_KEY AS EVENT_DATE_KEY,
			   R.CM_PLATFORM_KEY,
			   -2 AS RIID,
			   R.EMAIL_ADDR_KEY AS EMAIL_KEY,
			   R.CM_EMAIL_ADDR_KEY AS CM_EMAIL_ADDRESS_KEY,
			   -2 AS CM_LIST_KEY,
			   -2 AS LIST_ID,
			   R.CM_CAMPAIGN_KEY,
			   R.CAMPAIGN_NAME AS CAMPAIGN_ID,
			   R.CM_LAUNCH_KEY,
			   R.LAUNCH_ID,
			   '-2' AS SEGMENT_INFO,
			   0 AS FAIL_COUNT, 
			   0 AS SENT_COUNT, 
			   0 AS SKIPPED_COUNT, 
			   0 AS BOUNCE_COUNT, 
			   0 AS OPEN_COUNT,
			   0 AS CLICK_COUNT, 
			   0 AS CONVERT_COUNT, 
			   0 AS COMPLAINT_COUNT, 
			   0 AS OPT_IN_COUNT, 
			   1 AS OPT_OUT_COUNT,
			   0 AS RESPOND_COUNT,
			   $job_run_id AS JOB_RUN_ID,
			   CURRENT_TIMESTAMP AS M_INSERT_DATETIME  
		FROM $NZ_RDW_DB..CUSTOMER_MESSAGING_UNSUBSCRIBEDLOG R 
			WHERE  R.JOB_RUN_ID IN $PRED_JOB_IDS;
		"

echo ''		
echo $CDC_EMAIL_INTERACTION_SQL15;
echo ''

	if [[ $verbose_logging = 1 ]]; then
		print_msg "$CDC_EMAIL_INTERACTION_SQL15"
	fi

	run_query -d $NZ_STG_DB -q "$CDC_EMAIL_INTERACTION_SQL15" -m "0151 Failed while inserting unsubscribed log data into cdc table"
	if [[ $? != 0 ]]
	then 
		perl endJobRun.pl "$job_run_id" "FAILURE"
		print_msg "0152 *****ERROR***** Failed while inserting unsubscribed data into cdc table in $NZ_STG_DB.  $SCRIPT_NAME finished UNSUCCESSFULLY."
		exit $return_code_fail
	fi

	
	
	
	

#print_msg "0220 Updating Email Interaction Transform table - SEGMENT_INFO field"
#	export TRANSFORM_EMAIL_INTERACTION_UPDATE_SQL2="
#		UPDATE CM_EMAIL_INTERACTION_FACT_TRANSFORM T
#		SET T.SEGMENT_INFO = NVL(S.SEGMENT_INFO, '-2')
#		FROM CM_EMAIL_SEGMENTATION_INFO S
#		WHERE T.RIID = S.RIID
#			AND T.LIST_ID = S.LIST_ID
#			AND T.CAMPAIGN_ID = S.CAMPAIGN_ID
#			AND T.LAUNCH_ID = S.LAUNCH_ID
#			AND T.CM_PLATFORM_KEY = $cm_platform_key_contact
#			AND T.SEGMENT_INFO = '-2'
#			AND T.JOB_RUN_ID = $job_run_id;
#		"
#	if [[ $verbose_logging = 1 ]]; then
#		print_msg "$TRANSFORM_EMAIL_INTERACTION_UPDATE_SQL2"
#	fi
#
#	run_query -d $NZ_STG_DB -q "$TRANSFORM_EMAIL_INTERACTION_UPDATE_SQL2" -m "0221 Failed while updating email interaction data Segment_Info field in transform table"
#	if [[ $? != 0 ]]
#	then 
#		perl endJobRun.pl "$job_run_id" "FAILURE"
#		print_msg "0222 *****ERROR***** Failed while updating email interaction data Segment_Info field in transform table in $NZ_STG_DB.  $SCRIPT_NAME finished UNSUCCESSFULLY."
#		exit $return_code_fail
#	fi
	
	
	
print_msg "0160 Populating CDM Email Interaction table"
	export CDM_EMAIL_INTERACTION_SQL="
		INSERT INTO CM_EMAIL_INTERACTION_FACT
		(	EMAIL_INTERACTION_FACT_KEY, ORDER_ID, ORDER_HEADER_KEY,
                     EVENT_DATE_KEY, CM_PLATFORM_KEY, RIID, EMAIL_KEY, CM_EMAIL_ADDRESS_KEY,
			CM_LIST_KEY, LIST_ID, CM_CAMPAIGN_KEY, CAMPAIGN_ID, CM_LAUNCH_KEY, LAUNCH_ID, SEGMENT_INFO,
			FAIL_COUNT, SENT_COUNT, SKIPPED_COUNT, BOUNCE_COUNT, OPEN_COUNT, 
			CLICK_COUNT, CONVERT_COUNT, COMPLAINT_COUNT, OPT_IN_COUNT, OPT_OUT_COUNT, RESPOND_COUNT,
			EFF_START_DATETIME, JOB_RUN_ID, M_INSERT_DATETIME, M_LAST_UPDATE_DATETIME	)
		
                 SELECT CAST((SELECT COALESCE(MAX(EMAIL_INTERACTION_FACT_KEY),0) FROM $NZ_PRSNT_DB..CM_EMAIL_INTERACTION_FACT) + 
                      ROW_NUMBER() OVER (ORDER BY  ORDER_ID, ORDER_HEADER_KEY, 
			   EVENT_DATE_KEY, CM_PLATFORM_KEY, RIID, EMAIL_KEY, CM_EMAIL_ADDRESS_KEY,
			   CM_LIST_KEY, LIST_ID, CM_CAMPAIGN_KEY, CAMPAIGN_ID, CM_LAUNCH_KEY, LAUNCH_ID, SEGMENT_INFO,
			   FAIL_COUNT, SENT_COUNT, SKIPPED_COUNT, BOUNCE_COUNT, OPEN_COUNT, 
			   CLICK_COUNT, CONVERT_COUNT, COMPLAINT_COUNT, OPT_IN_COUNT, OPT_OUT_COUNT, RESPOND_COUNT,
			   EFF_START_DATETIME, JOB_RUN_ID, M_INSERT_DATETIME, M_LAST_UPDATE_DATETIME)AS BIGINT) AS EMAIL_INTERACTION_FACT_KEY, 
			   ORDER_ID, ORDER_HEADER_KEY, 
			   EVENT_DATE_KEY, CM_PLATFORM_KEY, RIID, EMAIL_KEY, CM_EMAIL_ADDRESS_KEY,
			   CM_LIST_KEY, LIST_ID, CM_CAMPAIGN_KEY, CAMPAIGN_ID, CM_LAUNCH_KEY, LAUNCH_ID, SEGMENT_INFO,
			   FAIL_COUNT, SENT_COUNT, SKIPPED_COUNT, BOUNCE_COUNT, OPEN_COUNT, 
			   CLICK_COUNT, CONVERT_COUNT, COMPLAINT_COUNT, OPT_IN_COUNT, OPT_OUT_COUNT, RESPOND_COUNT,
			   CURRENT_TIMESTAMP AS EFF_START_DATETIME, JOB_RUN_ID, CURRENT_TIMESTAMP AS M_INSERT_DATETIME, CURRENT_TIMESTAMP AS M_LAST_UPDATE_DATETIME
		FROM $NZ_STG_DB..CM_EMAIL_INTERACTION_FACT_CDC T
		WHERE T.JOB_RUN_ID = $job_run_id;
		"
echo ''		
echo $CDM_EMAIL_INTERACTION_SQL;
echo ''

	if [[ $verbose_logging = 1 ]]; then
		print_msg "$CDM_EMAIL_INTERACTION_SQL"
	fi

	run_query -d $NZ_PRSNT_DB -q "$CDM_EMAIL_INTERACTION_SQL" -m "0161 Failed while inserting email interaction data into CDM table"
	if [[ $? != 0 ]]
	then 
		perl endJobRun.pl "$job_run_id" "FAILURE"
		print_msg "0162 *****ERROR***** Failed while inserting email interaction data into CDM table in $NZ_PRSNT_DB.  $SCRIPT_NAME finished UNSUCCESSFULLY."
		exit $return_code_fail
	fi




print_msg "0170 Updating Email Interaction Fact table - EMAIL_KEY field"
#update email_key...not using decrypted view because email_addr is not needed
	export FACT_EMAIL_INTERACTION_UPDATE_SQL="
		UPDATE CM_EMAIL_INTERACTION_FACT T
		SET T.EMAIL_KEY = EA.EMAIL_KEY
		FROM $NZ_PRSNT_DB..CM_EMAIL_ADDRESS_DIM EA
		WHERE T.CM_EMAIL_ADDRESS_KEY = EA.CM_EMAIL_ADDRESS_KEY
			AND T.EMAIL_KEY = -2;

SELECT '';
SELECT CURRENT_TIMESTAMP;
SELECT '';
SELECT 'update order_header_key in target table for late arrival transactions';

UPDATE cm_email_interaction_fact c
   SET c.order_header_key = oh.order_header_key
     , c.m_last_update_datetime = current_timestamp
  FROM $NZ_PRSNT_DB..order_header oh
 WHERE c.order_id = oh.order_id
   AND c.order_header_key <> oh.order_header_key
   AND c.order_id IS NOT NULL ;

"

echo ''		
print_msg "$FACT_EMAIL_INTERACTION_UPDATE_SQL";
echo ''
		
	if [[ $verbose_logging = 1 ]]; then
		print_msg "$FACT_EMAIL_INTERACTION_UPDATE_SQL"
	fi

	run_query -d $NZ_PRSNT_DB -q "$FACT_EMAIL_INTERACTION_UPDATE_SQL" -m "0171 Failed while updating email interaction data Email_Key field in fact table"
	if [[ $? != 0 ]]
	then 
		perl endJobRun.pl "$job_run_id" "FAILURE"
		print_msg "0172 *****ERROR***** Failed while updating email interaction data Email_Key field in fact table in $NZ_PRSNT_DB.  $SCRIPT_NAME finished UNSUCCESSFULLY."
		exit $return_code_fail
	fi
	
	
	
	
	
	
print_msg "0300 Starting to truncate all records in level 2 cdc email interaction table in $NZ_STG_DB"	
	export CLEAR_CDC_TABLES_SQL="TRUNCATE TABLE CM_EMAIL_INTERACTION_FACT_CDC;"


		
	if [[ $verbose_logging = 1 ]]; then
		print_msg "$CLEAR_CDC_TABLES_SQL"
		print_msg "0301 NOTE: Level 2 cdc email interaction table were not purged because verbose logging is turned on"
	else	
		run_query -d $NZ_STG_DB -q "$CLEAR_CDC_TABLES_SQL" -m "0302 Failed while truncating records in level 2 cdc  email interaction table"
		if [[ $? != 0 ]]
		then 
			perl endJobRun.pl "$job_run_id" "FAILURE"
			print_msg "0403 *****ERROR***** Failed while truncating records in level 2 cdc  email interaction table in $NZ_STG_DB.  $SCRIPT_NAME finished UNSUCCESSFULLY."
			exit $return_code_fail
		else 
			print_msg "0304 All records in level 2 cdc  email interaction table truncated"
		fi	
	fi
	
	

	
	
print_msg "9999 Processing completed NORMALLY for $SCRIPT_NAME"

