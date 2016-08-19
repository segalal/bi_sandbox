#!/bin/ksh

########################################################
#
# Recreational Equipment Incorporated
#
# COPYRIGHT (c) 2000-2013 by Recreational Equipment Incorporated
#
# This software is furnished under a  license and may be used and copied  only
# in accordance with the terms of  such license and with the inclusion of  the
# above copyright notice.  This  software or any other copies thereof  may not
# be provided or  otherwise made available to  any other person.   No title to
# and ownership of the software is hereby transferred.
#
########################################################
#
# MODULE      : gc_stgcdm_giftcard_load1.ksh
# DESCRIPTION : Loads the STGCDM Gift Card Activity table with GiftCard AIM (fullfilment) and SVS (approved transactions) data
#
# NOTES: Here is the processing components and the order in which they are called:
#            Tidal Group: BI_CDM_DAILY_GIFTCARD_GROUP 
#            Tidal Job: BI_CDM_1_DAILY_GIFTCARD_RDW_TO_CDM
#               UNIX Script: gc_cdm_giftcard.ksh
#                  UNIX Script :gc_stgcdm_giftcard_load1.ksh
#                  UNIX Script : gc_cdm_giftcard_load2.ksh 
#                  UNIX Script : gc_cdm_giftcard_load3.ksh
#
########################################################
#
# MODIFICATION HISTORY:
#  Date       Name              Mods Description
#  ---------- ----------------- ---- -------------------------------------
#  02/06           /2013 Pooja Chadha           Loading CDM gift card tables for exposure to gift card universe users.
#  04/01/2013 Izic Chon		     Added four new columns (original_transaction_sequence_number, original_pos_date, original_pos_time, card_working_balance)
#  05/07/2013 Pooja Chadha           Data Model Changes and Transaction Groups addition for accurate Activation and Redemption Counts
#  05/09/2013 Anna Segal and Sumit Salian  Unique fact table primary keys
#  07/22/2013 Anna Segal             Fix for two Defects CHG50691 for ORDER_KEY assignment.  Added columns for existing_row_ind per order_key fix.
#                                    Added pull of historical data for ORDER_KEY correction based on new THF data that came in after the gift card SVS data.
#                                    Location 8 is to be treated like Location 10
#  08/20/2013 Anna Segal             Changed sql6 from gstc.transaction_code_key = gca.transaction_code to gstc.transaction_code = gca.transaction_code
#  08/21/2013 Anna Segal             Conversion of transaction_invoice_number per the Sterling orders (both in AIM and SVS data)
#  08/27/2013 Anna Segal             When merchant number is not 67314 (REI), then assign store_key = -2.  When the merchant_number is 70000, assign store_key 11 (storei_id 10)
#                                    For any merchant number, assign store_key 11 for location_id = 0000000008
#  08/29/2013 Anna Segal             NOTES:  1) The order_key should be updated from NULL to -2 in the work table of the LOAD1 script. This would eliminate the need to filter on "NULL or -2"
#                                    in the subsequent scripts.  2) Also the RDW Parse script gc_rdw_giftcard_card_activity_parse_level_1.ksh should TRIM all character columns. 
#                                    Care will need to be taken for this because the comparison between the source data and the target RDW data will still need to work
#                                    despite the removeal of spaces.  History will need to be updated FIRST with the trimmed data.  These are two future enhancements.
#                                    data.  These are two future enhancements.
# 11/14/2013 Anna Segal              This code needs to strip the leading "Y" until such time as the trans_header_fact is converted to character datatype order number columns 
# 05/11/2014 Srithar Venugopal       NOTES: To avoid the INT4 overflow while inserting the vaues into the cdm_card_activity_stg_target table, the datatype of ROWID as GIFT_TRANS                                     ACTION_ KEY  is changed from INTEGER to BIGINT
# 07/23/2015 Suresh Kumar			Changed data type for order number column from INTEGER to VARCHAR in the ETL flow to allow alphanumeric order numbers 
# 01/27/2016 Midhun Ramesh         Modified to fetch data from new txn tables instead of legacy transaction tables
# 03/02/2016 Midhun Ramesh         Modified the column name from order_key to order_header_key in gift card txn tables
# 03/03/2016 Govind Hassan 		Modified the logic as to remove leading 0 from client_order_id.
# MODIFICATION LEGEND:
#   B = Bugfixes
#   A = Architectural change
#   F = Feature addition
#   R = Code re-write
#   C = Comment update
#######################################################
export SCRIPT_NAME=$(basename  $0)
. $SCRIPTS_DIR/load_parameters.ksh
. $SCRIPTS_DIR/load_library.ksh
. $SCRIPTS_DIR/load_datastage_params.ksh


#  GIFT CARD CDM processing variables
export rdw_card_activity_source="GIFTCARD_CARD_ACTIVITY"
export cdm_card_activity_stg_target="STG_GIFTCARD_TRANSACTION_FACT"
export cdm_card_activity_prs_target="GIFTCARD_TRANSACTION_FACT"
export stgcdm_wrk_giftcard_transaction_fact="WRK_GIFTCARD_TRANSACTION_FACT"
export start_date_for_processing=$processing_datetime
export giftcard_go_live_date='2012-09-01'
export days_to_retrieve=10000

print_msg "0000 Processing started for $SCRIPT_NAME for $processing_datetime"

##############################################
# GIFT CARD CARD ACTIVITY                    #
##############################################

print_msg "0001 Beginning processing for SVS Card Activity (Approved Transactions) Load"

print_msg "0002 Job Run ID is $JOB_RUN_ID and current predecessor is $current_pred"

print_msg "0005 Drop work Table $stgcdm_wrk_giftcard_transaction_fact"
export cdmstg_drop_work_table_sql="DROP TABLE $NZ_STG_DB.$NZ_STG_SCHEMA.$stgcdm_wrk_giftcard_transaction_fact"

print_msg "0006 Query is $cdmstg_drop_work_table_sql"

print_msg "0007 Dropping table $NZ_STG_DB.$NZ_STG_SCHEMA.$stgcdm_wrk_giftcard_transaction_fact"
run_query -d $NZ_STG_DB -q "$cdmstg_drop_work_table_sql" -m "0008 Processing failed when attempting to drop table $NZ_STG_DB.$NZ_STG_SCHEMA.$stgcdm_wrk_giftcard_transaction_fact"
   if [[ $? != 0 ]]
   then
      perl endJobRun.pl "$JOB_RUN_ID" "FAILURE"
      chk_err -r $return_code_fail -m "0009 ===> ERROR <=== DROP TABLE $NZ_STG_DB.$NZ_STG_SCHEMA.$stgcdm_wrk_giftcard_transaction_fact failed.  $SCRIPT finished UNSUCCESSFULLY."
      exit $return_code_fail
   fi

print_msg "0010 Create Work Table"

export cdmstg_create_work_table_sql="CREATE TABLE $NZ_STG_DB.$NZ_STG_SCHEMA.$stgcdm_wrk_giftcard_transaction_fact
(
	STORE_KEY INTEGER,
	DATE_KEY INTEGER,
	ORDER_HEADER_KEY BIGINT,
	TRANSACTION_CODE_KEY BIGINT,
	TRANSACTION_GROUP_KEY INTEGER,
	RECORD_TYPE CHARACTER(1),
	BIN_RANGE_TAG CHARACTER VARYING(5),
	CARD_NUMBER CHARACTER VARYING(19),
	ALT_CARD_NUMBER CHARACTER VARYING(19),
	MERCHANT_NUMBER INTEGER,
	DIVISION_NUMBER INTEGER,
	LOCATION_ID CHARACTER VARYING(10),
	STATE CHARACTER(2),
	COUNTRY CHARACTER(2),
	TRANSACTION_CODE SMALLINT,
	VOIDED_FLAG SMALLINT,
	CREDIT_DEBIT_INDICATOR CHARACTER(1),
	TRANSACTION_SEQUENCE_NUMBER INTEGER,
	TRANSACTION_INVOICE_NUMBER CHARACTER VARYING(10),
	POS_TRANSACTION_DATETIME TIMESTAMP,
	POS_CURRENCY_CODE CHARACTER VARYING(3),
	POS_REQUESTED_AMOUNT INTEGER,
	POS_APPROVED_AMOUNT INTEGER,
	BASE_CURRENCY_CODE CHARACTER VARYING(3),
	BASE_REQUESTED_AMOUNT INTEGER,
	BASE_APPROVED_AMOUNT INTEGER,
	CURRENCY_CONVERSION_FACTOR INTEGER,
	CURRENCY_BASE_UNITS INTEGER,
	ACTIVATION_MERCHANT_NUMBER INTEGER,
	HOST_SYSTEM_DATETIME TIMESTAMP,
	ACTIVATION_DIVISION_NUMBER INTEGER,
	ACTIVATION_DATETIME TIMESTAMP,
	ACTIVATION_LOCATION_ID CHARACTER VARYING(10),
	CARD_WORKING_BALANCE BIGINT,
	EFFECTED_GIFTCARD_TRANSACTION_KEY BIGINT,
	EFFECTED_TRANSACTION_SEQUENCE_NUMBER INTEGER,
	EFFECTED_TRANSACTION_DATETIME TIMESTAMP,
        ELIGIBLE_EXISTING_ROW_IND SMALLINT,
        PROCESSING_CRITERIA_DESC VARCHAR(2000),
	EFF_START_DATETIME TIMESTAMP DEFAULT NOW(),
	EFF_END_DATETIME TIMESTAMP DEFAULT '9999-12-31 23:59:59',
	JOB_RUN_ID INTEGER,
	M_INSERT_DATETIME TIMESTAMP DEFAULT NOW(),
	M_LAST_UPDATE_DATETIME TIMESTAMP DEFAULT NOW()
)
DISTRIBUTE ON RANDOM"
 
print_msg "0011 Query is $cdmstg_create_work_table_sql"

print_msg "0012 Loading table $NZ_STG_DB.$NZ_STG_SCHEMA.$stgcdm_wrk_giftcard_transaction_fact"
run_query -d $NZ_STG_DB -q "$cdmstg_create_work_table_sql" -m "0003 Processing failed when attempting to create table $NZ_STG_DB.$NZ_STG_SCHEMA.$stgcdm_wrk_giftcard_transaction_fact"
   if [[ $? != 0 ]]
   then
      perl endJobRun.pl "$JOB_RUN_ID" "FAILURE"
      chk_err -r $return_code_fail -m "0003 ===> ERROR <=== CREATE TABLE $NZ_STG_DB.$NZ_STG_SCHEMA.$stgcdm_wrk_giftcard_transaction_fact failed.  $SCRIPT finished UNSUCCESSFULLY."
      exit $return_code_fail
   fi

print_msg "0020 Insert into work table $stgcdm_wrk_giftcard_transaction_fact"

export cdmstg_work_table_insert_new_sql="INSERT INTO $NZ_STG_DB.$NZ_STG_SCHEMA.$stgcdm_wrk_giftcard_transaction_fact
       					( STORE_KEY
      					, DATE_KEY
       					, ORDER_HEADER_KEY
       					, TRANSACTION_CODE_KEY
 					, TRANSACTION_GROUP_KEY
       					, RECORD_TYPE
      					, BIN_RANGE_TAG
       					, CARD_NUMBER
       					, ALT_CARD_NUMBER
       					, MERCHANT_NUMBER
       					, DIVISION_NUMBER
       					, LOCATION_ID
       					, STATE
       					, COUNTRY
       					, TRANSACTION_CODE
   					, VOIDED_FLAG
       					, CREDIT_DEBIT_INDICATOR
       					, TRANSACTION_SEQUENCE_NUMBER
      					, TRANSACTION_INVOICE_NUMBER
       					, POS_TRANSACTION_DATETIME
       					, POS_CURRENCY_CODE
       					, POS_REQUESTED_AMOUNT
       					, POS_APPROVED_AMOUNT
       					, BASE_CURRENCY_CODE
       					, BASE_REQUESTED_AMOUNT
       					, BASE_APPROVED_AMOUNT
       					, CURRENCY_CONVERSION_FACTOR
       					, CURRENCY_BASE_UNITS
       					, ACTIVATION_MERCHANT_NUMBER
       					, HOST_SYSTEM_DATETIME
       					, ACTIVATION_DIVISION_NUMBER
       					, ACTIVATION_DATETIME
       					, ACTIVATION_LOCATION_ID
       					, CARD_WORKING_BALANCE
       					, EFFECTED_GIFTCARD_TRANSACTION_KEY
       					, EFFECTED_TRANSACTION_SEQUENCE_NUMBER
       					, EFFECTED_TRANSACTION_DATETIME
                                        , ELIGIBLE_EXISTING_ROW_IND
                                        , PROCESSING_CRITERIA_DESC
       				        , EFF_START_DATETIME
       				        , EFF_END_DATETIME
      					, JOB_RUN_ID
     					, M_INSERT_DATETIME
      					, M_LAST_UPDATE_DATETIME)
			SELECT    CASE
                                     WHEN gca.location_id = '0000000008' THEN 11  -- store_id 10
                                     WHEN gca.location_id <> '0000000008' AND gca.merchant_number = 67314 THEN COALESCE(store_dim.store_key,-2) -- REI Merchant
                                     WHEN gca.merchant_number = 70000 THEN 11 -- store_id 10 
                                     ELSE -2
                                  END AS store_key
			        , COALESCE(DATE_DIM.DATE_KEY,-2)
				, null AS order_header_key
				, COALESCE(GIFTCARD_TRANSACTION_CODE.TRANSACTION_CODE_KEY,-2)
				, COALESCE(gcga.TRANSACTION_GROUP_KEY,-2)
       				, RECORD_TYPE
       				, BIN_RANGE_TAG
       				, CARD_NUMBER
       				, ALT_CARD_NUMBER
       				, MERCHANT_NUMBER
       				, DIVISION_NUMBER
       				, LOCATION_ID
       				, STATE
       				, COUNTRY
       				, gca.TRANSACTION_CODE
                                , gcga.VOIDED_FLAG
       				, gca.CREDIT_DEBIT_INDICATOR
       				, TRANSACTION_SEQUENCE_NUMBER
                                , TRIM(NVL(gca.transaction_invoice_number,'-2')) 
                                  AS TRANSACTION_INVOICE_NUMBER      
       				, to_timestamp(POS_DATE||STRRIGHT('000000' ||POS_TIME,6),'YYYYMMDDHHMISS')
       				, POS_CURRENCY_CODE
       				, POS_REQUESTED_AMOUNT
       				, POS_APPROVED_AMOUNT
       				, BASE_CURRENCY_CODE
       				, BASE_REQUESTED_AMOUNT
       				, BASE_APPROVED_AMOUNT
       				, CURRENCY_CONVERSION_FACTOR
       				, CURRENCY_BASE_UNITS
       				, ACTIVATION_MERCHANT_NUMBER
	   			, to_timestamp(HOST_DATE||STRRIGHT('000000' ||HOST_TIME,6),'YYYYMMDDHHMISS')
       			        , ACTIVATION_DIVISION_NUMBER
	   			, to_timestamp(ACTIVATION_DATE||STRRIGHT('000000' ||ACTIVATION_TIME,6),'YYYYMMDDHHMISS')
       				, ACTIVATION_LOCATION_ID
       				, CARD_WORKING_BALANCE
	   			, NULL
	   			, ORIGINAL_TRANSACTION_SEQUENCE_NUMBER
	  		, to_timestamp(decode(ORIGINAL_POS_DATE||STRRIGHT('000000' ||ORIGINAL_POS_TIME,6),'0000000', NULL, ORIGINAL_POS_DATE||STRRIGHT('000000' ||ORIGINAL_POS_TIME,6)),'YYYYMMDDHHMISS') 
                                , 0 AS ELIGIBLE_EXISTING_ROW_IND
                                , 'NEW ROW' AS ORDER_HEADER_KEY_PROCESSING_DESC 
	    		        , gcga.eff_start_datetime
				, gcga.eff_end_datetime
				, $JOB_RUN_ID
                    		, '$processing_datetime' AS m_insert_datetime
                    		, '$processing_datetime' AS m_last_update_datetime
  				FROM   $NZ_RDW_DB.$NZ_RDW_SCHEMA.$rdw_card_activity_source gca
  				LEFT OUTER JOIN   $NZ_PRSNT_DB.$NZ_PRSNT_SCHEMA.STORE_DIM ON (STORE_DIM.STORE_ID = cast(gca.location_id as bigint))
  				LEFT OUTER JOIN   $NZ_PRSNT_DB.$NZ_PRSNT_SCHEMA.DATE_DIM ON ( date_dim.DAY_DATE = TO_DATE(gca.POS_DATE,'YYYYMMDD'))
  				LEFT OUTER JOIN   $NZ_RDW_DB.$NZ_RDW_SCHEMA.GIFTCARD_TRANSACTION_CODE ON(GIFTCARD_TRANSACTION_CODE.TRANSACTION_CODE = gca.TRANSACTION_CODE )
				LEFT OUTER JOIN   $NZ_RDW_DB.$NZ_RDW_SCHEMA.GIFTCARD_CARD_GROUP_ACTIVITY gcga ON (gcga.CARD_ACTIVITY_KEY = gca.CARD_ACTIVITY_KEY)
				JOIN   $NZ_METAMART_DB.$NZ_METAMART_SCHEMA.$job_run j ON ( gca.job_run_id = j.job_run_id AND j.job_run_id=$current_pred)
					WHERE NOT EXISTS (SELECT 1 
                                                          FROM $NZ_PRSNT_DB.$NZ_PRSNT_SCHEMA.GIFTCARD_TRANSACTION_FACT r
                                JOIN $NZ_METAMART_DB.$NZ_METAMART_SCHEMA.JOB_RUN jr ON r.job_run_id = jr.job_run_id 
                                    AND (jr.processing_status = '$processing_status_success' 
                                    OR  (jr.processing_status = '$processing_status_running'  
                                    AND  jr.job_run_id=$JOB_RUN_ID ))
       			        WHERE r.TRANSACTION_CODE_KEY = GIFTCARD_TRANSACTION_CODE.TRANSACTION_CODE_KEY 
				AND r.TRANSACTION_GROUP_KEY = gcga.TRANSACTION_GROUP_KEY
       				AND r.RECORD_TYPE = gca.RECORD_TYPE
       				AND r.BIN_RANGE_TAG = gca.BIN_RANGE_TAG
       				AND r.CARD_NUMBER = gca.CARD_NUMBER
       				AND r.ALT_CARD_NUMBER = gca.ALT_CARD_NUMBER
       				AND r.MERCHANT_NUMBER = gca.MERCHANT_NUMBER
       				AND r.DIVISION_NUMBER = gca.DIVISION_NUMBER
       				AND r.LOCATION_ID = gca.LOCATION_ID
       				AND r.STATE = gca.STATE
       				AND r.COUNTRY = gca.COUNTRY
       				AND r.TRANSACTION_CODE = gca.TRANSACTION_CODE
                                AND r.VOIDED_FLAG = gcga.VOIDED_FLAG
       				AND r.CREDIT_DEBIT_INDICATOR = gca.CREDIT_DEBIT_INDICATOR
       				AND r.TRANSACTION_SEQUENCE_NUMBER = gca.TRANSACTION_SEQUENCE_NUMBER
       				AND r.TRANSACTION_INVOICE_NUMBER = gca.TRANSACTION_INVOICE_NUMBER
       				AND r.POS_TRANSACTION_DATETIME = to_timestamp(gca.POS_DATE||STRRIGHT('000000' ||gca.POS_TIME,6),'YYYYMMDDHHMISS')
       				AND r.POS_CURRENCY_CODE = gca.POS_CURRENCY_CODE
       				AND r.POS_REQUESTED_AMOUNT = gca.POS_REQUESTED_AMOUNT
       				AND r.POS_APPROVED_AMOUNT = gca.POS_APPROVED_AMOUNT
       				AND r.BASE_CURRENCY_CODE = gca.BASE_CURRENCY_CODE
       				AND r.BASE_REQUESTED_AMOUNT = gca.BASE_REQUESTED_AMOUNT
       				AND r.BASE_APPROVED_AMOUNT = gca.BASE_APPROVED_AMOUNT
       				AND r.CURRENCY_CONVERSION_FACTOR = gca.CURRENCY_CONVERSION_FACTOR
       				AND r.CURRENCY_BASE_UNITS = gca.CURRENCY_BASE_UNITS
       				AND r.ACTIVATION_MERCHANT_NUMBER = gca.ACTIVATION_MERCHANT_NUMBER
	   		        AND r.HOST_SYSTEM_DATETIME = to_timestamp(gca.HOST_DATE||STRRIGHT('000000' ||gca.HOST_TIME,6),'YYYYMMDDHHMISS')
       				AND r.ACTIVATION_DIVISION_NUMBER = gca.ACTIVATION_DIVISION_NUMBER
	   			AND r.ACTIVATION_DATETIME = to_timestamp(gca.ACTIVATION_DATE||STRRIGHT('000000' ||gca.ACTIVATION_TIME,6),'YYYYMMDDHHMISS')
       				AND r.ACTIVATION_LOCATION_ID = gca.ACTIVATION_LOCATION_ID
       				AND r.CARD_WORKING_BALANCE = gca.CARD_WORKING_BALANCE
	   			AND r.EFFECTED_TRANSACTION_SEQUENCE_NUMBER = gca.ORIGINAL_TRANSACTION_SEQUENCE_NUMBER
	   			AND coalesce(r.EFFECTED_TRANSACTION_DATETIME,'1900-01-01 00:00:00') = coalesce(to_timestamp(decode(ORIGINAL_POS_DATE||STRRIGHT('000000' ||ORIGINAL_POS_TIME,6),'0000000', NULL, ORIGINAL_POS_DATE||STRRIGHT('000000' ||ORIGINAL_POS_TIME,6)),'YYYYMMDDHHMISS'),'1900-01-01 00:00:00'))"

echo ''
print_msg "0021 Query is cdmstg_work_table_insert_new_sql ==> $cdmstg_work_table_insert_new_sql"
echo ''

print_msg "0022 Loading table $NZ_STG_DB.$NZ_STG_SCHEMA.$stgcdm_wrk_giftcard_transaction_fact with new data only"
run_query -d $NZ_STG_DB -q "$cdmstg_work_table_insert_new_sql" -m "0023 Work table Load Processing failed when attempting to insert into table $NZ_STG_DB.$NZ_STG_SCHEMA.$stgcdm_wrk_giftcard_transaction_fact for new data"
   if [[ $? != 0 ]]
   then
      perl endJobRun.pl "$JOB_RUN_ID" "FAILURE"
      chk_err -r $return_code_fail -m "0024 ===> ERROR <=== INSERT into table $NZ_STG_DB.$NZ_STG_SCHEMA.$stgcdm_wrk_giftcard_transaction_fact failed for inserting new data.  $SCRIPT finished UNSUCCESSFULLY."
      exit $return_code_fail
   fi

print_msg "0030 NOTES:  There are two notes dated 8/29/2013 in the LOAD1 and LOAD2 scripts.  These are future enhancements."

#  NOTES! 1) An update statement for order_header_key should be moved from the LOAD2 script to this script (LOAD1).  The order_header_key should be set to -2 quite early (here) thus eliminating the need to check for
#            "NULL or -2" order_header_key in all of the subsequent code of LOAD2 and LOAD3.
#
#         2) The RDW Parse script gc_rdw_giftcard_card_activity_parse_level_1.ksh should be altered to trim all character columns. 
#            All history will need to be updated at the same time to allow the comparisons to work when detecting change 
#

print_msg "0040 Insert into work table $stgcdm_wrk_giftcard_transaction_fact existing rows only which may obtain new ORDER_HEADER_KEY.  Note:  business_dates are dates while pos_transaction_datetimes are timestamps."

export cdmstg_work_table_insert_existing_sql="INSERT INTO $NZ_STG_DB.$NZ_STG_SCHEMA.$stgcdm_wrk_giftcard_transaction_fact
			SELECT                    gf.store_key
						, gf.date_key
						, DECODE(gf.order_header_key,-2,NULL,gf.order_header_key) AS order_header_key
						, gf.transaction_code_key
						, gf.transaction_group_key
						, gf.record_type
						, gf.bin_range_tag
						, gf.card_number
						, gf.alt_card_number
						, gf.merchant_number
						, gf.division_number
						, gf.location_id
						, gf.state
						, gf.country
						, gf.transaction_code
						, gf.voided_flag
						, gf.credit_debit_indicator
						, gf.transaction_sequence_number
						, gf.transaction_invoice_number
						, gf.pos_transaction_datetime
						, gf.pos_currency_code
						, gf.pos_requested_amount
						, gf.pos_approved_amount
						, gf.base_currency_code
						, gf.base_requested_amount
						, gf.base_approved_amount
						, gf.currency_conversion_factor
						, gf.currency_base_units
						, gf.activation_merchant_number
						, gf.host_system_datetime
						, gf.activation_division_number
						, gf.activation_datetime
						, gf.activation_location_id
						, gf.card_working_balance
						, gf.effected_giftcard_transaction_key
						, gf.effected_transaction_sequence_number
						, gf.effected_transaction_datetime
						, -2 AS eligible_existing_row_ind
                                                , 'UNKNOWN' AS processing_criteria_desc
						, gf.eff_start_datetime
						, gf.eff_end_datetime
						, gf.job_run_id
						, gf.m_insert_datetime
						, gf.m_last_update_datetime
			FROM (SELECT DISTINCT existing.*
			      FROM    ( SELECT gf.* -- AIM DATA (Merchant #7000) - Non Retail - LOAD1 Hist pull per LOAD2 sql2 
					FROM $NZ_PRSNT_DB.$NZ_PRSNT_SCHEMA.$cdm_card_activity_prs_target gf
                                        JOIN $NZ_METAMART_DB.$NZ_METAMART_SCHEMA.JOB_RUN j ON gf.job_run_id = j.job_run_id AND j.processing_status = '$processing_status_success'
					INNER JOIN (SELECT STRRIGHT('0000' || vendor_order_id, 8) AS aim_order_id, 
                                                           client_order_id 
							FROM $NZ_RDW_DB.$NZ_RDW_SCHEMA.giftcard_order_recon gor
                                                        JOIN $NZ_METAMART_DB.$NZ_METAMART_SCHEMA.JOB_RUN j ON gor.job_run_id = j.job_run_id AND j.processing_status = '$processing_status_success'
                                                                    ) gor ON TRIM(gf.transaction_invoice_number) = gor.aim_order_id
					INNER JOIN $NZ_PRSNT_DB.$NZ_PRSNT_SCHEMA.order_header oh ON
                                                        CASE
                                                           WHEN gor.client_order_id not like '%-%' 
								   --THEN SUBSTR(gor.client_order_id,1,10)
								   THEN trim(leading '0' from (SUBSTR(gor.client_order_id,1,10)))
                                                           --ELSE GET_VALUE_VARCHAR(ARRAY_SPLIT(gor.client_order_id,'-'),1)
								   ELSE trim(leading '0' from GET_VALUE_VARCHAR(ARRAY_SPLIT(gor.client_order_id,'-'),1))
                                                        END = TRIM(oh.order_id)	 AND DATE(oh.order_datetime) >= DATE(gf.pos_transaction_datetime) - 30
					JOIN $NZ_PRSNT_DB.$NZ_PRSNT_SCHEMA.order_entry_application oea  ON oea.order_entry_application_key = oh.order_entry_application_key
					WHERE (gf.order_header_key > oh.order_header_key OR gf.order_header_key = -2)
                                        AND '$processing_datetime' BETWEEN gf.eff_start_datetime AND gf.eff_end_datetime 
					AND gf.merchant_number = 70000
					AND (oea.order_entry_application_id NOT IN('POS','ERADMIN') OR (oea.order_entry_application_id ='MSA' AND oh.pos_register_id IS NULL))
					AND EXISTS (SELECT 1
					            FROM $NZ_PRSNT_DB.$NZ_PRSNT_SCHEMA.order_line ol
					            WHERE ol.order_header_key = oh.order_header_key)
					UNION -- SVS REI (Merchant #67314) - Non Retail - LOAD1 Hist pull per LOAD2 sql3
                                        SELECT gf.*
                                        FROM $NZ_PRSNT_DB.$NZ_PRSNT_SCHEMA.$cdm_card_activity_prs_target gf
                                        JOIN $NZ_METAMART_DB.$NZ_METAMART_SCHEMA.JOB_RUN j ON gf.job_run_id = j.job_run_id AND j.processing_status = '$processing_status_success'
                                        JOIN $NZ_PRSNT_DB.$NZ_PRSNT_SCHEMA.order_header oh ON
                                                        TRIM(gf.transaction_invoice_number) = TRIM(oh.order_id)
                                                         AND DATE(oh.order_datetime) >= DATE(gf.pos_transaction_datetime) - 30
                                        JOIN $NZ_PRSNT_DB.$NZ_PRSNT_SCHEMA.order_entry_application oea  ON oea.order_entry_application_key = oh.order_entry_application_key
                                        WHERE (gf.order_header_key > oh.order_header_key OR gf.order_header_key = -2)
                                        AND '$processing_datetime' BETWEEN gf.eff_start_datetime AND gf.eff_end_datetime
                                        AND gf.merchant_number = 67314
                                        AND  (oea.order_entry_application_id NOT IN('POS','ERADMIN') OR (oea.order_entry_application_id ='MSA' AND oh.pos_register_id IS NULL))
                                        AND gf.location_id IN ('0000000008',  '0000000010')
                                        AND EXISTS (SELECT 1
                                                    FROM $NZ_PRSNT_DB.$NZ_PRSNT_SCHEMA.order_line ol
                                                    WHERE ol.order_header_key = oh.order_header_key) -- A record exists in order line for this order
				/*	UNION  -- SVS REI (Merchant #67314) Internet Orders - Non-Retail - LOAD1 Hist pull per LOAD2 sql4'
                                        SELECT gf.*
					FROM $NZ_PRSNT_DB.$NZ_PRSNT_SCHEMA.$cdm_card_activity_prs_target gf
                                        JOIN $NZ_METAMART_DB.$NZ_METAMART_SCHEMA.JOB_RUN j ON gf.job_run_id = j.job_run_id AND j.processing_status = '$processing_status_success'
					JOIN $NZ_PRSNT_DB.$NZ_PRSNT_SCHEMA.order_header oh
							--ON TRIM(gf.transaction_invoice_number) = TRIM(CAST(oh.internet_order_nbr AS VARCHAR(10)))
						ON TRIM(gf.transaction_invoice_number) = TRIM(CAST(oh.order_id AS VARCHAR(10)))
							          AND DATE(oh.order_datetime) >= DATE(gf.pos_transaction_datetime) - 30 
                                        INNER JOIN $NZ_PRSNT_DB.$NZ_PRSNT_SCHEMA.order_tender ot  ON ot.order_header_key = oh.order_header_key -- GC txns only for redemptions
                                        INNER JOIN $NZ_PRSNT_DB.$NZ_PRSNT_SCHEMA.tender_type_dim ttd ON 
                                           ot.tender_type_key = ttd.tender_type_key AND
                                           ttd.tender_category_desc IN ('Gift Certificate', 'Merchant Credit', 'Refund Vouncher')
                                        JOIN $NZ_PRSNT_DB.$NZ_PRSNT_SCHEMA.order_entry_application oea  ON oea.order_entry_application_key = oh.order_entry_application_key
					WHERE (gf.order_header_key > oh.order_header_key OR gf.order_header_key = -2)
                                        AND '$processing_datetime' BETWEEN gf.eff_start_datetime AND gf.eff_end_datetime
					AND gf.merchant_number = 67314
                                        AND  (oea.order_entry_application_id NOT IN('POS','ERADMIN') OR (oea.order_entry_application_id ='MSA' AND oh.pos_register_id IS NULL))
                                        AND gf.location_id IN ('0000000008',  '0000000010')
					AND EXISTS (SELECT 1
         			 	            FROM $NZ_PRSNT_DB.$NZ_PRSNT_SCHEMA.order_line ol
						    WHERE ol.order_header_key = oh.order_header_key) -- A record exists in Trans Line Fact for this order
					UNION -- SVS REI (#67314) Internet Orders - Activations Articles - LOAD1 Hist pull per LOAD2 sql5
                                        SELECT gf.*
                                        FROM $NZ_PRSNT_DB.$NZ_PRSNT_SCHEMA.$cdm_card_activity_prs_target gf
                                        JOIN $NZ_METAMART_DB.$NZ_METAMART_SCHEMA.JOB_RUN j ON gf.job_run_id = j.job_run_id AND j.processing_status = '$processing_status_success'
                                        JOIN $NZ_PRSNT_DB.$NZ_PRSNT_SCHEMA.order_header oh
                                                       -- ON TRIM(gf.transaction_invoice_number) = TRIM(CAST(oh.internet_order_nbr AS VARCHAR(10)))
							ON TRIM(gf.transaction_invoice_number) = TRIM(CAST(oh.order_id AS VARCHAR(10)))
                                                            AND DATE(oh.order_datetime) >= DATE(gf.pos_transaction_datetime) - 30
                                        INNER JOIN $NZ_PRSNT_DB.$NZ_PRSNT_SCHEMA.order_line ol ON ol.order_header_key = oh.order_header_key 
                                        JOIN $NZ_PRSNT_DB.$NZ_PRSNT_SCHEMA.srv_article_dim a ON ol.sad_key = a.sad_key AND a.srv_article IN (9000000006, 9990490006, 9000000208) -- GC txns only  
                                         JOIN $NZ_PRSNT_DB.$NZ_PRSNT_SCHEMA.order_entry_application oea  ON oea.order_entry_application_key = oh.order_entry_application_key
                                        WHERE (gf.order_header_key > oh.order_header_key OR gf.order_header_key = -2)
                                        AND '$processing_datetime' BETWEEN gf.eff_start_datetime AND gf.eff_end_datetime
                                        AND gf.merchant_number = 67314
                                        AND  (oea.order_entry_application_id NOT IN('POS','ERADMIN') OR (oea.order_entry_application_id ='MSA' AND oh.pos_register_id IS NULL))
                                        AND gf.location_id IN ('0000000008',  '0000000010')
                                        AND EXISTS (SELECT 1
                                                    FROM $NZ_PRSNT_DB.$NZ_PRSNT_SCHEMA.order_line ol
                                                    WHERE ol.order_header_key = oh.order_header_key) -- A record exists in order line for this order
                                       */ UNION -- RETAIL TRANSACTIONS -  LOAD1 Hist pull per LOAD2 sql6
                                        SELECT gf.*
                                        FROM (SELECT gf.* 
                                              FROM $NZ_PRSNT_DB.$NZ_PRSNT_SCHEMA.$cdm_card_activity_prs_target gf
                                              JOIN $NZ_METAMART_DB.$NZ_METAMART_SCHEMA.JOB_RUN j ON gf.job_run_id = j.job_run_id AND j.processing_status = '$processing_status_success'
                                              WHERE gf.merchant_number = 67314
                                              AND REGEXP_LIKE(TRIM(gf.transaction_invoice_number), '^\d{7}$')) gf -- retail txns have trx invoice number made up of 7 digits
                                        JOIN $NZ_PRSNT_DB.$NZ_PRSNT_SCHEMA.order_header oh
                                                        ON CAST(STRLEFT(TRIM(gf.transaction_invoice_number),3) AS INTEGER) = oh.pos_register_id 
				                        AND CAST(STRRIGHT(TRIM(gf.transaction_invoice_number),4) AS INTEGER) = oh.pos_trans_nbr 
                                                                  AND DATE(oh.order_datetime) >= DATE(gf.pos_transaction_datetime) - 30
                                        INNER JOIN $NZ_PRSNT_DB.$NZ_PRSNT_SCHEMA.order_entry_application oea  ON oea.order_entry_application_key = oh.order_entry_application_key
 						AND  (oea.order_entry_application_id IN('POS','ERADMIN') OR (oea.order_entry_application_id = 'MSA' AND oh.pos_register_id IS NOT NULL))
                                        INNER JOIN $NZ_RDW_DB.$NZ_RDW_SCHEMA.giftcard_transaction_code gstc ON
                                                     gstc.transaction_code = gf.transaction_code AND
                                                     gstc.transaction_source <> 'IVR' AND
                                                     gstc.transaction_type <> 'Batch'
                                        WHERE (gf.order_header_key > oh.order_header_key OR gf.order_header_key = -2)
                                        AND '$processing_datetime' BETWEEN gf.eff_start_datetime AND gf.eff_end_datetime
                                        AND EXISTS (SELECT 1
                                                    FROM $NZ_PRSNT_DB.$NZ_PRSNT_SCHEMA.order_line ol
                                                    WHERE ol.order_header_key = oh.order_header_key) -- A record exists in order line for this order
                                        UNION -- Profile Activations (Transaction Code 98) - LOAD2 sql7'
                                        SELECT gf_activation.*
					FROM $NZ_PRSNT_DB.$NZ_PRSNT_SCHEMA.$cdm_card_activity_prs_target gf_activation 
                                        JOIN $NZ_METAMART_DB.$NZ_METAMART_SCHEMA.JOB_RUN j ON gf_activation.job_run_id = j.job_run_id AND j.processing_status = '$processing_status_success'
                                        JOIN (SELECT gf_enable.* 
                                              FROM $NZ_PRSNT_DB.$NZ_PRSNT_SCHEMA.$cdm_card_activity_prs_target gf_enable
                                              JOIN $NZ_METAMART_DB.$NZ_METAMART_SCHEMA.JOB_RUN j ON gf_enable.job_run_id = j.job_run_id AND j.processing_status = '$processing_status_success'
                                              WHERE gf_enable.transaction_code = 60
                                              AND '$processing_datetime' BETWEEN gf_enable.eff_start_datetime AND gf_enable.eff_end_datetime
                                              ) gf_enable 
                                           ON gf_enable.card_number = gf_activation.card_number
					WHERE gf_activation.transaction_code = 98 
                                        AND '$processing_datetime' BETWEEN gf_activation.eff_start_datetime AND gf_activation.eff_end_datetime
                                        AND '$processing_datetime' BETWEEN gf_enable.eff_start_datetime AND gf_enable.eff_end_datetime
                                        UNION -- Profile Enables (Transaction Code 60) - LOAD2 sql7'
                                        SELECT gf_enable.*
                                        FROM $NZ_PRSNT_DB.$NZ_PRSNT_SCHEMA.$cdm_card_activity_prs_target gf_activation
                                        JOIN $NZ_METAMART_DB.$NZ_METAMART_SCHEMA.JOB_RUN j ON gf_activation.job_run_id = j.job_run_id AND j.processing_status = '$processing_status_success'
                                        JOIN (SELECT gf_enable.*
                                              FROM $NZ_PRSNT_DB.$NZ_PRSNT_SCHEMA.$cdm_card_activity_prs_target gf_enable
                                              JOIN $NZ_METAMART_DB.$NZ_METAMART_SCHEMA.JOB_RUN j ON gf_enable.job_run_id = j.job_run_id AND j.processing_status = '$processing_status_success'
                                              WHERE gf_enable.transaction_code = 60
                                              AND '$processing_datetime' BETWEEN gf_enable.eff_start_datetime AND gf_enable.eff_end_datetime
                                              ) gf_enable
                                           ON gf_enable.card_number = gf_activation.card_number
                                        WHERE gf_activation.transaction_code = 98
                                        AND '$processing_datetime' BETWEEN gf_activation.eff_start_datetime AND gf_activation.eff_end_datetime
                                        AND '$processing_datetime' BETWEEN gf_enable.eff_start_datetime AND gf_enable.eff_end_datetime
					) existing ) gf"

echo ''
print_msg "0041 Query is cdmstg_work_table_insert_existing_sql ==> $cdmstg_work_table_insert_existing_sql"
echo ''

print_msg "0042 Loading table $NZ_STG_DB.$NZ_STG_SCHEMA.$stgcdm_wrk_giftcard_transaction_fact with existing data only"
run_query -d $NZ_STG_DB -q "$cdmstg_work_table_insert_existing_sql" -m "0043 Work table Load Processing failed when attempting to insert into table $NZ_STG_DB.$NZ_STG_SCHEMA.$stgcdm_wrk_giftcard_transaction_fact for existing data"
   if [[ $? != 0 ]]
   then
      perl endJobRun.pl "$JOB_RUN_ID" "FAILURE"
      chk_err -r $return_code_fail -m "0044 ===> ERROR <=== INSERT into table $NZ_STG_DB.$NZ_STG_SCHEMA.$stgcdm_wrk_giftcard_transaction_fact failed for inserting existing data.  $SCRIPT finished UNSUCCESSFULLY."
      exit $return_code_fail
   fi


print_msg "0050 Insert rows with unique keys into $NZ_STG_DB.$NZ_STG_SCHEMA.$cdm_card_activity_stg_target"

export cdmstg_card_activity_load_sql="INSERT INTO $NZ_STG_DB.$NZ_STG_SCHEMA.$cdm_card_activity_stg_target
                                      SELECT  CAST((SELECT COALESCE(MAX(GIFTCARD_TRANSACTION_KEY),0) FROM $NZ_PRSNT_DB.$NZ_PRSNT_SCHEMA.$cdm_card_activity_prs_target)
                                                 + (SELECT COALESCE(MAX(GIFTCARD_TRANSACTION_KEY),0) FROM $NZ_STG_DB.$NZ_STG_SCHEMA.STG_GIFTCARD_TRANSACTION_FACT)  
                                                 + RANK() OVER (ORDER BY A.ROWID) AS BIGINT) AS GIFTCARD_TRANSACTION_KEY,
                                              A.*
                                      FROM $NZ_STG_DB.$NZ_STG_SCHEMA.$stgcdm_wrk_giftcard_transaction_fact A
                                      ORDER BY A.POS_TRANSACTION_DATETIME" 

print_msg "0051 Query is $cdmstg_card_activity_load_sql"

print_msg "0052 Loading table $NZ_STG_DB.$NZ_STG_SCHEMA.$cdm_card_activity_stg_target"
run_query -d $NZ_STG_DB -q "$cdmstg_card_activity_load_sql" -m "0053 CARD ACTIVITY STG Load Processing failed when attempting to insert into table $NZ_STG_DB.$NZ_STG_SCHEMA.$cdm_card_activity_stg_target from table $NZ_RDW_DB.$NZ_RDW_SCHEMA.$rdw_card_activity_source"
   if [[ $? != 0 ]]
   then
      perl endJobRun.pl "$JOB_RUN_ID" "FAILURE"
      chk_err -r $return_code_fail -m "0054 ===> ERROR <=== INSERT into table $NZ_STG_DB.$NZ_STG_SCHEMA.$cdm_card_activity_stg_target failed.  $SCRIPT finished UNSUCCESSFULLY."
      exit $return_code_fail
   fi

#echo "*******************************************************  EARLY EXIT ********************************************"
#exit 3
   
print_msg "0100 Processing complete for Card Activity Load into table $NZ_RDW_DB.$NZ_RDW_SCHEMA.$cdm_card_activity_prs_target"
echo ""


print_msg "9999 Processing complete for $SCRIPT_NAME with return code = $return_code_succeed"

