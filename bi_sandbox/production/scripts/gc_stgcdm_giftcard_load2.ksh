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
# MODULE      : gc_cdm_giftcard_load2.ksh 
# DESCRIPTION : Updates the STGCDM Gift Card Activity table with order keys 
#
# ABSTRACT:
#
# NOTES: Here is the processing components and the order in which they are called:
#            Tidal Group: BI_CDM_DAILY_GIFTCARD_GROUP 
#            Tidal Job: BI_CDM_1_DAILY_GIFTCARD_RDW_TO_CDM
#               UNIX Script: gc_cdm_giftcard.ksh
#                  UNIX Script :gc_stgcdm_giftcard_load1.ksh
#                  UNIX Script : gc_cdm_giftcard_load2.ksh 
#                  UNIX Script : gc_cdm_giftcard_load3.ksh
#
# SYNOPSIS:
#
########################################################
#
# MODIFICATION HISTORY:
#  Date       Name              Mods Description
#  ---------- ----------------- ---- -------------------------------------
#  02/06/2013 Pooja Chadha           Loading CDM gift card tables for exposure to gift card universe users.
#  07/05/2013 Pooja Chadha           Changes to Data Model and Activation Redemption Groupings
#  07/23/2013 Anna Segal	     Fix for two Defects CHG50691 for ORDER_KEY assignment.  Added history pull to stgcdm fact table to allow 
#                                    previously processed rows to obtain the correct order_key when that key comes on a later day to TLF. 
#                                    Added metadata checking for successful processing per updates and inserts
#                                    Added date parameters for number of days to pull from history (future:  appprops)
#                                    Location 8 is to be treated like Location 10
#  08/19/2013 Anna Segal             Decode correction for eligible_existing_row. Concatenation for processing_criteria_desc
#                                    Removed AND  gca_activation.order_key = -2 from sql7.  Enable order_key (60) should be allowed to overwrite activation (98) order_key
#  08/20/2013 Anna Segal             Changed sql6 from gstc.transaction_code_key = gca.transaction_code to gstc.transaction_code = gca.transaction_code 
#                                    Allow new order_key to be assigned whenever it is deemed appropriate, regardless of wheter and existing key is there
#  08/21/2013 Anna Segal             Conversion of transaction_invoice_number per the Sterling orders (both in AIM and SVS data)
#  08/29/2013 Anna Segal             Two NOTES:  1) The order_key should be updated from NULL to -2 in the work table of the LOAD1 script. This would eliminate the need to filter on "NULL or -2"
#                                    in the subsequent scripts.  Also the RDW Parse script gc_rdw_giftcard_card_activity_parse_level_1.ksh should TRIM all character columns.  
#                                    Care will need to be taken for this because the comparison between the source data and the target RDW data will still need to work 
#                                    despite the removeal of spaces.  History will need to be updated FIRST with the trimmed data.  These are two future enhancements.
#  11/14/2013 Anna Segal             This code needs to test appropriately per the leading "Y" until such time as the
#                                    trans_header_fact is converted to character datatype order number columns.  Added a test for the Y in the code that pre-pends a 1, but later 
#                                    the "1 pre-pending" code should be completely removed to allow for order numbers from any system to be in any number range.  
#
# 07/23/2015 Suresh Kumar			Changed data type for order number column from INTEGER to VARCHAR in the ETL flow to allow alphanumeric order numbers 
# 01/27/2016 Midhun Ramesh                Modified to fetch data from new txn tables instead of legacy transaction tables
# 02/03/2016 Midhun Ramesh                Modified the column name from order_key to order_header_key in gift card txn tables
# 02/29/2015 Govind Hassan          Modified the logic for transaction_invoice_number. Code that pre-append of 1 is removed and check of < 300000 is removed.
#                                   Also lpad of pos trans nbr and retail id to ftech proper transaction invoice number and 
#                                   also modifcation of logic in client order id to have proper match with order header table.
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


print_msg "0000 Processing started for $SCRIPT_NAME for $processing_datetime"

##############################################
# CARD ACTIVITY                              #
##############################################

export cdm_card_activity_stg_target="STG_GIFTCARD_TRANSACTION_FACT"
export giftcard_go_live_date='2012-09-01'
echo $giftcard_go_live_date
 
print_msg "0000 Beginning processing for SVS Card Activity (Approved Transactions) Load"

print_msg "0000 Job Run ID is $JOB_RUN_ID and current predecessor is $current_pred"

export card_activity_stg_update_sql1="UPDATE $NZ_STG_DB.$NZ_STG_SCHEMA.$cdm_card_activity_stg_target 
                                      SET pos_requested_amount = -pos_requested_amount ,
                                          pos_approved_amount = -pos_approved_amount ,
                                          base_requested_amount = -base_requested_amount,
                                          base_approved_amount = -base_approved_amount
                                      WHERE credit_debit_indicator = 'D'
                                      AND eligible_existing_row_ind = 0"                                           

echo ''
echo "The SQL is card_activity_stg_update_sql1"
echo $card_activity_stg_update_sql1
echo ''

print_msg "0001 Updating Table $NZ_STG_DB.$NZ_STG_SCHEMA.$cdm_card_activity_stg_target for setting debit amounts to negative amounts"
run_query -d $NZ_STG_DB -q "$card_activity_stg_update_sql1" -m "0002 CARD ACTIVITY Load Processing failed when attempting to update the table $NZ_STG_DB.$NZ_STG_SCHEMA.$cdm_card_activity_stg_target with negative amounts"
   if [[ $? != 0 ]]
   then
      perl endJobRun.pl "$JOB_RUN_ID" "FAILURE"
      chk_err -r $return_code_fail -m "0003 =<== ERROR <=== update of Table $NZ_STG_DB.$NZ_STG_SCHEMA.$cdm_card_activity_stg_target failed when attempting to update the table with negative amounts.  $SCRIPT finished UNSUCCESSFULLY."
      exit $return_code_fail
   fi


#########################################################################################################################################################
#                                                                                                                                                       # 
#  TRANSACTION_INVOICE_NUMBER (aka ORDER NUMBER Plus) Updates                                                                                           #
#                                                                                                                                                       #
#########################################################################################################################################################


#############################################################################################################
# SVS REI (Merchant #67314) - Non Retail TRANSACTION_INVOICE_NUMBER for Sterling Order Number format change #        
#############################################################################################################

#export stg_update_invoice_sql3="UPDATE $NZ_STG_DB.$NZ_STG_SCHEMA.$cdm_card_activity_stg_target o
#                                --SET o.transaction_invoice_number = '1'||LPAD(TRIM(o.transaction_invoice_number),8,'0'),
#                                  SET o.transaction_invoice_number = LPAD(TRIM(o.transaction_invoice_number),8,'0'),
#                                    o.processing_criteria_desc = 'SVS REI (Merchant #67314) - Non Retail TRANSACTION_INVOICE_NUMBER for Sterling Order Number format change - LOAD2 sql3 <== '||o.processing_criteria_desc,
#	                            o.m_last_update_datetime = '$processing_datetime'
#                                WHERE SUBSTR(o.transaction_invoice_number,1,1) <> 'Y' 
#                                AND REGEXP_LIKE(TRIM(o.transaction_invoice_number), '^[0-9]+$')
#                                AND LENGTH(TRIM(o.transaction_invoice_number)) = 8
#                                --AND CAST(TRIM(o.transaction_invoice_number) AS INTEGER) < 3000000 
#                               AND o.merchant_number = 67314
#                               AND o.location_id = '0000000008'
#                                AND o.eligible_existing_row_ind = 0 -- new rows only"


#echo ''
#echo "The SQL is stg_update_invoice_sql3"
#echo $stg_update_invoice_sql3
#echo ''


#print_msg "0007 Updating Table $NZ_STG_DB.$NZ_STG_SCHEMA.$cdm_card_activity_stg_target for transaction invoice numbers for Non-Retail SVS REI. Sterling numbers are converted to look like non-sterling numbers "
#run_query -d $NZ_STG_DB -q "$stg_update_invoice_sql3" -m "0008 CARD ACTIVITY Load Processing failed when attempting to update the table $NZ_STG_DB.$NZ_STG_SCHEMA.$cdm_card_activity_stg_target for TRANSACTION_INVOICE_NUMBER for SVS REI Non-Retail"
#   if [[ $? != 0 ]]
#   then
#      perl endJobRun.pl "$JOB_RUN_ID" "FAILURE"
#      chk_err -r $return_code_fail -m "0009 =<== ERROR <=== update of Table $NZ_STG_DB.$NZ_STG_SCHEMA.$cdm_card_activity_stg_target column TRANSACTION_INVOICE_NUMBER failed for SVS REI Non-Retail.  $SCRIPT finished UNSUCCESSFULLY."
#      exit $return_code_fail
#  fi


#  NOTES! 1) An update statement for order_header_key should be moved from the LOAD2 script to this script (LOAD1).  The order_header_key should be set to -2 quite early (here) thus eliminating the need to check for
#            "NULL or -2" order_header_key in all of the subsequent code of LOAD2 and LOAD3.
#
#         2) The RDW Parse script gc_rdw_giftcard_card_activity_parse_level_1.ksh should be altered to trim all character columns.
#            All history will need to be updated at the same time to allow the comparisons to work when detecting change

#########################################################################################################################################################
#                                                                                                                                                       #
#  ORDER_HEADER_KEY Updates                                                                                                                                    #
#                                                                                                                                                       #
#########################################################################################################################################################


##########################################################
# AIM DATA (Merchant #70000) -  Non Retail ORDER_HEADER_KEY     #
##########################################################

export card_activity_stg_update_sql2="UPDATE $NZ_STG_DB.$NZ_STG_SCHEMA.$cdm_card_activity_stg_target o
                                      SET o.order_header_key = temp.order_header_key,
                                          o.eff_start_datetime =  DECODE(o.eligible_existing_row_ind,-2,'$processing_datetime',o.eff_start_datetime),
                                          o.eligible_existing_row_ind = DECODE(o.eligible_existing_row_ind,-2,1,o.eligible_existing_row_ind),
                                          o.processing_criteria_desc = 'AIM DATA (Merchant #70000) - Non Retail ORDER_HEADER_KEY - LOAD2 sql2 <== '||o.processing_criteria_desc,
                                          o.m_last_update_datetime = '$processing_datetime'
                                      FROM (SELECT MIN(oh.order_header_key) AS order_header_key, 
                                                   gca.card_number, gca.transaction_sequence_number, gca.pos_date, gca.pos_time
                                            FROM $NZ_RDW_DB.$NZ_RDW_SCHEMA.giftcard_card_activity gca
                                            INNER JOIN (SELECT STRRIGHT('0000' || vendor_order_id, 8) AS aim_order_id, 
                                                               client_order_id 
                                                        FROM $NZ_RDW_DB.$NZ_RDW_SCHEMA.giftcard_order_recon gor 
                                                        JOIN $NZ_METAMART_DB.$NZ_METAMART_SCHEMA.JOB_RUN j ON gor.job_run_id = j.job_run_id AND j.processing_status = '$processing_status_success'
                                                        ) gor ON TRIM(gca.transaction_invoice_number) = gor.aim_order_id
                                            INNER JOIN $NZ_PRSNT_DB.$NZ_PRSNT_SCHEMA.order_header oh ON
                                                        CASE
                                                           WHEN gor.client_order_id NOT LIKE '%-%'
                                                             -- THEN SUBSTR(gor.client_order_id,1,10)
                                                            THEN trim(leading '0' from (SUBSTR(gor.client_order_id,1,10)))
                                                          -- ELSE GET_VALUE_VARCHAR(ARRAY_SPLIT(gor.client_order_id,'-'),1)
                                                         ELSE trim(leading '0' from GET_VALUE_VARCHAR(ARRAY_SPLIT(gor.client_order_id,'-'),1)) 
                                                        END = TRIM(oh.order_id)
                                            INNER JOIN $NZ_PRSNT_DB.$NZ_PRSNT_SCHEMA.order_entry_application oea  ON oea.order_entry_application_key = oh.order_entry_application_key
                                            JOIN $NZ_METAMART_DB.$NZ_METAMART_SCHEMA.JOB_RUN j ON gca.job_run_id = j.job_run_id AND j.processing_status = '$processing_status_success'
                                      WHERE gca.merchant_number = 70000
                                      AND (oea.order_entry_application_id NOT IN('POS','ERADMIN') OR (oea.order_entry_application_id ='MSA' AND oh.pos_register_id IS NULL))
                                      -- AND REGEXP_LIKE(SUBSTR(TRIM(gca.transaction_invoice_number),2,9), '^[0-9]+$')
                                      AND EXISTS (SELECT 1 
                                                 FROM $NZ_PRSNT_DB.$NZ_PRSNT_SCHEMA.order_line ol 
                                                 WHERE ol.order_header_key = oh.order_header_key) -- A record exists in order line for this order
                                      GROUP BY gca.card_number, gca.transaction_sequence_number, gca.pos_date, gca.pos_time ) temp
                                      WHERE o.card_number = temp.card_number AND
                                            o.transaction_sequence_number = temp.transaction_sequence_number AND
                                            o.pos_transaction_datetime = TO_TIMESTAMP(temp.pos_date||STRRIGHT('000000' ||temp.pos_time,6),'YYYYMMDDHHMISS') AND
                                            temp.order_header_key <> -2 AND temp.order_header_key IS NOT NULL AND
                                            ((o.transaction_code = 98 AND (o.order_header_key = -2 OR o.order_header_key IS NULL)) OR o.transaction_code <> 98) AND
                                            (o.order_header_key IS NULL OR o.order_header_key = -2 OR o.order_header_key > temp.order_header_key) AND
                                            o.eligible_existing_row_ind <> 1"

echo ''
echo "The SQL is card_activity_stg_update_sql2"
echo $card_activity_stg_update_sql2
echo ''


print_msg "0010 Updating Table $NZ_STG_DB.$NZ_STG_SCHEMA.$cdm_card_activity_stg_target for updsting the ORDER_HEADER_KEY for AIM Non-Retail"
run_query -d $NZ_STG_DB -q "$card_activity_stg_update_sql2" -m "0011 CARD ACTIVITY Load Processing failed when attempting to update the table $NZ_STG_DB.$NZ_STG_SCHEMA.$cdm_card_activity_stg_target for ORDER_HEADER_KEY for AIM Non-Retail"
   if [[ $? != 0 ]]
   then
      perl endJobRun.pl "$JOB_RUN_ID" "FAILURE"
      chk_err -r $return_code_fail -m "0012 =<== ERROR <=== update of Table $NZ_STG_DB.$NZ_STG_SCHEMA.$cdm_card_activity_stg_target column ORDER_HEADER_KEY failed for AIM Non-Retail.  $SCRIPT finished UNSUCCESSFULLY."
      exit $return_code_fail
   fi


########################################################
# SVS REI (Merchant #67314) - Non Retail ORDER_HEADER_KEY     #
########################################################


export card_activity_stg_update_sql3="UPDATE $NZ_STG_DB.$NZ_STG_SCHEMA.$cdm_card_activity_stg_target o 
                                      SET o.order_header_key = temp.order_header_key,
                                          o.eff_start_datetime =  DECODE(o.eligible_existing_row_ind,-2,'$processing_datetime',o.eff_start_datetime),
                                          o.eligible_existing_row_ind = DECODE(o.eligible_existing_row_ind,-2,1,o.eligible_existing_row_ind),
                                          o.processing_criteria_desc = 'SVS REI (Merchant #67314) - Non Retail - LOAD2 sql3 <== '||o.processing_criteria_desc,
                                          o.m_last_update_datetime = '$processing_datetime'
                                      FROM (SELECT MIN(oh.order_header_key) AS order_header_key, 
                                                   gca.card_number, gca.transaction_sequence_number, gca.pos_date, gca.pos_time
                                           FROM $NZ_RDW_DB.$NZ_RDW_SCHEMA.giftcard_card_activity gca
                                           JOIN $NZ_METAMART_DB.$NZ_METAMART_SCHEMA.JOB_RUN j ON gca.job_run_id = j.job_run_id AND j.processing_status = '$processing_status_success'
                                           INNER JOIN $NZ_PRSNT_DB.$NZ_PRSNT_SCHEMA.order_header oh ON
                                            -- CASE WHEN SUBSTR(gca.transaction_invoice_number,1,1) = 'Y'
                                                          -- THEN SUBSTR(gca.transaction_invoice_number,1,10)
                                                       --WHEN LENGTH(TRIM(gca.transaction_invoice_number)) = 8 AND 
                                                           -- LPAD(TRIM(gca.transaction_invoice_number),10,'0') < '0003000000' AND 
                                                           -- SUBSTR(gca.transaction_invoice_number,1,1) <> 'Y'
                                                          --THEN '1'||gca.transaction_invoice_number
                                                       --ELSE TRIM(gca.transaction_invoice_number) END = oh.order_id
						            TRIM(gca.transaction_invoice_number)= oh.order_id
                                           INNER JOIN $NZ_PRSNT_DB.$NZ_PRSNT_SCHEMA.order_entry_application oea  ON oea.order_entry_application_key = oh.order_entry_application_key
                                           WHERE gca.merchant_number = 67314
                                           AND (oea.order_entry_application_id NOT IN('POS','ERADMIN') OR (oea.order_entry_application_id ='MSA' AND oh.pos_register_id IS NULL))  -- Non Retail means same thing as location_id  = '0000000010' and '0000000008'
                                           AND gca.location_id IN ('0000000008',  '0000000010')
                                           --AND REGEXP_LIKE(SUBSTR(TRIM(gca.transaction_invoice_number),2,9), '^[0-9]+$')
                                           AND EXISTS (SELECT 1 
                                                       FROM $NZ_PRSNT_DB.$NZ_PRSNT_SCHEMA.order_line ol
                                                       WHERE ol.order_header_key = oh.order_header_key  ) -- A record exists in order line for this order
                                           GROUP BY gca.card_number, gca.transaction_sequence_number, gca.pos_date, gca.pos_time ) temp
                                      WHERE o.card_number = temp.card_number AND
                                            o.transaction_sequence_number = temp.transaction_sequence_number AND
                                            o.pos_transaction_datetime = TO_TIMESTAMP(temp.pos_date||STRRIGHT('000000' ||temp.pos_time,6),'YYYYMMDDHHMISS') AND
                                            temp.order_header_key <> -2 AND temp.order_header_key IS NOT NULL AND
                                            ((o.transaction_code = 98 AND (o.order_header_key = -2 OR o.order_header_key IS NULL)) OR o.transaction_code <> 98) AND
	                                    (o.order_header_key IS NULL OR o.order_header_key = -2 OR o.order_header_key > temp.order_header_key) AND
                                            o.eligible_existing_row_ind <> 1"

echo
echo "The SQL is card_activity_stg_update_sql3"
echo $card_activity_stg_update_sql3
echo ''
 
print_msg "0020 Updating Table $NZ_STG_DB.$NZ_STG_SCHEMA.$cdm_card_activity_stg_target for Non-Retail ORDER_HEADER_KEY"
run_query -d $NZ_STG_DB -q "$card_activity_stg_update_sql3" -m "0021 CARD ACTIVITY Load Processing failed when attempting to update the table $NZ_STG_DB.$NZ_STG_SCHEMA.$cdm_card_activity_stg_target for ORDER_HEADER_KEY columns for Non-Retail"
   if [[ $? != 0 ]]
   then
      perl endJobRun.pl "$JOB_RUN_ID" "FAILURE"
      chk_err -r $return_code_fail -m "0022 =<== ERROR <=== Update of Table $NZ_STG_DB.$NZ_STG_SCHEMA.$cdm_card_activity_stg_target failed for column ORDER_HEADER_KEY for Non-Retail.  $SCRIPT finished UNSUCCESSFULLY."
      exit $return_code_fail
   fi


####################################################################################################################
# SVS REI (Merchant #67314) Internet Orders - Non-Retail (Keeps Filters due to multiple matches) ORDER_HEADER_KEY         #
####################################################################################################################

#
# Internet Orders Non-Retail Gift Card-related Tender categories
#

#export card_activity_stg_update_sql4="UPDATE $NZ_STG_DB.$NZ_STG_SCHEMA.$cdm_card_activity_stg_target o 
#                                     SET o.order_header_key = temp.order_header_key,
#                                         o.eff_start_datetime =  DECODE(o.eligible_existing_row_ind,-2,'$processing_datetime',o.eff_start_datetime),
#                                         o.eligible_existing_row_ind = DECODE(o.eligible_existing_row_ind,-2,1,o.eligible_existing_row_ind),
#                                         o.processing_criteria_desc = 'SVS REI (Merchant #67314) Internet Orders - Non-Retail ORDER_HEADER_KEY - LOAD2 sql4 <== '||o.processing_criteria_desc,
#                                         o.m_last_update_datetime = '$processing_datetime'
#                                     FROM (SELECT MIN(oh.order_header_key) AS order_header_key, 
#                                                   gca.card_number, gca.transaction_sequence_number, gca.pos_date, gca.pos_time
#                                           FROM $NZ_RDW_DB.$NZ_RDW_SCHEMA.giftcard_card_activity gca
#                                           JOIN $NZ_METAMART_DB.$NZ_METAMART_SCHEMA.JOB_RUN j ON gca.job_run_id = j.job_run_id AND j.processing_status = '$processing_status_success'
#                                           INNER JOIN $NZ_PRSNT_DB.$NZ_PRSNT_SCHEMA.order_header oh ON
#                                           --TRIM(gca.transaction_invoice_number) = TRIM(CAST(oh.internet_order_nbr AS VARCHAR(10))) 
#				               TRIM(gca.transaction_invoice_number) = TRIM(CAST(oh.order_id AS VARCHAR(10))) 
#                                           INNER JOIN $NZ_PRSNT_DB.$NZ_PRSNT_SCHEMA.order_tender ot ON ot.order_header_key = oh.order_header_key  -- Gift Card transactions Only for Redemptions
#                                           INNER JOIN $NZ_PRSNT_DB.$NZ_PRSNT_SCHEMA.tender_type_dim ttd ON 
#                                              ot.tender_type_key = ttd.tender_type_key AND
#                                              ttd.tender_category_desc IN ('Gift Certificate','Merchant Credit','Refund Voucher')
#                                           INNER JOIN $NZ_PRSNT_DB.$NZ_PRSNT_SCHEMA.order_entry_application oea  ON oea.order_entry_application_key = oh.order_entry_application_key
#                                           WHERE gca.merchant_number = 67314
#                                           AND (oea.order_entry_application_id NOT IN('POS','ERADMIN') OR (oea.order_entry_application_id ='MSA' AND oh.pos_register_id IS NULL))
#                                           AND gca.location_id IN ('0000000008',  '0000000010')
#                                           --AND REGEXP_LIKE(SUBSTR(TRIM(gca.transaction_invoice_number),2,9), '^[0-9]+$')
#                                           AND EXISTS (SELECT 1
#                                                       FROM $NZ_PRSNT_DB.$NZ_PRSNT_SCHEMA.order_line ol
#                                                       WHERE ol.order_header_key = oh.order_header_key) -- A record exists in order line for this order
#                                           GROUP BY gca.card_number, gca.transaction_sequence_number, gca.pos_date, gca.pos_time ) temp
#                                      WHERE o.card_number = temp.card_number AND
#                                            o.transaction_sequence_number = temp.transaction_sequence_number AND
#                                            o.pos_transaction_datetime = TO_TIMESTAMP(temp.pos_date||STRRIGHT('000000' ||temp.pos_time,6),'YYYYMMDDHHMISS') AND
#                                            temp.order_header_key <> -2 AND temp.order_header_key IS NOT NULL AND
#                                            ((o.transaction_code = 98 AND (o.order_header_key = -2 OR o.order_header_key IS NULL)) OR o.transaction_code <> 98) AND           
#                                            (o.order_header_key IS NULL OR o.order_header_key = -2 OR o.order_header_key > temp.order_header_key) AND
#                                            o.eligible_existing_row_ind <> 1"

#echo ''
#echo "The SQL is card_activity_stg_update_sql4"
#echo $card_activity_stg_update_sql4
#echo ''

   
#   print_msg "0030 Updating Table $NZ_STG_DB.$NZ_STG_SCHEMA.$cdm_card_activity_stg_target for Internet Orders Non-Retail Gift Card-related Tender categories"
#   run_query -d $NZ_STG_DB -q "$card_activity_stg_update_sql4" -m "0031 CARD ACTIVITY Load Processing failed when attempting to update the table $NZ_STG_DB.$NZ_STG_SCHEMA.$cdm_card_activity_stg_target for column ORDER_HEADER_KEY for Non-Retail Internet Orders for GC-related tender categories"
#   if [[ $? != 0 ]]
#   then
#      perl endJobRun.pl "$JOB_RUN_ID" "FAILURE"
#      chk_err -r $return_code_fail -m "0032 =<== ERROR <=== update of Table $NZ_STG_DB.$NZ_STG_SCHEMA.$cdm_card_activity_stg_target failed for ORDER_HEADER_KEY for Non-Retail Internet Orders for GC-related tendor categories.  $SCRIPT finished UNSUCCESSFULLY."
#      exit $return_code_fail
#   fi

#
# Internet Orders Non-Retail Activations per GC Articles
#
 
#export card_activity_stg_update_sql5="UPDATE $NZ_STG_DB.$NZ_STG_SCHEMA.$cdm_card_activity_stg_target o 
#                                     SET o.order_header_key = temp.order_header_key,
#                                         o.eff_start_datetime =  DECODE(o.eligible_existing_row_ind,-2,'$processing_datetime',o.eff_start_datetime),
#                                         o.eligible_existing_row_ind = DECODE(o.eligible_existing_row_ind,-2,1,o.eligible_existing_row_ind),
#                                         o.processing_criteria_desc = 'SVS REI (Merchant #67314) Internet Orders - Activations per GC Articles ORDER_HEADER_KEY - LOAD2 sql5 <== '||o.processing_criteria_desc,
#                                         o.m_last_update_datetime = '$processing_datetime'
#                                     FROM (SELECT MIN(oh.order_header_key) AS order_header_key, 
#                                                  gca.card_number, gca.transaction_sequence_number, gca.pos_date, gca.pos_time
#                                          FROM $NZ_RDW_DB.$NZ_RDW_SCHEMA.giftcard_card_activity gca
#                                          JOIN $NZ_METAMART_DB.$NZ_METAMART_SCHEMA.JOB_RUN j ON gca.job_run_id = j.job_run_id AND j.processing_status = '$processing_status_success'
#                                          INNER JOIN $NZ_PRSNT_DB.$NZ_PRSNT_SCHEMA.order_header oh ON
#                                           --TRIM(gca.transaction_invoice_number) = TRIM(CAST(oh.internet_order_nbr AS VARCHAR(10))) 
#						  TRIM(gca.transaction_invoice_number) = TRIM(CAST(oh.order_id AS VARCHAR(10))) 
#                                          INNER JOIN $NZ_PRSNT_DB.$NZ_PRSNT_SCHEMA.order_line ol ON ol.order_header_key = oh.order_header_key
#                                          JOIN $NZ_PRSNT_DB.$NZ_PRSNT_SCHEMA.srv_article_dim a ON ol.sad_key = a.sad_key AND a.srv_article IN (9000000006, 9990490006, 9000000208) -- GC txns only
#                                           INNER JOIN $NZ_PRSNT_DB.$NZ_PRSNT_SCHEMA.order_entry_application oea  ON oea.order_entry_application_key = oh.order_entry_application_key
#                                           WHERE gca.merchant_number = 67314
#                                           AND (oea.order_entry_application_id NOT IN('POS','ERADMIN') OR (oea.order_entry_application_id ='MSA' AND oh.pos_register_id IS NULL))
#                                          AND gca.location_id IN ('0000000008',  '0000000010')
#                                         --AND REGEXP_LIKE(SUBSTR(TRIM(gca.transaction_invoice_number),2,9), '^[0-9]+$')
#                                           AND EXISTS (SELECT 1
#                                                       FROM $NZ_PRSNT_DB.$NZ_PRSNT_SCHEMA.order_line ol
#                                                       WHERE ol.order_header_key = oh.order_header_key)  -- A record exists in order line for this order 
#                                          GROUP BY gca.card_number, gca.transaction_sequence_number, gca.pos_date, gca.pos_time ) temp
#                                     WHERE o.card_number = temp.card_number AND
#                                           o.transaction_sequence_number = temp.transaction_sequence_number AND
#                                           o.pos_transaction_datetime = TO_TIMESTAMP(temp.pos_date||STRRIGHT('000000' ||temp.pos_time,6),'YYYYMMDDHHMISS') AND
#                                           temp.order_header_key <> -2 AND temp.order_header_key IS NOT NULL AND
#                                           ((o.transaction_code = 98 AND (o.order_header_key = -2 OR o.order_header_key IS NULL)) OR o.transaction_code <> 98) AND
#                                           (o.order_header_key IS NULL OR o.order_header_key = -2 OR o.order_header_key > temp.order_header_key) AND
#                                           o.eligible_existing_row_ind <> 1"

#echo ''
#echo "The SQL is card_activity_stg_update_sql5"
#echo $card_activity_stg_update_sql5
#echo ''


# print_msg "0040 Updating Table $NZ_STG_DB.$NZ_STG_SCHEMA.$cdm_card_activity_stg_target for Internet Orders Non-Retail Activations per GC Articles"
# run_query -d $NZ_STG_DB -q "$card_activity_stg_update_sql5" -m "0041 CARD ACTIVITY Load Processing failed when attempting to update the table $NZ_STG_DB.$NZ_STG_SCHEMA.$cdm_card_activity_stg_target for Internet Orders Non-Retail Activations per GC Articles"
#  if [[ $? != 0 ]]
#  then
#     perl endJobRun.pl "$JOB_RUN_ID" "FAILURE"
#     chk_err -r $return_code_fail -m "0042 =<== ERROR <=== update of Table $NZ_STG_DB.$NZ_STG_SCHEMA.$cdm_card_activity_stg_target failed for Internet Orders Non-Retail Activations per GC Articles.  $SCRIPT finished UNSUCCESSFULLY."
#     exit $return_code_fail
#  fi


################################################################
# RETAIL TRANSACTIONS                                          #
################################################################

export card_activity_stg_update_sql6="UPDATE $NZ_STG_DB.$NZ_STG_SCHEMA.$cdm_card_activity_stg_target o
                                      SET o.order_header_key = temp.order_header_key,
                                         o.transaction_invoice_number = DECODE(o.eligible_existing_row_ind,0,
                                                                                temp.transaction_invoice_number_constructed,
                                                                                o.transaction_invoice_number),
                                          o.eff_start_datetime =  DECODE(o.eligible_existing_row_ind,-2,'$processing_datetime',o.eff_start_datetime),
                                          o.eligible_existing_row_ind = DECODE(o.eligible_existing_row_ind,-2,1,o.eligible_existing_row_ind),
                                          o.processing_criteria_desc = 'RETAIL TRANSACTIONS - ORDER_HEADER_KEY LOAD2 sql6 <== '||o.processing_criteria_desc,
                                          o.m_last_update_datetime = '$processing_datetime'
                                      FROM (SELECT MIN(oh.order_header_key) AS order_header_key,
                                                  -- MIN(TRIM(CAST(oh.pos_register_id||oh.pos_trans_nbr AS VARCHAR(10)))) AS transaction_invoice_number_constructed,
                                                     MIN(TRIM(CAST(lpad(oh.pos_register_id,3,'0')||lpad(oh.pos_trans_nbr,4,'0')AS VARCHAR(10)))) AS transaction_invoice_number_constructed,
                                                   gca.card_number, gca.transaction_sequence_number, gca.pos_date, gca.pos_time
                                            FROM (SELECT*
                                                  FROM $NZ_RDW_DB.$NZ_RDW_SCHEMA.giftcard_card_activity gca
                                                  JOIN $NZ_METAMART_DB.$NZ_METAMART_SCHEMA.JOB_RUN j ON gca.job_run_id = j.job_run_id AND j.processing_status = '$processing_status_success'
                                                  WHERE gca.merchant_number = 67314
                                                  AND REGEXP_LIKE(TRIM(gca.transaction_invoice_number), '^\d{7}$')) gca -- Retail Transactions should have Transaction Invoice Number made of 7 digits
                                                  INNER JOIN $NZ_PRSNT_DB.$NZ_PRSNT_SCHEMA.store_dim s ON CAST(gca.location_id AS BIGINT) = s.store_id 
                                                  INNER JOIN $NZ_PRSNT_DB.$NZ_PRSNT_SCHEMA.order_header oh ON 
                                                     CAST(STRLEFT(TRIM(gca.transaction_invoice_number),3) AS INTEGER) = oh.pos_register_id AND
                                                     CAST(STRRIGHT(TRIM(gca.transaction_invoice_number),4) AS INTEGER) = oh.pos_trans_nbr AND
                                                     oh.order_date_key = gca.pos_date AND 
                                                     s.store_key = oh.origination_location_key 
                                                  INNER JOIN $NZ_PRSNT_DB.$NZ_PRSNT_SCHEMA.order_entry_application oea  
								ON oea.order_entry_application_key = oh.order_entry_application_key AND
                                                    (oea.order_entry_application_id IN('POS','ERADMIN') OR (oea.order_entry_application_id = 'MSA' AND oh.pos_register_id IS NOT NULL))                                                  INNER JOIN $NZ_RDW_DB.$NZ_RDW_SCHEMA.giftcard_transaction_code gstc ON 
                                                     gstc.transaction_code = gca.transaction_code AND
                                                     gstc.transaction_source <> 'IVR' AND
                                                     gstc.transaction_type <> 'Batch' AND
                                                     EXISTS (SELECT 1
                                                       FROM $NZ_PRSNT_DB.$NZ_PRSNT_SCHEMA.order_line ol
                                                       WHERE ol.order_header_key = oh.order_header_key) -- A record exists in order line for this order 
                                           GROUP BY gca.card_number, gca.transaction_sequence_number, gca.pos_date, gca.pos_time ) temp
                                      WHERE o.card_number = temp.card_number AND
                                            o.transaction_sequence_number = temp.transaction_sequence_number AND
                                            o.pos_transaction_datetime = TO_TIMESTAMP(temp.pos_date||STRRIGHT('000000' ||temp.pos_time,6),'YYYYMMDDHHMISS') AND
                                            temp.order_header_key <> -2 AND temp.order_header_key IS NOT NULL AND
                                            ((o.transaction_code = 98 AND (o.order_header_key = -2 OR o.order_header_key IS NULL)) OR o.transaction_code <> 98) AND
                                            (o.order_header_key IS NULL OR o.order_header_key = -2 OR o.order_header_key > temp.order_header_key) AND
                                            o.eligible_existing_row_ind <> 1"

echo ''
echo "The SQL is card_activity_stg_update_sql6"
echo $card_activity_stg_update_sql6
echo ''
 
   print_msg "0050 Updating Table $NZ_STG_DB.$NZ_STG_SCHEMA.$cdm_card_activity_stg_target column ORDER_HEADER_KEY for Retail Transactions"
   run_query -d $NZ_STG_DB -q "$card_activity_stg_update_sql6" -m "0051 CARD ACTIVITY Load Processing failed when attempting to update the table $NZ_STG_DB.$NZ_STG_SCHEMA.$cdm_card_activity_stg_target column ORDER_HEADER_KEY for Retail Transactions"
   if [[ $? != 0 ]]
   then
      perl endJobRun.pl "$JOB_RUN_ID" "FAILURE"
      chk_err -r $return_code_fail -m "0052 =<== ERROR <=== update of Table $NZ_STG_DB.$NZ_STG_SCHEMA.$cdm_card_activity_stg_target failed for column ORDER_HEADER_KEY for Retail Transactions.  $SCRIPT finished UNSUCCESSFULLY."
      exit $return_code_fail
   fi

################################################################
# Profile Activations (Transaction Code 98)                    #
################################################################

export card_activity_stg_update_sql7="UPDATE $NZ_STG_DB.$NZ_STG_SCHEMA.$cdm_card_activity_stg_target o
                                      SET o.order_header_key = temp.order_header_key,
                                          o.eff_start_datetime = DECODE(o.eligible_existing_row_ind,-2,'$processing_datetime',o.eff_start_datetime),
                                          o.eligible_existing_row_ind = DECODE(o.eligible_existing_row_ind,-2,1,o.eligible_existing_row_ind),
                                          o.processing_criteria_desc = 'Profile Activations (Transaction Code 98) ORDER_HEADER_KEY.  LOAD2 sql7 <== '||o.processing_criteria_desc,
                                          o.m_last_update_datetime = '$processing_datetime'
                                      FROM  (SELECT MIN(gca_enable.order_header_key) AS order_header_key, gca_activation.card_number, gca_activation.transaction_sequence_number, gca_activation.pos_transaction_datetime
                                             FROM $NZ_STG_DB.$NZ_STG_SCHEMA.$cdm_card_activity_stg_target gca_enable
                                             JOIN $NZ_METAMART_DB.$NZ_METAMART_SCHEMA.JOB_RUN j ON gca_enable.job_run_id = j.job_run_id AND j.processing_status = '$processing_status_success'
                                             INNER JOIN $NZ_STG_DB.$NZ_STG_SCHEMA.$cdm_card_activity_stg_target gca_activation
                                                 ON   gca_enable.card_number = gca_activation.card_number
                                                 AND  gca_enable.transaction_code = 60
                                                 AND  gca_enable.order_header_key <> -2 AND gca_enable.order_header_key IS NOT NULL
                                                 AND  gca_activation.transaction_code = 98
                                                 AND  (gca_activation.order_header_key = -2 OR gca_activation.order_header_key IS NULL OR gca_activation.order_header_key <> gca_enable.order_header_key)
                                             JOIN $NZ_METAMART_DB.$NZ_METAMART_SCHEMA.JOB_RUN ja ON gca_activation.job_run_id = ja.job_run_id AND ja.processing_status = '$processing_status_success'
                                             GROUP BY gca_activation.card_number, gca_activation.transaction_sequence_number, gca_activation.pos_transaction_datetime) temp
                                      WHERE o.card_number = temp.card_number AND
                                            o.transaction_sequence_number = temp.transaction_sequence_number AND
                                            o.pos_transaction_datetime = temp.pos_transaction_datetime AND
                                            temp.order_header_key <> -2 AND temp.order_header_key IS NOT NULL AND
                                            o.transaction_code = 98 AND
                                            (o.order_header_key IS NULL OR o.order_header_key = -2 OR o.order_header_key <> temp.order_header_key)"

echo ''
echo "The SQL is card_activity_stg_update_sql7"
echo $card_activity_stg_update_sql7
echo ''

   print_msg "0060 Updating Table $NZ_STG_DB.$NZ_STG_SCHEMA.$cdm_card_activity_stg_target column ORDER_HEADER_KEY for Profile Activations (Transaction Code 98)"
   run_query -d $NZ_STG_DB -q "$card_activity_stg_update_sql7" -m "0061 CARD ACTIVITY Load Processing failed when attempting to update the table $NZ_STG_DB.$NZ_STG_SCHEMA.$cdm_card_activity_stg_target column ORDER_HEADER_KEY for Profile Activations (Transaction Code 98)"
   if [[ $? != 0 ]]
   then
      perl endJobRun.pl "$JOB_RUN_ID" "FAILURE"
      chk_err -r $return_code_fail -m "0062 =<== ERROR <=== update of Table $NZ_STG_DB.$NZ_STG_SCHEMA.$cdm_card_activity_stg_target failed for column ORDER_HEADER_KEY Profile Activations (Transaction Code 98).  $SCRIPT finished UNSUCCESSFULLY."
      exit $return_code_fail
   fi

#  NOTE!  This update statement should be moved much earlier, into LOAD1, against the work table even.  Then all the subsequent "NULL or -2" order_header_key testing can be eliminated.  
#         This change will be beneficial for performance, ease of code maintence, clarity, etc.  

export card_activity_stg_update_sql8="UPDATE $NZ_STG_DB.$NZ_STG_SCHEMA.$cdm_card_activity_stg_target o 
                                      SET o.order_header_key = -2,
                                          o.m_last_update_datetime = '$processing_datetime'
                                      WHERE o.order_header_key IS NULL"

echo ''
echo "The SQL is card_activity_stg_update_sql8"
echo $card_activity_stg_update_sql
echo ''

   print_msg "0070 Updating Table $NZ_STG_DB.$NZ_STG_SCHEMA.$cdm_card_activity_stg_target setting all remaining NULL ORDER_HEADER_KEY to the value -2"
   run_query -d $NZ_STG_DB -q "$card_activity_stg_update_sql8" -m "0071 CARD ACTIVITY Load Processing failed when attempting to update the table $NZ_STG_DB.$NZ_STG_SCHEMA.$cdm_card_activity_stg_target for setting all remaining NULL ORDER_HEADER_KEY to the value -2"
   if [[ $? != 0 ]]
   then
      perl endJobRun.pl "$JOB_RUN_ID" "FAILURE"
      chk_err -r $return_code_fail -m "0072 =<== ERROR <=== update of Table $NZ_STG_DB.$NZ_STG_SCHEMA.$cdm_card_activity_stg_target failed for setting all remaining NULL ORDER_HEADER_KEY to the value -2.  $SCRIPT finished UNSUCCESSFULLY."
      exit $return_code_fail
   fi

print_msg "0100 Processing complete for Updating STG Card Activity table $NZ_STG_DB.$NZ_STG_SCHEMA.$cdm_card_activity_stg_target"
echo ""

print_msg "9999 Processing complete for $SCRIPT_NAME with return code = $return_code_succeed"

