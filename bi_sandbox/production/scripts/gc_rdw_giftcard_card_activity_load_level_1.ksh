#!/bin/ksh

########################################################
#
# Recreational Equipment Incorporated
#
# COPYRIGHT (c) 2000-2012 by Recreational Equipment Incorporated
#
# This software is furnished under a  license and may be used and copied  only
# in accordance with the terms of  such license and with the inclusion of  the
# above copyright notice.  This  software or any other copies thereof  may not
# be provided or  otherwise made available to  any other person.   No title to
# and ownership of the software is hereby transferred.
#
########################################################
#
# MODULE      : gc_rdw_giftcard_card_activity_load_level_1.ksh 
# DESCRIPTION : This script loads new data into the level 1 RDW GIFTCARD_CARD_ACTIVITY table.
#
#               This data is pulled by REI via file REIRQ132 and populated into table
#               HIST SRC_GC_ORDER_RECON.  This script is the 4th of four which will perform
#               change data capture, transformation, and load into RDW table GIFTCARD_CARD_ACTIVITY
#                   1) parsing
#                   2) change data capture
#                   3) transformation
#                   4) load into RDW table GIFTCARD_CARD_ACTIVITY
#
#
# ABSTRACT:
#
# NOTES: Here is the processing components and the order in which they are called:
#            Tidal Group
#               UNIX Script
#                  UNIX Script : Load_SRC_GC_ORDER_RECON.ksh
#                  UNIX Script : Load_RDW_GC_ORDER_RECON.ksh
#                  UNIX Script : gc_rdw_giftcard_card_activity.ksh
#                     UNIX Script:  gc_rdw_giftcard_card_activity_parse_level_1.ksh
#                     UNIX Script:  gc_rdw_giftcard_card_activity_cdc_level_1.ksh
#                     UNIX Script:  gc_rdw_giftcard_card_activity_transform_level_1.ksh
#                     UNIX Script:  gc_rdw_giftcard_card_activity_load_level_1.ksh
#
# SYNOPSIS:
#
########################################################
#
# MODIFICATION HISTORY:
#  Date       Name              Mods Description
#  ---------- ----------------- ---- -------------------------------------
#  10/21/2012 Anna Segal             Code creation
#  11/04/2012 Anna Segal             Changed from activation to host times for eff_start_datetime
#  04/01/2013 Izic Chon				 Added four new columns (original_transaction_sequence_number, original_pos_date, original_pos_time, card_working_balance)	
#
# MODIFICATION LEGEND:
#   B = Bugfixes
#   A = Architectural change
#   F = Feature addition
#   R = Code re-write
#   C = Comment update
#
########################################################

export SCRIPT_NAME=$(basename  $0)

. $SCRIPTS_DIR/load_parameters.ksh
. $SCRIPTS_DIR/load_library.ksh
. $SCRIPTS_DIR/load_datastage_params.ksh


print_msg "0000 Processing started for $SCRIPT_NAME"

##############################################
# CARD ACTIVITY                              #
##############################################

print_msg "0001 Beginning processing for SVS Card Activity (Approved Transactions) Load"

print_msg "0002 Job Run ID is $JOB_RUN_ID and current predecessor is $current_pred"

export card_activity_load_sql="INSERT INTO $rdw_card_activity_target
       (CARD_ACTIVITY_KEY
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
       , CREDIT_DEBIT_INDICATOR
       , TRANSACTION_SEQUENCE_NUMBER
       , TRANSACTION_INVOICE_NUMBER
       , HOST_DATE
       , HOST_TIME
       , POS_DATE
       , POS_TIME
       , POS_CURRENCY_CODE
       , POS_REQUESTED_AMOUNT
       , POS_APPROVED_AMOUNT
       , BASE_CURRENCY_CODE
       , BASE_REQUESTED_AMOUNT
       , BASE_APPROVED_AMOUNT
       , CURRENCY_CONVERSION_FACTOR
       , CURRENCY_BASE_UNITS
       , ACTIVATION_DATE
       , ACTIVATION_TIME
       , ACTIVATION_MERCHANT_NUMBER
       , ACTIVATION_DIVISION_NUMBER
       , ACTIVATION_LOCATION_ID
       , EFF_START_DATETIME
       , EFF_END_DATETIME
       , JOB_RUN_ID
       , M_INSERT_DATETIME
       , M_LAST_UPDATE_DATETIME
	, original_transaction_sequence_number
	, original_pos_date
	, original_pos_time
	, card_working_balance

)
                               SELECT card_activity_key
                                    , record_type
                                    , bin_range_tag
                                    , card_number
                                    , alt_card_number
                                    , merchant_number
                                    , division_number
                                    , location_id
                                    , state
                                    , country
                                    , transaction_code
                                    , credit_debit_indicator
                                    , transaction_sequence_number
                                    , transaction_invoice_number
                                    , host_date
                                    , host_time
                                    , pos_date
                                    , pos_time
                                    , pos_currency_code
                                    , pos_requested_amount
                                    , pos_approved_amount
                                    , base_currency_code
                                    , base_requested_amount
                                    , base_approved_amount
                                    , currency_conversion_factor
                                    , currency_base_units
                                    , activation_date
                                    , activation_time
                                    , activation_merchant_number
                                    , activation_division_number
                                    , activation_location_id
                                    , '$processing_datetime' AS eff_start_datetime
                                    , '31-DEC-9999'
                                    , $JOB_RUN_ID
                                    , '$processing_datetime' AS m_insert_datetime
                                    , '$processing_datetime' AS m_last_update_datetime
									, original_transaction_sequence_number
									, original_pos_date
									, original_pos_time
									, card_working_balance
                              FROM $NZ_STGRDW_DB.$NZ_STGRDW_SCHEMA.$wrk_card_activity_transform
                              ORDER BY card_activity_key,
                                       job_run_id,
                                       source_location,
                                       transmit_datetime,
                                       line_number"

print_msg "0005 Loading table $rdw_card_activity_target"
run_query -d $NZ_RDW_DB -q "$card_activity_load_sql" -m "0006 CARD ACTIVITY Load Processing failed when attempting to insert into table $rdw_card_activity_target from table $NZ_STGRDW_DB.$NZ_STGRDW_SCHEMA.$wrk_card_activity_transform"
   if [[ $? != 0 ]]
   then
      perl endJobRun.pl "$JOB_RUN_ID" "FAILURE"
      chk_err -r $return_code_fail -m "0007 ===> ERROR <=== INSERT into table $NZ_RDW_DB.$NZ_RDW_SCHEMA.$rdw_card_activity_target failed.  $SCRIPT finished UNSUCCESSFULLY."
      exit $return_code_fail
   fi


print_msg "0010 Processing complete for Card Activity Load into table $NZ_RDW_DB.$NZ_RDW_SCHEMA.$rdw_card_activity_target"
echo ""

print_msg "9999 Processing complete for $SCRIPT_NAME with return code = $return_code_succeed"

