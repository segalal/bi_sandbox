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
# MODULE      : gc_rdw_giftcard_card_activity_cdc_level_1.ksh 
# DESCRIPTION : This code identifies and captures all new data (cdc for the
#               level 1 (no dependencies) SVS non-AIM approved table:
#                 GIFTCARD_CARD_ACTIVITY 
#
#               This data is pulled by REI via file REIRQ132 and populated into table
#               HIST SRC_GC_ORDER_RECON.  This script is the 2nd of four which will perform
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
#                     UNIX Script:  gc_rdw_giftcard_card_activity_trans_level_1.ksh
#                     UNIX Script:  gc_rdw_giftcard_card_activity_transform_level_1.ksh
#                     UNIX Script:  gc_rdw_giftcard_card_activity_load_level_1.ksh
#
#
# SYNOPSIS:
#
########################################################
#
# MODIFICATION HISTORY:
#  Date       Name              Mods Description
#  ---------- ----------------- ---- -------------------------------------
#  10/21/2012 Anna Segal             Code creation
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

print_msg "0001 Beginning processing for SVS Approved Transactions CDC"

print_msg "0002 Job Run ID is $JOB_RUN_ID and current predecessor is $current_pred"


print_msg "0005 Dropping table $wrk_card_activity_cdc"
drop_table -d $NZ_STGRDW_DB -t $NZ_STGRDW_DB.$NZ_STGRDW_SCHEMA.$wrk_card_activity_cdc
   if [[ $? != 0 ]]
   then
      perl endJobRun.pl "$JOB_RUN_ID" "FAILURE"
      chk_err -r $return_code_fail -m "0006 ===> ERROR <=== Drop of table $NZ_STGRDW_DB.$NZ_STGRDW_SCHEMA.$wrk_card_activity_cdc failed.  $SCRIPT finished UNSUCCESSFULLY."
      exit $return_code_fail
   fi

print_msg "0007 Table $wrk_card_activity_cdc was successfully dropped or did not exist"


export card_activity_cdc_sql="CREATE TABLE $wrk_card_activity_cdc
                              AS
                              SELECT  p.record_type
                                    , p.bin_range_tag
                                    , p.card_number
                                    , p.alt_card_number
                                    , p.merchant_number
                                    , p.division_number
                                    , p.location_id
                                    , p.state
                                    , p.country
                                    , p.transaction_code
                                    , p.credit_debit_indicator
                                    , p.transaction_sequence_number
                                    , p.transaction_invoice_number
                                    , p.host_date
                                    , p.host_time
                                    , p.pos_date
                                    , p.pos_time
                                    , p.pos_currency_code
                                    , p.pos_requested_amount
                                    , p.pos_approved_amount
                                    , p.base_currency_code
                                    , p.base_requested_amount
                                    , p.base_approved_amount
                                    , p.currency_conversion_factor
                                    , p.currency_base_units
                                    , p.activation_date
                                    , p.activation_time
                                    , p.activation_merchant_number
                                    , p.activation_division_number
                                    , p.activation_location_id 
									, p.original_transaction_sequence_number
									, p.original_pos_date
									, p.original_pos_time
									, p.card_working_balance
				    , p.transmit_datetime
				    , p.source_location
				    , p.line_number
				    , p.job_run_id
                                    , '$processing_datetime' AS M_INSERT_DATETIME
                                    , '$processing_datetime' AS M_LAST_UPDATE_DATETIME
                             FROM $NZ_STGRDW_DB.$NZ_STGRDW_SCHEMA.$wrk_card_activity_parse p
  	                     WHERE ( p.record_type
                                    , p.bin_range_tag
                                    , p.card_number
                                    , p.alt_card_number
                                    , p.merchant_number
                                    , p.division_number
                                    , p.location_id
                                    , p.state
                                    , p.country
                                    , p.transaction_code
                                    , p.credit_debit_indicator
                                    , p.transaction_sequence_number
                                    , p.transaction_invoice_number
                                    , p.host_date
                                    , p.host_time
                                    , p.pos_date
                                    , p.pos_time
                                    , p.pos_currency_code
                                    , p.pos_requested_amount
                                    , p.pos_approved_amount
                                    , p.base_currency_code
                                    , p.base_requested_amount
                                    , p.base_approved_amount
                                    , p.currency_conversion_factor
                                    , p.currency_base_units
                                    , p.activation_date
                                    , p.activation_time
                                    , p.activation_merchant_number
                                    , p.activation_division_number
                                    , p.activation_location_id
									, p.original_transaction_sequence_number
									, p.original_pos_date
									, p.original_pos_time
									, p.card_working_balance)
                               IN (SELECT record_type
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
				   , original_transaction_sequence_number
				   , original_pos_date
				   , original_pos_time
				   , card_working_balance
	                     FROM $NZ_STGRDW_DB.$NZ_STGRDW_SCHEMA.$wrk_card_activity_parse
	                     MINUS
  	                     SELECT COALESCE(r.record_type,'?') AS record_type
			          , COALESCE(r.bin_range_tag,'-9999') AS bin_range_tag
			          , COALESCE(r.card_number,'-999999999999999999') AS card_number 
			          , COALESCE(r.alt_card_number,'-999999999999999999') AS alt_card_number
			          , COALESCE(r.merchant_number,-99999) AS merchant_number
			          , COALESCE(r.division_number,-9999) AS division_number
			          , COALESCE(r.location_id,'-999999999') AS location_id
			          , COALESCE(r.state,'??') AS state
			          , COALESCE(r.country,'??') AS country
			          , COALESCE(r.transaction_code,-9) AS transaction_code
			          , COALESCE(r.credit_debit_indicator,'?') AS credit_debit_indicator
			          , COALESCE(r.transaction_sequence_number,-99999) AS transaction_sequence_number
			          , COALESCE(r.transaction_invoice_number,'-999999999') AS transaction_invoice_number
			          , COALESCE(r.host_date,-9999999) AS host_date
			          , COALESCE(r.host_time,-99999) AS host_time
			          , COALESCE(r.pos_date,-9999999) AS pos_date
			          , COALESCE(r.pos_time,-99999) AS pos_time
			          , COALESCE(r.pos_currency_code,'-99') AS pos_currency_code
			          , COALESCE(r.pos_requested_amount,-9999999999999) AS pos_requested_amount
			          , COALESCE(r.pos_approved_amount,-9999999999999) AS pos_approved_amount
			          , COALESCE(r.base_currency_code,'-99') AS base_currency_code
			          , COALESCE(r.base_requested_amount,-9999999999999) AS base_requested_amount
			          , COALESCE(r.base_approved_amount,-9999999999999) AS base_approved_amount
			          , COALESCE(r.currency_conversion_factor,-99999999) AS currency_conversion_factor
			          , COALESCE(r.currency_base_units,-99999999) AS currency_base_units
			          , COALESCE(r.activation_date,-9999999) AS activation_date
			          , COALESCE(r.activation_time,-99999) AS activation_time
			          , COALESCE(r.activation_merchant_number,-99999) AS activation_merchant_number
			          , COALESCE(r.activation_division_number,-9999) AS activation_division_number
			          , COALESCE(r.activation_location_id,'-999999999') AS activation_location_id
					  , COALESCE(r.original_transaction_sequence_number,-99999) AS original_transaction_sequence_number
					  , COALESCE(r.original_pos_date,-9999999) AS original_pos_date
			          , COALESCE(r.original_pos_time,-99999) AS original_pos_time
					  , COALESCE(r.card_working_balance,-9999999999999) AS card_working_balance
	                     FROM $NZ_RDW_DB.$NZ_RDW_SCHEMA.$rdw_card_activity_target r
                             JOIN $NZ_METAMART_DB.$NZ_METAMART_SCHEMA.$job_run j ON r.job_run_id = j.job_run_id 
                                                                                 AND (j.processing_status = '$processing_status_success' 
                                                                                      OR (j.processing_status = '$processing_status_running'  
                                                                                          AND j.job_run_id=$JOB_RUN_ID )))
                          ORDER BY p.job_run_id,
                                   p.source_location,
                                   p.transmit_datetime,
                                   p.line_number"

print_msg "0010 Building table $wrk_card_activity_cdc"
run_query -d $NZ_STGRDW_DB -q "$card_activity_cdc_sql" -m "0011 CARD ACTIVITY change data capture (cdc) Processing failed when attempting to build table $wrk_card_activity_cdc"
   if [[ $? != 0 ]]
   then
      perl endJobRun.pl "$JOB_RUN_ID" "FAILURE"
      chk_err -r $return_code_fail -m "0012 ===> ERROR <=== CREATE of table $NZ_STGRDW_DB.$NZ_STGRDW_SCHEMA.$wrk_card_activity_cdc failed.  $SCRIPT finished UNSUCCESSFULLY."
      exit $return_code_fail
   fi


print_msg "0020 Processing complete for Card Activity cdc into table $NZ_STGRDW_DB.$NZ_STGRDW_SCHEMA.$wrk_card_activity_cdc"
echo ""

print_msg "9999 Processing complete for $SCRIPT_NAME with return code = $return_code_succeed"

