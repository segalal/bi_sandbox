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
# MODULE      : gc_rdw_giftcard_card_activity_transform_level_1.ksh 
# DESCRIPTION : This script loads new data into the level 1 RDW TRAN table for SVS non-AIM
#               approved transations. Keys and NULLs are handled.   
#
#               This data is pulled by REI via file REIRQ132 and populated into table
#               HIST SRC_GC_ORDER_RECON.  This script is the 3rd of four which will perform
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
#                     UNIX Script:  gc_rdw_giftcard_card_activity_load_level_1.ksh#
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
#  04/12/2013 Izic Chon				 Fixed bug - DECODE defaults to -99999 instead of -9 for original_transaction_sequence_number and transaction_sequence_number
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

print_msg "0001 Beginning processing for SVS Card Activity (Approved Transactions) Transformations"

print_msg "0002 Job Run ID is $JOB_RUN_ID and current predecessor is $current_pred"

print_msg "0005 Dropping table $wrk_card_activity_transform"
drop_table -d $NZ_STGRDW_DB -t $NZ_STGRDW_DB.$NZ_STGRDW_SCHEMA.$wrk_card_activity_transform
   if [[ $? != 0 ]]
   then
      perl endJobRun.pl "$JOB_RUN_ID" "FAILURE"
      chk_err -r $return_code_fail -m "0006 ===> ERROR <=== Drop of table $NZ_STGRDW_DB.$NZ_STGRDW_SCHEMA.$wrk_card_activity_transform failed.  $SCRIPT finished UNSUCCESSFULLY."
      exit $return_code_fail
   fi

print_msg "0007 Table $wrk_card_activity_tran was successfully dropped or did not exist"


export card_activity_transform_sql="CREATE TABLE $wrk_card_activity_transform
                               AS
                               SELECT COALESCE((SELECT MAX(card_activity_key) 
                                               FROM $NZ_RDW_DB.$NZ_RDW_SCHEMA.$rdw_card_activity_target),0) 
                                               + RANK() OVER (ORDER BY p.job_run_id, p.transmit_datetime, p.source_location, p.line_number) AS card_activity_key
                                    , DECODE(c.record_type,'?',NULL,c.record_type) AS record_type
			            , DECODE(c.bin_range_tag,'-9999',NULL,c.bin_range_tag) AS bin_range_tag
			            , DECODE(c.card_number,'-999999999999999999',NULL,c.card_number) AS card_number 
			            , DECODE(c.alt_card_number,'-999999999999999999',NULL,c.alt_card_number) AS alt_card_number
			            , DECODE(c.merchant_number,-99999,NULL,c.merchant_number) AS merchant_number
			            , DECODE(c.division_number,-9999,NULL,c.division_number) AS division_number
			            , DECODE(c.location_id,'-999999999',NULL,c.location_id) AS location_id
			            , DECODE(c.state,'??',NULL,c.state) AS state
			            , DECODE(c.country,'??',NULL,c.country) AS country
			            , DECODE(c.transaction_code,-9,NULL,c.transaction_code) AS transaction_code
			            , DECODE(c.credit_debit_indicator,'?',NULL,c.credit_debit_indicator) AS credit_debit_indicator
			            , DECODE(c.transaction_sequence_number,-99999,NULL,c.transaction_sequence_number) AS transaction_sequence_number
			            , DECODE(c.transaction_invoice_number,'-999999999',NULL,c.transaction_invoice_number) AS transaction_invoice_number
			            , DECODE(c.host_date,-9999999,NULL,c.host_date) AS host_date
			            , DECODE(c.host_time,-99999,NULL,c.host_time) AS host_time
			            , DECODE(c.pos_date,-9999999,NULL,c.pos_date) AS pos_date
			            , DECODE(c.pos_time,-99999,NULL,c.pos_time) AS pos_time
			            , DECODE(c.pos_currency_code,'-99',NULL,c.pos_currency_code) AS pos_currency_code
			            , DECODE(c.pos_requested_amount,-9999999999999,NULL,c.pos_requested_amount) AS pos_requested_amount
			            , DECODE(c.pos_approved_amount,-9999999999999,NULL,c.pos_approved_amount) AS pos_approved_amount
			            , DECODE(c.base_currency_code,'-99',NULL,c.base_currency_code) AS base_currency_code
			            , DECODE(c.base_requested_amount,-9999999999999,NULL,c.base_requested_amount) AS base_requested_amount
			            , DECODE(c.base_approved_amount,-9999999999999,NULL,c.base_approved_amount) AS base_approved_amount
			            , DECODE(c.currency_conversion_factor,-99999999,NULL,c.currency_conversion_factor) AS currency_conversion_factor
			            , DECODE(c.currency_base_units,-99999999,NULL,c.currency_base_units) AS currency_base_units
			            , DECODE(c.activation_date,-9999999,NULL,c.activation_date) AS activation_date
			            , DECODE(c.activation_time,-99999,NULL,c.activation_time) AS activation_time
			            , DECODE(c.activation_merchant_number,-99999,NULL,c.activation_merchant_number) AS activation_merchant_number
			            , DECODE(c.activation_division_number,-9999,NULL,c.activation_division_number) AS activation_division_number
			            , DECODE(c.activation_location_id,'-999999999',NULL,c.activation_location_id) AS activation_location_id
						, DECODE(c.original_transaction_sequence_number,-99999,NULL,c.original_transaction_sequence_number) AS original_transaction_sequence_number
						, DECODE(c.original_pos_date,-9999999,NULL,c.original_pos_date) AS original_pos_date
			            , DECODE(c.original_pos_time,-99999,NULL,c.original_pos_time) AS original_pos_time
						, DECODE(c.card_working_balance,-9999999999999,NULL,c.card_working_balance) AS card_working_balance
                                    , p.transmit_datetime
                                    , p.source_location
                                    , p.line_number
                                    , p.job_run_id
                                    , '$processing_datetime' AS m_insert_datetime
                                    , '$processing_datetime' AS m_last_update_datetime
                                FROM $NZ_STGRDW_DB.$NZ_STGRDW.$wrk_card_activity_parse p
                                INNER JOIN $NZ_STGRDW_DB.$NZ_STGRDW.$wrk_card_activity_cdc c 
                                                        ON  p.record_type = c.record_type
                         	                        AND p.bin_range_tag = c.bin_range_tag
						 	AND p.card_number = c.card_number
							AND p.card_number = c.card_number
							AND p.alt_card_number = c.alt_card_number
							AND p.merchant_number = c.merchant_number
							AND p.division_number = c.division_number
							AND p.location_id = c.location_id
							AND p.state = c.state
							AND p.country = c.country
							AND p.transaction_code = c.transaction_code
							AND p.credit_debit_indicator = c.credit_debit_indicator
							AND p.transaction_sequence_number = c.transaction_sequence_number
							AND p.transaction_invoice_number = c.transaction_invoice_number
							AND p.host_date = c.host_date
							AND p.host_time = c.host_time
							AND p.pos_date = c.pos_date
							AND p.pos_time = c.pos_time
							AND p.pos_currency_code = c.pos_currency_code
							AND p.pos_requested_amount = c.pos_requested_amount
							AND p.pos_approved_amount = c.pos_approved_amount
							AND p.base_currency_code = c.base_currency_code
							AND p.base_requested_amount = c.base_requested_amount
							AND p.base_approved_amount = c.base_approved_amount
							AND p.currency_conversion_factor = c.currency_conversion_factor
							AND p.currency_base_units = c.currency_base_units
							AND p.activation_date = c.activation_date
							AND p.activation_time = c.activation_time
							AND p.activation_merchant_number = c.activation_merchant_number
							AND p.activation_division_number = c.activation_division_number
							AND p.activation_location_id = c.activation_location_id
							AND p.original_transaction_sequence_number = c.original_transaction_sequence_number
							AND p.original_pos_date = c.original_pos_date
							AND p.original_pos_time = c.original_pos_time
							AND p.card_working_balance = c.card_working_balance
                                                        AND p.transmit_datetime = c.transmit_datetime
                                                        AND p.source_location = c.source_location
                                                        AND p.line_number = c.line_number
                                                        AND p.job_run_id = c.job_run_id
                                ORDER BY p.job_run_id,
                                         p.source_location,
                                         p.transmit_datetime,
                                         p.line_number"
							
			

print_msg "0010 Building table $wrk_card_activity_tran"
run_query -d $NZ_STGRDW_DB -q "$card_activity_transform_sql" -m "0011 CARD ACTIVITY change data Transformation Processing failed when attempting to build table $wrk_card_activity_transform"
   if [[ $? != 0 ]]
   then
      perl endJobRun.pl "$JOB_RUN_ID" "FAILURE"
      chk_err -r $return_code_fail -m "0012 ===> ERROR <=== CREATE of table $NZ_STGRDW_DB.$NZ_STGRDW_SCHEMA.$wrk_card_activity_transform failed.  $SCRIPT finished UNSUCCESSFULLY."
      exit $return_code_fail
   fi


print_msg "0020 Processing complete for Card Activity Transformations into table $NZ_STGRDW_DB.$NZ_STGRDW_SCHEMA.$wrk_card_activity_transform"
echo ""

print_msg "9999 Processing complete for $SCRIPT_NAME with return code = $return_code_succeed"

