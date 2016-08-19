#!/bin/ksh
######################################################################
# Script :      product_merch_stage_legacy.ksh 
# Description: This script runs the sequence job that loads the stage table with legacy data
# 
# Modification:
# 1.0	3/07/2016	smohamm	:Creation
#######################################################################

export SCRIPT_NAME=$(basename  $0)
export SCRIPT_VERSION="1.1"

. $SCRIPTS_DIR/load_parameters.ksh
. $SCRIPTS_DIR/load_library.ksh
. $SCRIPTS_DIR/load_datastage_params.ksh

   export script_return_code=0
   export return_code_fail=3

   print_msg "0001 Processing started for script $SCRIPT_NAME Version $SCRIPT_VERSION"

   export JOB_RUN_ID=`perl createJobRun.pl "seq_legacy_load_product_merch" "$SCRIPT_VERSION"`

   print_msg "0002 Current Job ID = $JOB_RUN_ID"  


print_msg "0003 Starting the hist table loading sequenzer seq_legacy_load_product_merch"
    		dsjob -run -param job_run_id="$JOB_RUN_ID"  -wait -warn 0 -jobstatus $DATA_STAGE_PROJECT seq_legacy_load_product_merch

		ds_ret_code=$?


if [[ $ds_ret_code>=0 && $ds_ret_code<=2 ]]; then
   			print_msg "0004 Processing completed SUCCESSFULLY"
	
			else
			print_msg "0005 Processing completed UNSUCCESSFULLY"
			export script_return_code=3
			fi


if [[ $script_return_code != 0 ]]
   then
      print_msg "9999 *****ERROR**** Processing Completed Abnormally for $SCRIPT_NAME with Return Code="$script_return_code
      perl endJobRunFlow.pl "$JOB_RUN_ID" "FAILURE"
      exit $return_code_fail
   else
      print_msg "9999 Processing Completed Normally for $SCRIPT_NAME"
      perl endJobRunFlow.pl "$JOB_RUN_ID" "SUCCESS"
   fi

exit $script_return_code

