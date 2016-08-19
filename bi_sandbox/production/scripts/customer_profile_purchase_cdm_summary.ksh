#!/bin/ksh
#################################################################################################
# Script      :customer_profile_purchase_cdm_summary.ksh
# Description :This script is used to trigger the sequence seq_customer_profile_purchase_cdm_summary sequence job
# Modification:
# Creation Date:04-APR-2016
#################################################################################################

export SCRIPT_NAME=$(basename  $0)
export SCRIPT_VERSION="1.1"

. $SCRIPTS_DIR/load_parameters.ksh
. $SCRIPTS_DIR/load_library.ksh
. $SCRIPTS_DIR/load_datastage_params.ksh

   export script_return_code=0
   export return_code_fail=3
   export JOB_NAME="seq_customer_profile_purchase_cdm_summary"
   
   print_msg "0001 Processing started for script $SCRIPT_NAME Version $SCRIPT_VERSION"

   export JOB_RUN_ID=`perl createJobRun.pl "$JOB_NAME" "$SCRIPT_VERSION"`

   print_msg "0002 Current Job ID = $JOB_RUN_ID"  


   print_msg "0003 Starting the cdm aggregation table loading sequence job $JOB_NAME"
   dsjob -run -param job_run_id="$JOB_RUN_ID" -param rollingyears="0" -param rollingdays="15" -wait -warn 0 -jobstatus $DATA_STAGE_PROJECT seq_customer_profile_purchase_cdm_summary
   ds_ret_code=$?


 if [[ $ds_ret_code -ge 0 && $ds_ret_code -le 2 ]]; then
 	print_msg "0004 Processing completed SUCCESSFULLY"
	
 else
	print_msg "0005 Processing completed UNSUCCESSFULLY"
	export script_return_code=3
 fi


if [[ $script_return_code -ne 0 ]]
   then
      print_msg "9999 *****ERROR**** Processing Completed Abnormally for $SCRIPT_NAME with Return Code="$script_return_code
      perl endJobRunFlow.pl "$JOB_RUN_ID" "FAILURE"
      exit $return_code_fail
   else
      print_msg "9999 Processing Completed Normally for $SCRIPT_NAME"
      perl endJobRunFlow.pl "$JOB_RUN_ID" "SUCCESS"
   fi

exit $script_return_code

