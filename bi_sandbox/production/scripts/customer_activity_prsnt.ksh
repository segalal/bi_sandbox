#!/bin/ksh
######################################################################
# Script :      customer_activity_prsnt.ksh 
# Description: This script runs the sequence job that loads the CUSTOMER_ACTIVITY 
#		and CUSTOMER_ACTIVITY_HISTORY in CDME   
# Modification:
# 1.0	07/23/2016	pnair	:Creation
#######################################################################

export SCRIPT_NAME=$(basename  $0)
export SCRIPT_VERSION="1.1"

. $SCRIPTS_DIR/load_parameters.ksh
. $SCRIPTS_DIR/load_library.ksh
. $SCRIPTS_DIR/load_datastage_params.ksh

   export script_return_code=0
   export return_code_fail=3

   print_msg "0001 Processing started for script $SCRIPT_NAME Version $SCRIPT_VERSION"

   export JOB_RUN_ID=`perl createJobRun.pl "seq_customer_activity_cdm_assignment" "$SCRIPT_VERSION"`

   print_msg "0002 Current Job ID = $JOB_RUN_ID"  

print_msg "0003 Process started for the Product Activity stage load"
    		dsjob -run -param job_run_id="$JOB_RUN_ID" -wait -warn 0 -jobstatus $DATA_STAGE_PROJECT seq_customer_activity_cdm_assignment
		ds_ret_code=$?


if [[ $ds_ret_code>=0 && $ds_ret_code<=2 ]]; then
   			print_msg "0010 Processing completed SUCCESSFULLY"
	
			else
			print_msg "0011 Processing completed UNSUCCESSFULLY"
			export script_return_code=3
			fi


if [[ $script_return_code != 0 ]]
   then
      print_msg "9999 *****ERROR**** Processing Completed Abnormally for $SCRIPT_NAME with Return Code="$script_return_code
      perl endJobRun.pl "$JOB_RUN_ID" "FAILURE"
      exit $return_code_fail
   else
      print_msg "9999 Processing Completed Normally for $SCRIPT_NAME"
      perl endJobRun.pl "$JOB_RUN_ID" "SUCCESS" 
   fi

   exit $script_return_code

