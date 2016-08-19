#!/bin/ksh
######################################################################
# Script :     product_catalog_code_stg.ksh 
# Description: This script runs the sequence job that loads the stage table with history data of Product Catalaog Code
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

   export JOB_RUN_ID=`perl createJobRun.pl "product_catalog_code_stg_load" "$SCRIPT_VERSION"`

   print_msg "0002 Current Job ID = $JOB_RUN_ID"  


print_msg "0003 unprcoessed job run ids"
	set -A predjobids `perl findUnprocessedJobRunsFlow.pl "product_catalog_code_stg_load"`

print_msg "0004 Creating a master array to store all unprcoessed job run ids"
	set -A allJobIDs
	t=0

print_msg "0005 Creating variable for each array to concatenate as a string to use in SQL later"
	export PRED_JOBS_IDS="("
	i=0
	while [ $i -lt ${#predjobids[*]} ]
	do
		export PRED_JOBS_IDS="$PRED_JOBS_IDS${predjobids[$i]}," 
		allJobIDs[$t]=${predjobids[$i]}
		(( i = i + 1 ))
		(( t = t + 1 ))
	done
	export PRED_JOBS_IDS="${PRED_JOBS_IDS:0:${#PRED_JOBS_IDS}-1})"
	if [[ ${#PRED_JOBS_IDS} = 1 ]]
	then
		export PRED_JOBS_IDS="(0)"
	fi
	print_msg "0006   Merch Pred Job_Run_Ids: $PRED_JOBS_IDS" 


print_msg "0007 Starting the hist table loading sequenzer product_catalog_code_stg_load"
    		dsjob -run -param job_run_id="$JOB_RUN_ID" -param pjob_run_id="$PRED_JOBS_IDS" -wait -warn 0 -jobstatus $DATA_STAGE_PROJECT stg_Load_Stage_WRK_PRODUCT_CATALOG_CODE

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
      perl endJobRunFlow.pl "$JOB_RUN_ID" "FAILURE"
      exit $return_code_fail
   else
      print_msg "9999 Processing Completed Normally for $SCRIPT_NAME"
      perl endJobRunFlow.pl "$JOB_RUN_ID" "SUCCESS" "${predjobids[@]}"
   fi

exit $script_return_code

