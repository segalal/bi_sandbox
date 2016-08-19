#!/bin/ksh
######################################################################
# script :      load_src_cde_customer_demographics_control_file.ksh 
# description : This script calls LOAD_SRC_table_v2.ksh to perform the initial load of feed file data 
#               from flatfiles residing at /workspace/dstage/data/3CPDATA/ongoing/dgraphic/current/zarchive 
#               to table SRC tables
# Modifications :
# 1.0  2015-Aug-19  Infosys : Creation of Script
####################################################################################

export SCRIPT_NAME=$(basename  $0)
export SCRIPT_VERSION="1.1"

. $SCRIPTS_DIR/load_parameters.ksh
. $SCRIPTS_DIR/load_library.ksh
. $SCRIPTS_DIR/load_datastage_params.ksh

# Uncomment the following if testing against the ETL% dev databases
#export NZ_HIST_DB=etlhist
#export NZ_FEEDTEMP_DB=etlstg

export script_return_code=0

print_msg "0001 Processing started for script $SCRIPT_NAME Version $SCRIPT_VERSION"
echo "" 

export process_name="load_src_cde_customer_demographics_control_file"
export JOB_RUN_ID_MASTER=`perl createJobRun.pl "$SCRIPT_NAME" "$SCRIPT_VERSION"`

print_msg "0002 Current Job ID = $JOB_RUN_ID_MASTER for $SCRIPT_NAME"
echo ""


print_msg "0003 Start processing each of the Contact Event sources (15)"
echo ""


export source="CONTROL_FILE"
print_msg "0010 Load of Table SRC_ACX_CUSTOMER_DEMOGRAPHICS started for source $source"

export JOB_RUN_ID=`perl createJobRun.pl "$process_name.$source" "$SCRIPT_VERSION"`
print_msg "0011 Current Job ID = $JOB_RUN_ID"

.$SCRIPT_DIR/LOAD_SRC_table_v2.ksh -t SRC_ACX_CUSTOMER_DEMOGRAPHICS -a dgraphic -f acxiom_to_rei_build_*.ctl -b XML -z UNZIPPED -r KEEP_FILE -i $JOB_RUN_ID
ret_code=$?
if [[ $ret_code != 0 ]]
then
   print_msg "0012 Table SRC_ACX_CUSTOMER_DEMOGRAPHICS did not load successfully for the acxiom_to_rei_build_*.ctl  source file"
   perl endJobRun.pl "$JOB_RUN_ID" "FAILURE"   
   export script_return_code=3
else
   perl endJobRun.pl "$JOB_RUN_ID" "SUCCESS" "${predjobs[@]}"
fi
echo ""




if [[ $script_return_code != 0 ]]
then
   print_msg "9999 *****ERROR**** Processing Completed Abnormally for $SCRIPT_NAME with Return Code="$script_return_codei
   perl endJobRun.pl "$JOB_RUN_ID_MASTER" "FAILURE"
else
   print_msg "9999 Processing Completed Normally for $SCRIPT_NAME"
   perl endJobRun.pl "$JOB_RUN_ID_MASTER" "SUCCESS" "${predjobs[@]}"
fi

exit $script_return_code

# Finish
