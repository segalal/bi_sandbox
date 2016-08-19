#!/bin/ksh
######################################################################
# Script Name:  address_dim_onetime_update.ksh 
# Description : This script will be called from DataStage and will update the dma_geo_key of addr_dim
#               CTASK78846
# Purpose:      A DataStage Seq and script is necessary to update the METAMART.job_run table to have test rows ignored
#               in downstream processing. 
#
#
# Created :
# 1.0  2016-Apr-20 Govind Hassan : Creation of Script
#
#######################################################################

export SCRIPT_NAME=$(basename  $0)
export SCRIPT_VERSION="1.0"

. $SCRIPTS_DIR/load_parameters.ksh
. $SCRIPTS_DIR/load_library.ksh
. $SCRIPTS_DIR/load_datastage_params.ksh

   export script_return_code=0

   print_msg "0001 Processing started for script $SCRIPT_NAME"

#   export JOB_RUN_ID=`perl createJobRun.pl "$SCRIPT_NAME" "$SCRIPT_VERSION"`
#   print_msg "0002 Current Job ID = $JOB_RUN_ID"

   echo ''
   print_msg "0003 Update the status for JOB_RUN rows which are to be ignored in downstream processing for the HISTP SRC_CUSTOMER_MESSAGING table"
 

update_statement="

-----------------------------
update stgcdm..ADDR_DIM addr set addr.dma_geo_key = dma.dma_geo_key,
addr.LAST_UPDATE_DATETIME=current_date
from cdme..DMA_GEO_DIM dma
where dma.dma_zip_code = addr.zip5_code
and dma.dma_geo_key <> addr.dma_geo_key
---------------------------
";



   run_query -d STGCDM -q "$update_statement" -m "0004 Update of table METAMART..JOB_RUN column PROCESSING_STATUS failed"
   ret_code=$?
   print_msg "0005 The update of table METAMART..JOB_RUN for SRC_CUSTOMER_MESSAGING rows was successful"
   if [[ $ret_code != 0 ]]
   then
      print_msg "0005 The update of table METAMART..JOB_RUN for SRC_CUSTOMER_MESSAGING rows was unsuccessful"
      export script_return_code=3

   fi
 
if [[ $script_return_code != 0 ]]
then
   print_msg "9999 *****ERROR**** Processing Completed Abnormally for $SCRIPT_NAME with Return Code="$script_return_code
#   perl endJobRun.pl "$JOB_RUN_ID" "FAILURE"
else
   print_msg "9999 Processing Completed Normally for $SCRIPT_NAME"
#   perl endJobRun.pl "$JOB_RUN_ID" "SUCCESS" "${predjobs[@]}"
fi



update_statement="

-----------------------------
update cdme..ADDR_DIM addr set addr.dma_geo_key = dma.dma_geo_key,
addr.LAST_UPDATE_DATETIME=current_date
from cdme..DMA_GEO_DIM dma
where dma.dma_zip_code = addr.zip5_code
and dma.dma_geo_key <> addr.dma_geo_key
---------------------------
";



   run_query -d CDME -q "$update_statement" -m "0004 Update of table METAMART..JOB_RUN column PROCESSING_STATUS failed"
   ret_code=$?
   print_msg "0005 The update of table METAMART..JOB_RUN for SRC_CUSTOMER_MESSAGING rows was successful"
   if [[ $ret_code != 0 ]]
   then
      print_msg "0005 The update of table METAMART..JOB_RUN for SRC_CUSTOMER_MESSAGING rows was unsuccessful"
      export script_return_code=3

   fi
 
if [[ $script_return_code != 0 ]]
then
   print_msg "9999 *****ERROR**** Processing Completed Abnormally for $SCRIPT_NAME with Return Code="$script_return_code
#   perl endJobRun.pl "$JOB_RUN_ID" "FAILURE"
else
   print_msg "9999 Processing Completed Normally for $SCRIPT_NAME"
#   perl endJobRun.pl "$JOB_RUN_ID" "SUCCESS" "${predjobs[@]}"
fi

exit $script_return_code

# Finish

