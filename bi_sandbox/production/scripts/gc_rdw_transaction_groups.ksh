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
# MODULE      : giftcard_post_rdw_load.pl
# DESCRIPTION : Loads the STGCDM Gift Card Activity table with GiftCard AIM (fullfilment) and SVS (approved transactions) data
#
# NOTES: Here is the processing components and the order in which they are called:
#            Tidal Group: BI_CDM_DAILY_GIFTCARD_GROUP 
#            Tidal Job: BI_CDM_1_DAILY_GIFTCARD_RDW_TO_CDM
#
#               PERL Script: giftcard_post_rdw_load.pl
#               UNIX Script: gc_cdm_giftcard.ksh
#                  UNIX Script :gc_stgcdm_giftcard_load1.ksh
#                  UNIX Script : gc_cdm_giftcard_load2.ksh 
#                  UNIX Script : gc_cdm_giftcard_load3.ksh
#
########################################################
#
# MODIFICATION HISTORY:
#  Date       Name              Mods Description
#  ---------- ----------------- ---- -------------------------------------
#  04/15/2013 Pooja Chadha           Including Activations and Redemptions Logic on the RDW giftcard activity and gift card group tables.
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
export SCRIPT_VERSION="1.0"

export ONGOING_DIR="/workspace/dstage/data/3CPDATA/ongoing"
export SCRIPTS_DIR="$ONGOING_DIR/scripts"

. $SCRIPTS_DIR/load_parameters.ksh
. $SCRIPTS_DIR/load_library.ksh
. $SCRIPTS_DIR/load_datastage_params.ksh

print_msg "0000 Processing started for $SCRIPT_NAME"
echo ''

# Variables
export processing_status_success="SUCCESS"
export processing_status_running="RUNNING"
export processing_datetime=`date +"%m/%d/%Y %T"`
export return_code_fail=3
export return_code_succeed=0
export rollbackflg=0
export q="'"
export qq='"'

print_msg "0001 Perform initial job run activities"

#export JOB_RUN_ID=`perl createJobRun.pl "$SCRIPT_NAME" "$SCRIPT_VERSION"`
#set -A predjobs `perl findUnprocessedJobRuns.pl gc_rdw_giftcard_aim_and_svs.ksh`
#debug $debug_ind "predjobs=${#predjobs[@]}"


echo ''
echo ''
echo ' ************** Process transaction groups for new job run ids**********************'
echo ''
echo ''
print_msg "0010 Start processing"

#  AIM processing variables

export src_table="SRC_GC_ORDER_RECON"
export source_data="SOURCE_DATA"
export job_run="JOB_RUN"
export job_run_id=$JOB_RUN_ID
export rdw_card_activity_target="GIFTCARD_CARD_ACTIVITY"
export source_name="REIRQ132"
export processing_datetime=`date +"%m/%d/%Y %T"`
export JOBRUN="JOB_RUN"
export M_DATETIME=`date +%Y-%m-%d\ %H:%M:%S`
export rollbackflg=0

echo '' 
print_msg "0011 Master JOB_RUN_ID = $JOB_RUN_ID"
echo ''


        print_msg "0020 gc_transaction_groups_db.pl starting..."
        # perl gc_rdw_transaction_groups_db.pl ${predjobs[$i]}
	 perl gc_rdw_transaction_groups_db.pl $JOB_RUN_ID
        ret_code=$?
        if [[ $ret_code != $rollbackflg ]]
        then
           print_msg "0023 ===> ERROR <=== gc_assign_transaction_groups.plfailed"
         #  perl endJobRun.pl "$JOB_RUN_ID" "FAILURE"
           exit $return_code_fail
        fi
        print_msg " 0024 gc_transaction_groups_db.pl script completed."
        echo ""
		

print_msg "0030 Processing complete for gift card transaction groups - gc_rdw_transaction_groups.ksh"
echo ""

print_msg "9999 Processing complete for $SCRIPT_NAME with return code = $return_code_succeed"

exit $return_code_succeed 

# Finish
