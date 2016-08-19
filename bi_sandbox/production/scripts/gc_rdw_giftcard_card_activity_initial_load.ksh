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
# MODULE      : gc_rdw_giftcard_card_activity_initial_load.ksh 
# DESCRIPTION : Loads the RDW tables for GiftCard Approved Transactions (non-AIM) Order Recon 
#               with data from SVS files
#
#               This data is pulled by REI via file REIRQ132 and populated into table
#               HIST SRC_GC_ORDER_RECON.  This script calls four script which perform the following: 
#                   1) parsing
#                   2) change data capture
#                   3) transformation
#                   4) load into RDW table GIFTCARD_CARD_ACTIVITY
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
#
# SYNOPSIS:
#
########################################################
#
# MODIFICATION HISTORY:
#  Date       Name              Mods Description
#  ---------- ----------------- ---- -------------------------------------
#  11/04/2012 Anna Segal             Original Release
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

. $SCRIPTS_DIR/load_parameters.ksh
. $SCRIPTS_DIR/load_library.ksh
. $SCRIPTS_DIR/load_datastage_params.ksh

print_msg "0000 Processing started for $SCRIPT_NAME"

# Debug constants
export debug_ind="DEBUG"

# Variables

export min_job_run_id=1
export max_job_run_id=65

export src_table="SRC_GC_ORDER_RECON"
export wrk_card_activity_parse="WRK_GC_GIFTCARD_CARD_ACTIVITY_PARSE"
export wrk_card_activity_cdc="WRK_GC_GIFTCARD_CARD_ACTIVITY_CDC"
export wrk_card_activity_transform="WRK_GC_GIFTCARD_CARD_ACTIVITY_TRANSFORM"
export rdw_card_activity_target="GIFTCARD_CARD_ACTIVITY"

export job_run="JOB_RUN"
export source_data="SOURCE_DATA"
export processing_status_success="SUCCESS"
export processing_status_running="RUNNING"
export source_name="REIRQ132" 

export processing_datetime=`date +"%m/%d/%Y %T"`

export return_code_fail=3
export return_code_succeed=0

export q="'"
export qq='"'

echo ''
print_msg "0001 Processing will be performed against the following variables for the following tables and files:"
echo ''
echo "          src_table="$src_table
echo "          wrk_card_activity_parse="$wrk_card_activity_parse
echo "          wrk_card_activity_cdc="$wrk_card_activity_cdc
echo "          wrk_card_activity_tranform="$wrk_card_activity_transform
echo "          rdw_card_activity_target="$rdw_card_activity_target
echo "          job_run="$job_run
echo "          source_data="$source_data
echo "          source_name="$source_name
echo ''

export JOB_RUN_ID=`perl createJobRun.pl "$SCRIPT_NAME" "$SCRIPT_VERSION"`

debug $debug_ind "JOB_RUN_ID=$JOB_RUN_ID"


set -A predjobs `perl findUnprocessedJobRuns.pl Load_SRC_GC_ORDER_RECON.ksh`

print_msg "0002 ***** Note that the preds will be overwritten by the incrementing of variable i to process all historical rows regardless of whether they have been processed by other processes.  The closure of the job_run_id is not part of the mix for this backfill one-time processing.  The initial and ending values for i should be adjusted for each environment ****"

i=$min_job_run_id #array element count begins with .0.

debug $debug_ind "predjobs=${#predjobs[@]}"


while [ $i -lt $max_job_run_id ]
do
        export current_pred=$i
        print_msg "0005 Currently predecessor $current_pred is being processed"
        echo ""

	print_msg "0010 SVS Card Activity (Transactions) Level 1 Parsing script gc_rdw_giftcard_card_activity_parse_level_1.ksh starting..."
        .$SCRIPT_DIR/gc_rdw_giftcard_card_activity_parse_level_1.ksh
	ret_code=$?
	if [[ $ret_code != $return_code_succeed ]]
	then
	   print_msg "0011 ===> ERROR <=== SVS Card Activity Level 1 Parsing script gc_rdw_giftcard_card_activity_parse_level_1.ksh failed"
	   perl endJobRun.pl "$JOB_RUN_ID" "FAILURE"
	   exit $return_code_fail
	fi
	print_msg " 0012 SVS Card Activity Level 1 Parsing script completed."
	echo ""

	print_msg "0020 SVS Card Activity (Transactions) Level 1 CDC script gc_rdw_giftcard_card_activity_cdc_level_1.ksh starting..."
	.$SCRIPT_DIR/gc_rdw_giftcard_card_activity_cdc_level_1.ksh
	ret_code=$?
	if [[ $ret_code != $return_code_succeed ]]
	then
	   print_msg "0021 ===> ERROR <=== SVS Card Activity Level 1 CDC script gc_rdw_giftcard_card_activity_cdc_level_1.ksh failed."
	   perl endJobRun.pl "$JOB_RUN_ID" "FAILURE"
           exit $return_code_fail
	fi
	print_msg " 0022 SVS Card Activity Level 1 CDC completed."
	echo ""

        print_msg "0030 SVS Card Activity (Transactions) Level 1 Transformation script gc_rdw_giftcard_card_activity_transform_level_1.ksh starting..."
	.$SCRIPT_DIR/gc_rdw_giftcard_card_activity_transform_level_1.ksh
	ret_code=$?
        if [[ $ret_code != $return_code_succeed ]]
	then
	   print_msg "0031 ===> ERROR <=== SVS Card Activity Level 1 Transform script gc_rdw_giftcard_card_activity_transform_level_1.ksh failed."
	   perl endJobRun.pl "$JOB_RUN_ID" "FAILURE"
           exit $return_code_fail
	fi
	print_msg " 0032 SVS Card Activity Level 1 Transformation completed."
	echo ""

	print_msg "0040 SVS Card Activity (Transactions) Level 1 RDW gc_rdw_giftcard_card_activity_load_level_1.ksh starting..."
	.$SCRIPT_DIR/gc_rdw_giftcard_card_activity_load_level_1.ksh
	ret_code=$?
        if [[ $ret_code != $return_code_succeed ]]
	then
	   print_msg "0041 ===> ERROR <=== SVS Card Activity Level 1 RDW load script gc_rdw_giftcard_card_activity_load_level_1.ksh failed."
	   perl endJobRun.pl "$JOB_RUN_ID" "FAILURE"
           exit $return_code_fail
	fi
	print_msg " 0042 SVS Card Activity Level 1 RDW completed."
	echo ""

	(( i = i + 1 ))
done

print_msg "0050 Backfill Historical Processing Completed Normally for $SCRIPT_NAME with return code = $return_code_succeed"
perl endJobRun.pl "$JOB_RUN_ID" "SUCCESS" "${predjobs[@]}"


print_msg "0060 Processing complete for Card Activity table $NZ_RDW_DB.$NZ_RDW_SCHEMA.$rdw_card_activity_target"
echo ""

print_msg "9999 Processing complete for $SCRIPT_NAME with return code = $return_code_succeed"

exit $return_code_succeed 

# Finish
