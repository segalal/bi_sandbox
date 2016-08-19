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
# MODULE      : gc_rdw_giftcard_aim_and_svs.ksh 
# DESCRIPTION : Loads the RDW Gift Card tables wth GiftCard AIM (fullfullment) and SVS (approved transactions) data 
#
#               This script calls four script which perform the following: 
#                   1) parsing
#                   2) change data capture
#                   3) transformation
#                   4) load into RDW tables
#
# ABSTRACT:
#
# NOTES: Here is the processing components and the order in which they are called:
#            Tidal Group
#               UNIX Script
#                  UNIX Script : Load_SRC_GC_ORDER_RECON.ksh
#                  UNIX Script : Load_RDW_GC_ORDER_RECON.ksh
#                  UNIX Script : gc_rdw_giftcard_aim_and_svs.ksh 
#
#
# SYNOPSIS:
#
########################################################
#
# MODIFICATION HISTORY:
#  Date       Name              Mods Description
#  ---------- ----------------- ---- -------------------------------------
#  11/05/2012 Anna Segal             Combined Load_RDW_GC_ORDER_RECON.ksh and gc_rdw_giftcard_card_activity.ksh 
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
echo ''

# Debug constants
export debug_ind="DEBUG"

# Variables
export processing_status_success="SUCCESS"
export processing_status_running="RUNNING"
export processing_datetime=`date +"%m/%d/%Y %T"`
export return_code_fail=3
export return_code_succeed=0
export q="'"
export qq='"'

print_msg "0005 Perform initial job run activities"

export JOB_RUN_ID=`perl createJobRun.pl "$SCRIPT_NAME" "$SCRIPT_VERSION"`
set -A predjobs `perl findUnprocessedJobRuns.pl Load_SRC_GC_ORDER_RECON.ksh`
debug $debug_ind "predjobs=${#predjobs[@]}"


echo ''
echo ''
echo ' ************** AIM Fullfillment Processing *****************************************'
echo ''

echo ''
echo ''
print_msg "0010 Start processing for AIM data"

#  AIM processing variables

export WRKEMAIL="WRK_AIM_EMAIL_CDC"
export WRKPHONE="WRK_AIM_PHONE_CDC"
export WRKPERSON="WRK_AIM_PERSON_CDC"
export WRKADDRESS="WRK_AIM_ADDRESS_CDC"
export WRKORDER="WRK_AIM_ORD_PARSED"
export JOBRUN="JOB_RUN"
export M_DATETIME=`date +%Y-%m-%d\ %H:%M:%S`

echo '' 
print_msg "0011 Master JOB_RUN_ID = $JOB_RUN_ID"
echo ''

i=0 #array element count begins with .0.

while [ $i -lt ${#predjobs[*]} ]
do

        export current_pred=${predjobs[$i]}
        print_msg "0021 Currently predecessor $current_pred is being processed"
        echo ""
        print_msg "Pred = "$i
        echo ""

        print_msg "0022 AIM Parsing script gc_aim_parse.pl starting..."
        perl gc_aim_parse.pl ${predjobs[$i]}
        ret_code=$?
        if [[ $ret_code != $return_code_succeed ]]
        then
           print_msg "0023 ===> ERROR <=== Aim Parsing script Parse_Json_flat.pl failed"
           perl endJobRun.pl "$JOB_RUN_ID" "FAILURE"
           exit $return_code_fail
        fi
        print_msg " 0024 AIM Parsing script completed."
        echo ""

        print_msg "0030 Aim Level 1 CDC starting..."
        .$SCRIPT_DIR/gc_aim_cdc_level_1.ksh
        ret_code=$?
        if [[ $ret_code != $return_code_succeed ]]
        then
           print_msg "0031 ===> ERROR <=== Aim Level 1 CDC failed."
           perl endJobRun.pl "$JOB_RUN_ID" "FAILURE"
           exit $return_code_fail
        fi
        print_msg " 0032 AIM Level 1 CDC completed."
        echo ""

        print_msg "0040 Aim Level 1 Trans starting..."
        .$SCRIPT_DIR/gc_aim_trans_level_1.ksh
        ret_code=$?
        if [[ $ret_code != $return_code_succeed ]]
        then
           print_msg "0041 ===> ERROR <=== Aim Level 1 Trans failed."
           perl endJobRun.pl "$JOB_RUN_ID" "FAILURE"
           exit $return_code_fail
        fi
        print_msg " 0042 AIM Level 1 Trans completed."
        echo ""

        print_msg "0050 Aim Level 1 RDW starting..."
        .$SCRIPT_DIR/gc_aim_load_level_1.ksh
        ret_code=$?
        if [[ $ret_code != $return_code_succeed ]]
        then
           print_msg "0051 ===> ERROR <=== Aim Level 1 RDW failed."
           perl endJobRun.pl "$JOB_RUN_ID" "FAILURE"
           exit $return_code_fail
        fi
        print_msg " 0052 AIM Level 1 RDW completed."
        echo ""

        print_msg "0060 Aim Level 2 CDC starting..."
        .$SCRIPT_DIR/gc_aim_cdc_level_2.ksh
        ret_code=$?
        if [[ $ret_code != $return_code_succeed ]]
        then
           print_msg "0061 ===> ERROR <=== Aim Level 2 CDC failed."
           perl endJobRun.pl "$JOB_RUN_ID" "FAILURE"
           exit $return_code_fail
        fi
        print_msg " 0062 AIM Level 2 CDC completed."
        echo ""

        print_msg "0070 Aim Level 2 Trans starting..."
        .$SCRIPT_DIR/gc_aim_trans_level_2.ksh
        ret_code=$?
        if [[ $ret_code != $return_code_succeed ]]
        then
           print_msg "0071 ===> ERROR <=== Aim Level 2 Trans failed."
           perl endJobRun.pl "$JOB_RUN_ID" "FAILURE"
           exit $return_code_fail
        fi
        print_msg " 0072 AIM Level 2 Trans completed."
        echo ""

        print_msg "0080 Aim Level 2 RDW starting..."
        .$SCRIPT_DIR/gc_aim_load_level_2.ksh
        ret_code=$?
        if [[ $ret_code != $return_code_succeed ]]
        then
           print_msg "0081 ===> ERROR <=== Aim Level 2 RDW failed."
           perl endJobRun.pl "$JOB_RUN_ID" "FAILURE"
           exit $return_code_fail
        fi
        print_msg " 0082 AIM Level 2 RDW completed."
        echo ""

        print_msg "0090 Aim Level 3 CDC starting..."
        .$SCRIPT_DIR/gc_aim_cdc_level_3.ksh
        ret_code=$?
        if [[ $ret_code != $return_code_succeed ]]
        then
           print_msg "0091 ===> ERROR <=== Aim Level 3 CDC failed."
           perl endJobRun.pl "$JOB_RUN_ID" "FAILURE"
           exit $return_code_fail
        fi
        print_msg " 0092 AIM Level 3 CDC completed."
        echo ""

        print_msg "0100 Aim Level 3 Trans starting..."
        .$SCRIPT_DIR/gc_aim_trans_level_3.ksh
        ret_code=$?
        if [[ $ret_code != $return_code_succeed ]]
        then
           print_msg "0101 ===> ERROR <=== Aim Level 3 Trans failed."
           perl endJobRun.pl "$JOB_RUN_ID" "FAILURE"
           exit $return_code_fail
        fi
        print_msg " 0102 AIM Level 3 Trans completed."
        echo ""

        print_msg "0110 Aim Level 3 RDW starting..."
        .$SCRIPT_DIR/gc_aim_load_level_3.ksh
        ret_code=$?
        if [[ $ret_code != $return_code_succeed ]]
        then
           print_msg "0111 ===> ERROR <=== Aim Level 3 RDW failed."
           perl endJobRun.pl "$JOB_RUN_ID" "FAILURE"
           exit $return_code_fail
        fi
        print_msg " 0112 AIM Level 3 RDW completed."
        echo ""

        print_msg "0120 Aim Level 4 CDC starting..."
        .$SCRIPT_DIR/gc_aim_cdc_level_4.ksh
        ret_code=$?
        if [[ $ret_code != $return_code_succeed ]]
        then
           print_msg "0121 ===> ERROR <=== Aim Level 4 CDC failed."
           perl endJobRun.pl "$JOB_RUN_ID" "FAILURE"
           exit $return_code_fail
        fi
        print_msg " 0122 AIM Level 4 CDC completed."
        echo ""

        print_msg "0130 Aim Level 4 Trans starting..."
        .$SCRIPT_DIR/gc_aim_trans_level_4.ksh
        ret_code=$?
        if [[ $ret_code != $return_code_succeed ]]
        then
           print_msg "0131 ===> ERROR <=== Aim Level 4 Trans failed."
           perl endJobRun.pl "$JOB_RUN_ID" "FAILURE"
           exit $return_code_fail
        fi
        print_msg " 0132 AIM Level 4 Trans completed."
        echo ""

        print_msg "0140 Aim Level 4 RDW starting..."
        .$SCRIPT_DIR/gc_aim_load_level_4.ksh
        ret_code=$?
        if [[ $ret_code != $return_code_succeed ]]
        then
           print_msg "0141 ===> ERROR <=== Aim Level 4 RDW failed."
           perl endJobRun.pl "$JOB_RUN_ID" "FAILURE"
           exit $return_code_fail
        fi
        print_msg " 0142 AIM Level 4 RDW completed."
        echo ""

        (( i = i + 1 ))
done

if [[ $debug_ind != "DEBUG" ]]
then
        print_msg "0150 dropping WRK tables"

        drop_table -d $NZ_STGRDW_DB -t $NZ_STGRDW_DB.$NZ_STGRDW_SCHEMA.$WRKEMAIL
           if [[ $ret_code != 0 ]]
           then
               print_msg "0151 ===> ERROR <=== Drop of table $NZ_STGRDW_DB..$WRKEMAIL failed"
               perl endJobRun.pl "$JOB_RUN_ID" "FAILURE"
               exit $return_code_fail
           fi

        drop_table -d $NZ_STGRDW_DB -t $NZ_STGRDW_DB.$NZ_STGRDW_SCHEMA.$WRKPHONE
           if [[ $ret_code != 0 ]]
           then
               print_msg "0152 ===> ERROR <=== Drop of table $NZ_STGRDW_DB..$WRKPHONE failed"
               perl endJobRun.pl "$JOB_RUN_ID" "FAILURE"
               exit $return_code_fail
           fi

        drop_table -d $NZ_STGRDW_DB -t $NZ_STGRDW_DB.$NZ_STGRDW_SCHEMA.$WRKPERSON
           if [[ $ret_code != 0 ]]
           then
               print_msg "0153 ===> ERROR <=== Drop of table $NZ_STGRDW_DB..$WRKPERSON failed"
               perl endJobRun.pl "$JOB_RUN_ID" "FAILURE"
               exit $return_code_fail
           fi

        drop_table -d $NZ_STGRDW_DB -t $NZ_STGRDW_DB.$NZ_STGRDW_SCHEMA.$WRKADDRESS
           if [[ $ret_code != 0 ]]
           then
               print_msg "0154 ===> ERROR <=== Drop of table $NZ_STGRDW_DB..$WRKADDRESS failed"
               perl endJobRun.pl "$JOB_RUN_ID" "FAILURE"
               exit $return_code_fail
           fi

        drop_table -d $NZ_STGRDW_DB -t $NZ_STGRDW_DB.$NZ_STGRDW_SCHEMA.$WRKORDER
           if [[ $ret_code != 0 ]]
           then
               print_msg "0155 ===> ERROR <=== Drop of table $NZ_STGRDW_DB..$WRKORDER failed"
               perl endJobRun.pl "$JOB_RUN_ID" "FAILURE"
               exit $return_code_fail
           fi

else
        print_msg "0500 WARNING: Debug is turned on.  WRK tables will not be dropped."
fi

echo ''
echo ''
echo ' ************** SVS Transaction Processing *****************************************'
echo ''
print_msg "1000 Start processing for SVS Approved Transactions data"
echo ''

#  SVS processing variables

export src_table="SRC_GC_ORDER_RECON"
export source_data="SOURCE_DATA"
export job_run="JOB_RUN"
export wrk_card_activity_parse="WRK_GC_GIFTCARD_CARD_ACTIVITY_PARSE"
export wrk_card_activity_cdc="WRK_GC_GIFTCARD_CARD_ACTIVITY_CDC"
export wrk_card_activity_transform="WRK_GC_GIFTCARD_CARD_ACTIVITY_TRANSFORM"
export rdw_card_activity_target="GIFTCARD_CARD_ACTIVITY"
export source_name="REIRQ132"
export processing_datetime=`date +"%m/%d/%Y %T"`

echo ''
print_msg "1001 Processing will be performed against the following variables for the following tables and files:"
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

print_msg "1002 Master JOB_RUN_ID = $JOB_RUN_ID"
echo ''

i=0 #array element count begins with .0.

while [ $i -lt ${#predjobs[*]} ]
do

        export current_pred=${predjobs[$i]}
        print_msg "1005 Currently predecessor $current_pred is being processed"
        echo ""
        print_msg "Pred = "$i
        echo ""

	print_msg "1010 SVS Card Activity (Transactions) Level 1 Parsing script gc_rdw_giftcard_card_activity_parse_level_1.ksh starting..."
        .$SCRIPT_DIR/gc_rdw_giftcard_card_activity_parse_level_1.ksh
	ret_code=$?
	if [[ $ret_code != $return_code_succeed ]]
	then
	   print_msg "1011 ===> ERROR <=== SVS Card Activity Level 1 Parsing script gc_rdw_giftcard_card_activity_parse_level_1.ksh failed"
	   perl endJobRun.pl "$JOB_RUN_ID" "FAILURE"
	   exit $return_code_fail
	fi
	print_msg " 1012 SVS Card Activity Level 1 Parsing script completed."
	echo ""

	print_msg "1020 SVS Card Activity (Transactions) Level 1 CDC script gc_rdw_giftcard_card_activity_cdc_level_1.ksh starting..."
	.$SCRIPT_DIR/gc_rdw_giftcard_card_activity_cdc_level_1.ksh
	ret_code=$?
	if [[ $ret_code != $return_code_succeed ]]
	then
	   print_msg "1021 ===> ERROR <=== SVS Card Activity Level 1 CDC script gc_rdw_giftcard_card_activity_cdc_level_1.ksh failed."
	   perl endJobRun.pl "$JOB_RUN_ID" "FAILURE"
           exit $return_code_fail
	fi
	print_msg " 1022 SVS Card Activity Level 1 CDC completed."
	echo ""

        print_msg "1030 SVS Card Activity (Transactions) Level 1 Transformation script gc_rdw_giftcard_card_activity_transform_level_1.ksh starting..."
	.$SCRIPT_DIR/gc_rdw_giftcard_card_activity_transform_level_1.ksh
	ret_code=$?
        if [[ $ret_code != $return_code_succeed ]]
	then
	   print_msg "1031 ===> ERROR <=== SVS Card Activity Level 1 Transform script gc_rdw_giftcard_card_activity_transform_level_1.ksh failed."
	   perl endJobRun.pl "$JOB_RUN_ID" "FAILURE"
           exit $return_code_fail
	fi
	print_msg " 1032 SVS Card Activity Level 1 Transformation completed."
	echo ""

	print_msg "1040 SVS Card Activity (Transactions) Level 1 RDW gc_rdw_giftcard_card_activity_load_level_1.ksh starting..."
	.$SCRIPT_DIR/gc_rdw_giftcard_card_activity_load_level_1.ksh
	ret_code=$?
        if [[ $ret_code != $return_code_succeed ]]
	then
	   print_msg "1041 ===> ERROR <=== SVS Card Activity Level 1 RDW load script gc_rdw_giftcard_card_activity_load_level_1.ksh failed."
	   perl endJobRun.pl "$JOB_RUN_ID" "FAILURE"
           exit $return_code_fail
	fi
	print_msg " 1042 SVS Card Activity Level 1 RDW completed."
	echo ""


	(( i = i + 1 ))
done


  print_msg "1050 Process Gift Card Transaction Groups gc_rdw_transaction_groups.ksh in RDW"
  $SCRIPTS_DIR/gc_rdw_transaction_groups.ksh

   if [[ $? != 0 ]]
   then
      print_msg "1051 ===> ERROR <=== Processing completed UNSUCCESSFULLY for $SCRIPTS_DIR/gc_rdw_transaction_groups.ksh"
      perl endJobRun.pl "$JOB_RUN_ID" "FAILURE"
      exit $return_code_fail
   else print_msg "1051 Processing Completed Normally for $SCRIPTS_DIR/gc_rdw_transaction_groups.ksh"
   fi

print_msg "1060 Processing complete for Card Activity table $NZ_RDW_DB.$NZ_RDW_SCHEMA.$rdw_card_activity_target"
echo ""

perl endJobRun.pl "$JOB_RUN_ID" "SUCCESS" "${predjobs[@]}"

print_msg "9999 Processing complete for $SCRIPT_NAME with return code = $return_code_succeed"

exit $return_code_succeed 

# Finish
