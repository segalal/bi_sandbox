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
# MODULE      : load_src_mhr_2014.ksh 
# DESCRIPTION : Loads the 2014 merchandise hierarchy reclassification data into SRC_MERCH_HIERARCHY_RECLASSIFICATION 
#               This reclassification was originally scheduled for 3/1/2014, but it was postponed until 4/5/2014
#
# ABSTRACT:
#
# NOTES:
#
# SYNOPSIS:
#
########################################################
#
# MODIFICATION HISTORY:
#  Date       Name              Mods Description
#  ---------- ----------------- ---- -------------------------------------
#  03-01-2014 Anna Segal             Original Release postponed
#  04-05-2014 Anna Segal             Released for the first time
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

export script_return_code=0

export JOB_RUN_ID=`perl createJobRun.pl "$SCRIPT_NAME" "$SCRIPT_VERSION"`

print_msg "0001 Load table SRC_MERCH_HIERARCHY_RECLASSIFICATION"

.$SCRIPT_DIR/LOAD_SRC_table_v2.ksh -t SRC_MERCH_HIERARCHY_RECLASSIFICATION -a jdad -f mhr_20150711.csv -b NEWLINE -z UNZIPPED -r KEEP_FILE -i $JOB_RUN_ID 

ret_code=$?
if [[ $ret_code != 0 ]]
then
   print_msg "0002 Table SRC_MERCH_HIERARCHY_RECLASSIFICATION did not load successfully"
   perl endJobRun.pl "$JOB_RUN_ID" "FAILURE"
   export script_return_code=3
fi

if [[ $script_return_code != 0 ]]
then
   print_msg "0005 *****ERROR**** Processing Completed Abnormally for $SCRIPT_NAME with Return Code="$script_return_code
   exit $script_return_code
else
   print_msg "0006 Processing Completed Normally for $SCRIPT_NAME"
   perl endJobRun.pl "$JOB_RUN_ID" "SUCCESS"
fi

exit $script_return_code

# Finish
