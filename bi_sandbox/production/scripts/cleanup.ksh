#!/bin/ksh
# Script :      cleanup.ksh
# Description : This Script remove old files from unix location including vendersextract
#
#
# Modifications :
# 1.0  2014-May-06      REI        : Creation
# 2.0  2014-Aug-08      REI        : Added new file for weekly attribution for daily cleanup retention for 14 days.
# 3.0  2015-Jul-27	smohamm	: Added new scripts to delete responsys .log and .bad files older than 7days.
# 4.0  2016-Mar-08  ksudha  : Added scripts to delete product reject files older than 14 days.
#######################################################################

export SCRIPTS_DIR=/workspace/dstage/data/3CPDATA/ongoing/scripts

#. $SCRIPTS_DIR/load_${1}_datastage_params.ksh
#. $SCRIPTS_DIR/load_library.ksh




export SCRIPT_NAME=$(basename  $0)
export SCRIPT_VERSION="1.0"

export File1="WhatCounts"
export File2="REIDIST"
export File3="REICRM"
export File4="REIGCREC"
export File5="REIRQ132"
export File6="attribution_offline_sales"
export File7="x320.rei1.t800"
export File8="responsys_retail_feed"
export File9="AuditWorkForceFilesList"
export File10="Rejects_Article_GTIN_INFO_MERCH"
export File11="Rejects_Article_GTIN_INFO_SERVICE"
export File12="Rejects_Article_PI_Message_MERCH"
export File13="Rejects_Article_PI_Message_SERVICE"
pattern="[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]"



. $SCRIPTS_DIR/load_parameters.ksh
. $SCRIPTS_DIR/load_library.ksh
. $SCRIPTS_DIR/load_datastage_params.ksh


################################################################################################################

print_msg "0001 Deleting 7 older log for script $SCRIPT_NAME Version $SCRIPT_VERSION"
echo ''
   print_msg "	The following log files will be removed:"
echo ''
cd $SCRIPTS_DIR/log;

find -name "Cleanup*.log" -type f -mtime +7 -print;
find -name "Cleanup*.log" -type f -mtime +7 -exec rm -f {} \;

print_msg "0002 Processing started for script $SCRIPT_NAME Version $SCRIPT_VERSION"

print_msg "0003 Clean up process has been started for $RUN_DATE"

#################################################################################################################

print_msg "0004 Looking for files in $VENDOR_DIR/archive for $File1.txt older then 2 days"

echo ''
   print_msg "	The following files will be removed:"
echo ''

cd $VENDOR_DIR/archive;

find -name "$File1*" -type f -mtime +2 -print;
find -name "$File1*" -type f -mtime +2 -exec rm -f {} \;

echo ''
echo ''
#################################################################################################################


print_msg "0005 Looking for files in $SEQFILE_DIR for $File2*.csv files older then 2 weeks"

echo ''
   print_msg "	The following files will be removed:"
echo ''

cd $SEQFILE_DIR;

find -name "$File2*.csv" -type f -mtime +14 -print;
find -name "$File2*.csv" -type f -mtime +14 -exec rm -f {} \;
find -name "$File9*.txt" -type f -mtime +7 -print;
find -name "$File9*.txt" -type f -mtime +7 -exec rm -f {} \;

echo ''
echo ''

#################################################################################################################

print_msg "0006 Looking for files in $SEQFILE_DIR for $File3*.csv files older then 2 weeks"

echo ''
   print_msg "	The following files will be removed:"
echo ''

cd $SEQFILE_DIR;

find -name "$File3*.csv" -type f -mtime +14 -print;
find -name "$File3*.csv" -type f -mtime +14 -exec rm -f {} \;

echo ''
echo ''
#################################################################################################################

print_msg "0007 Looking for files in $GIFTCARD_FEED_DIR/current/ztransmit for $File4*.csv files older then 1 day"

echo ''
   print_msg "	The following files will be removed:"
echo ''

cd $GIFTCARD_FEED_DIR/current/ztransmit;

find -name "$File4*" -type f -mtime +1 -print;
find -name "$File4*" -type f -mtime +1 -exec rm -f {} \;

echo ''
echo ''

#################################################################################################################

print_msg "0008 Looking for files in $GIFTCARD_FEED_DIR/current/ztransmit for $File5*.csv files older then 2 day"

echo ''
   print_msg "	The following files will be removed:"
echo ''

cd $GIFTCARD_FEED_DIR/current/ztransmit;

find -name "$File5*" -type f -mtime +2 -print;
find -name "$File5*" -type f -mtime +2 -exec rm -f {} \;

echo ''
echo ''

#################################################################################################################

print_msg "0009 Looking for files in $VENDOR_DIR/archive for $File6.txt older then 14 days"

echo ''
   print_msg "	The following files will be removed:"
echo ''

cd $VENDOR_DIR/archive;

find -name "$File6*" -type f -mtime +14 -print;
find -name "$File6*" -type f -mtime +14 -exec rm -f {} \;

echo ''
echo ''
#################################################################################################################

print_msg "0010 Looking for files in $VENDOR_DIR/archive for $File7 older then 10 days"

echo ''
   print_msg "	The following files will be removed:"
echo ''

cd $VENDOR_DIR/archive;

find -name "$File7*" -type f -mtime +10 -print;
find -name "$File7*" -type f -mtime +10 -exec rm -f {} \;

echo ''
echo ''
#################################################################################################################


print_msg "0011 Looking for files in $VENDOR_DIR/archive for $File8 older then 2 days"

echo ''
   print_msg "	The following files will be removed:"
echo ''

cd $VENDOR_DIR/archive;

find -name "$File8*" -type f -mtime +2 -print;
find -name "$File8*" -type f -mtime +2 -exec rm -f {} \;

echo ''
echo ''
#################################################################################################################

print_msg "0012 Deleting log files older than 7 days in $SCRIPTS_DIR/log having naming pattern as LOAD_SRC_table_v2.ksh*_nzload.date.log"
echo ''
   print_msg "	The following log files will be removed:"
echo ''
cd $SCRIPTS_DIR/log;


find -maxdepth 1 -name "LOAD_SRC_table_v2.ksh*_nzload.$pattern.log" -type f -mtime +7 -print;
find -maxdepth 1 -name "LOAD_SRC_table_v2.ksh*_nzload.$pattern.log" -type f -mtime +7 -exec rm -f {} \;

echo ''
print_msg "0003 Clean up process for LOAD_SRC_table_v2.ksh*_nzload.date.log files have completed for $RUN_DATE"
echo ''
echo ''

#################################################################################################################

print_msg "0013 Deleting bad files older than 7 days in $SCRIPTS_DIR/log having naming pattern as LOAD_SRC_table_v2.ksh*_nzload.date.bad"
echo ''
   print_msg "	The following bad files will be removed:"
echo ''
cd $SCRIPTS_DIR/log;

find -maxdepth 1 -name "LOAD_SRC_table_v2.ksh*_nzload.$pattern.bad" -type f -mtime +7 -print;
find -maxdepth 1 -name "LOAD_SRC_table_v2.ksh*_nzload.$pattern.bad" -type f -mtime +7 -exec rm -f {} \;

echo ''
print_msg "0003 Clean up process for LOAD_SRC_table_v2.ksh*_nzload.date.bad files have completed for $RUN_DATE"

echo ''
echo ''

#################################################################################################################

print_msg "0013 Deleting product merch and service files $File14*.csv , $File15*.csv and $File16*.csv in  $REJECT_DIR  older than 14 days"
echo ''
   print_msg "  The following files will be removed:"
echo ''
cd $REJECT_DIR;

find -name "$File10*.csv" -type f -mtime +14 -print;
find -name "$File10*.csv" -type f -mtime +14 -exec rm -f {} \;
find -name "$File11*.csv" -type f -mtime +14 -print;
find -name "$File11*.csv" -type f -mtime +14 -exec rm -f {} \;
find -name "$File12*.csv" -type f -mtime +14 -print;
find -name "$File12*.csv" -type f -mtime +14 -exec rm -f {} \;
find -name "$File13*.csv" -type f -mtime +14 -print;
find -name "$File13*.csv" -type f -mtime +14 -exec rm -f {} \;

echo ''
print_msg "0003 Clean up process for product merch and service reject files in RejectDir folder have completed for $RUN_DATE"

echo ''
echo ''

#################################################################################################################



print_msg "9999 Processing completed NORMALLY for $SCRIPT_NAME"
