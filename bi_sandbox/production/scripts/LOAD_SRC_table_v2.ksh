#!/bin/ksh
######################################################################
# script :  LOAD_SRC_table_v2.ksh
# description : This script performs the initial load of feed file data 
#               fme_zippedrom flatfiles residing at /workspace/dstage/data/3CPDATA/ongoing/* 
#               to table SRC tables
#
# Modifications :
# 1.0  2011-Aug-19  REI asegal : Creation of Script
# 1.1  2012-Jul-17  REI raj :    Modified to include TABLE feed source format
# 2.0  2012-Jul-23  REI migreen: Changed into a driving script that calls functions
#                                Existing File and Table code has been moved into load_src_functions.ksh.
# 2.1 2013-Jan-23   asegal :     Added multiple_feed_files parameter to allow expection of 
#                                multiple files to be honored or denied.  
#                                Added feed_file_expected_ind parameter to force an error if no feed file
#                                is present when one is expected.  Build 42 ETM Rejects files.  Added SCRIPT_VERSION
#
#######################################################################

export SCRIPT_NAME=$(basename  $0)
export SCRIPT_VERSION="2.1"

. $SCRIPTS_DIR/load_parameters.ksh
. $SCRIPTS_DIR/load_library.ksh
. $SCRIPTS_DIR/load_datastage_params.ksh
. $SCRIPTS_DIR/load_src_functions.ksh 

# Uncomment the following if testing against the ETL% dev databases
##export NZ_HIST_DB=etlhist
##export NZ_FEEDTEMP_DB=etlstg

# Initialize parameter variables
export feed_target_table=""
export feed_area=""
export feed_source_format=""
export feed_source_name=""
export feed_file_name_zipped=""
export feed_file_zip_mode=""
export feed_file_delim=""
export feed_file_zipped_ind=""
export feed_catchup_location=""
export feed_file_remove_ind=""
#export feed_encryption_ind=""
export etl_run_id=""
export debug_ind=""
export lineno_delimiter=""
export max_error_number=30
export ignore_zero="no"
export multiple_feed_files_ind=""
export feed_file_expected_ind=""


########################
# Get script parameters#
######################## 

while getopts "t:a:s:f:o:m:b:z:c:r:i:d:l:e:0:n:g:x" arg
do
  case $arg in
	 t ) feed_target_table=$OPTARG;;

	 a ) feed_area=$OPTARG;;

	 s ) feed_source_format=$OPTARG;;

	 f ) feed_source_name=$OPTARG;;

	 o ) feed_file_name_zipped=$OPTARG;;

	 m ) feed_file_zip_mode=$OPTARG;;

	 b ) feed_file_delim=$OPTARG;;

	 z ) feed_file_zipped_ind=$OPTARG;;

	 c ) feed_catchup_location=$OPTARG;;

	 r ) feed_file_remove_ind=$OPTARG;;
	 	 
	 i ) etl_run_id=$OPTARG;;

	 d ) debug_ind=$OPTARG;;

         l ) lineno_delimiter=$OPTARG;;

         e ) max_error_number=$OPTARG;;

         0 ) ignore_zero=$OPTARG;;

         n ) multiple_feed_files_ind=$OPTARG;;

         g ) missing_feed_files_permitted_ind=$OPTARG;;

	\?)  print 'LOAD_SRC_table usage: -t feed_target_table -a feed_area -s feed_source_format -f feed_source_name -o feed_file_name_zipped -m feed_file_zip_mode -b feed_file_delim -z feed_file_zipped_ind -c feed_catchup_location -r feed_file_remove_ind -i etl_run_id -d debug_ind -l lineno_delimiter -e max_error_number -0 ignore_zero -n multiple_feed_files_ind -g missing_feed_files_permitted_ind'
  
  esac
done

export SCRIPT_NAME=$(basename  $0)"_"$feed_area
echo ""

if [[ $debug_ind != "DEBUG" ]]
then
  export debug_ind="NO_DEBUG"
fi

debug $debug_ind "Debugging is turned on"

echo ""
print_msg "0001 Processing started for script $SCRIPT_NAME Version $SCRIPT_VERSION"

print_msg "Processing will occur in databases $NZ_HIST_DB and $NZ_FEEDTEMP_DB for table $feed_target_table"

# Check to ensure that the required parameters is present
if [[ $feed_target_table == "" ]]
then
  chk_err -r 3 -m "0001 No Target Table was passed to $SCRIPT_NAME"
  exit 3
fi
if [[ $etl_run_id == "" ]]
then
  chk_err -r 3 -m "0002 No ETL Run ID was passed to $SCRIPT_NAME"
  exit 3
fi
if [[ $feed_source_name == *\'* ]]
then
  chk_err -r 3 -m "0003 feed_source_name parameter contains a quote"
  exit 3
fi


######################################################
# Set variables to NOT_PROVIDED if not in parameters #
######################################################
if [[ $feed_area == "" ]]
then 
  export feed_area="NOT_PROVIDED"
fi

if [[ $feed_source_name == "" ]]
then 
  export feed_source_name="NOT_PROVIDED"
fi

if [[ $feed_file_name_zipped == "" ]]
then 
  export feed_file_name_zipped="NOT_PROVIDED"
fi

if [[ $feed_file_zip_mode == "" ]]
then 
  export feed_file_zip_mode="NOT_PROVIDED"
fi

if [[ $feed_file_delim == "" ]]
then 
  export feed_file_delim="NOT_PROVIDED"
fi

if [[ $feed_file_zipped_ind == "" ]]
then 
  export feed_file_zipped_ind="NOT_PROVIDED"
fi

if [[ $feed_catchup_location == "" ]]
then 
  export feed_catchup_location="NOT_PROVIDED"
fi

if [[ $feed_file_remove_ind == "" ]]
then 
  export feed_file_remove_ind="NOT_PROVIDED"
fi

if [[ $lineno_delimiter == "" ]]
then 
  export lineno_delimiter="NOT_PROVIDED"
fi

if [[ $max_error_number == "" ]]
then 
  export max_error_number=1
fi

if [[ $ignore_zero == "" ]]
then 
  export ignore_zero="no"
fi

if [[ $multiple_feed_files_ind == "" ]]
then
  export multiple_feed_files_ind="NOT_PROVIDED"
fi

if [[ $missing_feed_files_permitted_ind == "" ]]
then
  export missing_feed_files_permitted_ind="NOT_PROVIDED"
fi


debug $debug_ind "Input parameters are:" "feed_target_table=$feed_target_table" "feed_area=$feed_area" "feed_source_format=$feed_source_format" "feed_source_name=$feed_source_name" "feed_file_name_zipped=$feed_file_name_zipped" "feed_file_zip_mode=$feed_file_zip_mode" "feed_file_delim=$feed_file_delim" "feed_file_zipped_ind=$feed_file_zipped_ind" "feed_catchup_location=$feed_catchup_location" "feed_file_remove_ind=$feed_file_remove_ind" "etl_run_id=$etl_run_id" "lineno_delimiter=$lineno_delimiter" "max_error_number=$max_error_number" "ignoreZero=$ignore_zero" "multiple_feed_files_ind=$multiple_feed_files_ind" "missing_feed_files_permitted_ind=$missing_feed_files_permitted_ind"
  
##########################################
# Lookup variable values from parameters #
##########################################

# Get the source data key from the table SOURCE_DATA based on the file name
export SOURCE_DATA_KEY=`nzsql -A -t -c "SELECT SOURCE_DATA_KEY FROM $NZ_METAMART_DB..SOURCE_DATA WHERE SOURCE_NAME = '$feed_source_name'"` 
#export ENCRYPTION_FLAG=`nzsql -A -t -c "SELECT MAX(SDC.ENCRYPTION_INDICATOR) FROM $NZ_METAMART_DB..SOURCE_DATA SD JOIN $NZ_METAMART_DB..SOURCE_DATA_COLUMNS SDC ON SD.SOURCE_DATA_KEY = SDC.SOURCE_DATA_KEY WHERE SOURCE_NAME = '$feed_source_name'"`
# Set the source data key to default if no record exists in the table SOURCE_DATA
if [[ $SOURCE_DATA_KEY == "" ]]
then 
  chk_err -r 3 -m "0004 SOURCE_NAME value = $feed_source_name was not found in the SOURCE_DATA table."
  exit 3
fi

debug $debug_ind "SOURCE DATA values are:" "SOURCE_DATA_KEY=$SOURCE_DATA_KEY" 
#"ENCRYPTION_FLAG=$ENCRYPTION_FLAG"

#if [[ $ENCRYPTION_FLAG == "" ]]
#then 
#  chk_err -r 3 -m "0005 encryption_indicator value not found in SOURCE_DATA_COLUMNS table."
#  exit 3
#fi  

# NEED TO ADD SPLITTING FUNCTIONALITY #
   
# Process data	
if [[ $feed_source_format = "TABLE" ]]
then
  echo ""
  print_msg "processing started for source type: table"
  fnLoadFromTable $feed_target_table $feed_source_name $SOURCE_DATA_KEY  $etl_run_id $debug_ind
elif [[ $feed_source_format = "STANDARD_INPUT" ]]
then
  echo ""
  print_msg "processing started for source type: standard input"
  fnLoadFromStdInput $feed_target_table $SOURCE_DATA_KEY $feed_source_name  $etl_run_id $debug_ind $lineno_delimiter $ignore_zero $max_error_number
else
  echo ""
  print_msg "processing started for source type: file"
  fnLoadFromFile  $feed_target_table $feed_area $feed_source_name $feed_file_name_zipped $feed_file_zip_mode $feed_file_delim $feed_file_zipped_ind $feed_catchup_location $feed_file_remove_ind $SOURCE_DATA_KEY  $etl_run_id $debug_ind $multiple_feed_files_ind $missing_feed_files_permitted_ind
fi

# Finish
echo ""
print_msg "9999 Processing completed successfully for script $SCRIPT_NAME "
echo ""


