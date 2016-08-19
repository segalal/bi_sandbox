#!/bin/ksh
######################################################################
# script :      customer_messaging_process_to_stgrdw_ecoa_and_profile_parse.ksh 
# description : This script calls customer_messaging_process_to_stgrdw_insert_parse.ksh 
#               to insert SRC_CUSTOMER_MESSAGING contact event data (15 types) into STGRDW parsed tables 
#
#               Script linage:
#
#                  Tidal Group BI_CDM_RSYS_CUSTOMER_MESSAGING_RETRIEVE
#                     Tidal Job 0001_BI_CDM_RSYS_CUSTOMER_MESSAGING_SFTP
#                        SFTP process responsys_in rsys *_<BICDM_Feed_Date.102.yyyymmdd>_*
#                     Tidal Job 0002_BI_CDM_RSYS_CUSTOMER_MESSAGING_RTM_SFTP
#                        SFTP process reirtm_in rsys  RTM_*_<BICDM_Feed_Date.102.yyyymmdd>*
#
#                  Tidal Group BI_CDM_RSYS_CUSTOMER_MESSAGING_STAGE
#                     Tidal Job 0001_BI_CDM_RSYS_CUSTOMER_MESSAGING_Stage
#                        UNIX script customer_messaging_rsys_copy.ksh
#
#                  Tidal Group BI_CDM_RSYS_CUSTOMER_MESSAGING_ARCHIVE
#                     Tidal Job 0001_BI_CDM_RSYS_CUSTOMER_MESSAGING_FILES
#                        UNIX script load_src_customer_messaging_feed_tables.ksh
#                           UNIX script load_src_rsys_customer_messaging_contact_event.ksh
#                           UNIX script load_src_rsys_customer_messaging_rtm_event.ksh
#                           UNIX script load_src_rsys_customer_messaging_ecoa_and_profile.ksh
#
#                  Tidal Group BI_CDM_CUSTOMER_MESSAGING_RDW_GROUP
#                     Tidal Job 0001_BI_CDM_PROCESS_TO_STGRDW_CUSTOMER_MESSAGING
#                        UNIX script customer_messaging_process_to_stgrdw.ksh
#                           UNIX script customer_messaging_process_to_stgrdw_contact_event_parse.ksh
#                           UNIX script customer_messaging_process_to_stgrdw_rtm_event_parse.ksh    
#                           UNIX script customer_messaging_process_to_stgrdw_ecoa_and_profile_parse.ksh   <================
#
#                              UNIX script customer_messaging_process_to_stgrdw_insert_parse.ksh
#
#                     Tidal Job 0002_BI_CDM_PROCESS_TO_RDW_CUSTOMER_MESSAGING
#                        UNIX script customer_messaging_process_to_stgrdw.ksh
#
#                  Tidal Group BI_CDM_CUSTOMER_MESSAGING_CDM_GROUP
#                     Tidal Job 0001_BI_CDM_PROCESS_TO_STGCDM_CUSTOMER_MESSAGING
#                        UNIX script customer_messaging_process_to_stgcdm.ksh
#
#                     Tidal Job 0002_BI_CDM_PROCESS_TO_CDM_CUSTOMER_MESSAGING
#                        UNIX script customer_messaging_process_to_cdm.ksh
#
# Modifications :
# 1.0  2013-Feb-10  asegal : initial creation of script
#                            PRJ10696 Preference Center and Transactional Email project - replacing Epsilon with Responsys
#      2013-Feb-14  asegal : Older version LOAD_SCR_table.ksh was used to Load the ECOA and Profile data into the old SRC tables
#                            rather than LOAD_SCR_table_v2.ksh.  This is because the "\n" code was removed from LOAD_SCR_table_v2.ksh
#                            and some special characters caused failure.  The older version of the script handled this.
# 2.0  2015-Jun-25  smohamm: Introduced truncate load of staging tables.
#
#######################################################################

export SCRIPT_NAME=$(basename  $0)
export SCRIPT_VERSION="1.0"

. $SCRIPTS_DIR/load_parameters.ksh
. $SCRIPTS_DIR/load_library.ksh
. $SCRIPTS_DIR/load_datastage_params.ksh

# Uncomment the following if testing against the ETL% dev databases
#export NZ_HIST_DB=etlhist
#export NZ_FEEDTEMP_DB=etlstg

export process_name="customer_messaging_process_to_stgrdw_ecoa_and_profile_parse"
export pred_jobname="load_src_rsys_customer_messaging_ecoa_and_profile"

export processing_timestamp=`date +%Y-%m-%d\ %H:%M:%S`
export script_return_code=0
export quote="'"

print_msg "0001 Processing started for script $SCRIPT_NAME Version $SCRIPT_VERSION.  Processing timestamp is $processing_timestamp"

print_msg "0002 Note that ECOA and PROFILE processing uses the old V1 LOAD script which does not have the job_run handling"


print_msg "0003 Parsing for ECOA and Profile files (2)" 

export source="ecoa"

   export parm_target_table="CUSTOMER_MESSAGING_"$source"_PARSE"
   export parm_src_table="SRC_EMAIL_ECOA_DAILY"

   export job_name=$process_name"."$parm_target_table
   export JOB_RUN_ID=`perl createJobRun.pl "$job_name" "$SCRIPT_VERSION"`

   print_msg "0010 Parsing of Table $parm_src_table started for source $source with job_run_id=$JOB_RUN_ID and $job_name"

   export active_ind=1

# Truncating the staging table 

	print_msg "Truncating the stating table $parm_target_table"

	truncate_table -d $NZ_STGRDW_DB -t $parm_target_table
	


   SELECT_STATEMENT="SELECT MAX(M_ACTIVE_IND) 
                     FROM $NZ_HIST_DB.$NZ_HIST_SCHEMA.V_SRC_CUSTOMER_MESSAGING_ECOA_PARSE"
   
   INSERT_STATEMENT="INSERT INTO $parm_target_table
                       SELECT CAST(COALESCE((SELECT MAX(CUSTOMER_MESSAGING_ECOA_PARSE_KEY) FROM CUSTOMER_MESSAGING_ECOA_PARSE),0) + RANK() OVER (ORDER BY FILE_LOCATION, DATE_TIME) AS INTEGER) 
   		        	AS CUSTOMER_MESSAGING_ECOA_PARSE_KEY,
                       E.OLD_EMAIL_ADDR,
                       E.NEW_EMAIL_ADDR,
                       E.DATE_TIME,
                       E.TRANSMIT_DATETIME,
                       E.FILE_LOCATION,
                       S.SOURCE_DATA_KEY,
                       '$processing_timestamp' AS EFF_START_DATETIME,
                       $JOB_RUN_ID,
                       '$processing_timestamp' AS M_INSERT_DATETIME,
                       '$processing_timestamp' AS M_LAST_UPDATE_DATETIME
                     FROM $NZ_HIST_DB.$NZ_HIST_SCHEMA.V_SRC_CUSTOMER_MESSAGING_ECOA_PARSE E,
                          $NZ_METAMART_DB.$NZ_METAMART_SCHEMA.SOURCE_DATA S
                     WHERE UPPER(E.DATE_TIME) <> 'DATETIME'
                     AND E.FILE_LOCATION NOT LIKE '%ECOAdaily_REIclient%'
                     AND S.SOURCE_NAME LIKE '%ecoa%'
                     AND E.M_INSERT_DATETIME = (SELECT MIN(M_INSERT_DATETIME)
                                              FROM $NZ_HIST_DB.$NZ_HIST_SCHEMA.$parm_src_table
                                              WHERE M_ACTIVE_IND = 1) 

                     ORDER BY E.FILE_LOCATION, E.DATE_TIME"

   echo "INSERT_STATEMENT="
   echo $INSERT_STATEMENT
   echo ""

   while [[ $active_ind = 1 && $script_return_code = 0 ]]  
   do 

      export SELECT_CURRENTLY_PROCESSING="SELECT MAX(FILE_LOCATION)  
                                          FROM $NZ_HIST_DB.$NZ_HIST_SCHEMA.$parm_src_table
                                          WHERE M_INSERT_DATETIME = (SELECT MIN(M_INSERT_DATETIME)
                                                                     FROM $NZ_HIST_DB.$NZ_HIST_SCHEMA.$parm_src_table
                                                                     WHERE M_ACTIVE_IND = 1)"
 
      export CURRENTLY_PROCESSING=$(nzsql -d $NZ_HIST_DB -A -t -c "$SELECT_CURRENTLY_PROCESSING")

      echo "Looping for unprocessed rows for $parm_target_table for $CURRENTLY_PROCESSING"
 
      run_query -d $NZ_STGRDW_DB -q "$INSERT_STATEMENT" -m "0011 ===> ERROR <=== Insert into table $parm_target_table failed.  Script $SCRIPT_NAME and Process $process_name finished UNSUCCESSFULLY" 
      export script_return_code=$?
      if [[ $script_return_code != 0 ]]
      then
         print_msg "0012 ===> ERROR <=== Insert into table $parm_target_table failed.  Script $SCRIPT_NAME and Process $process_name finished UNSUCCESSFULLY."
         perl endJobRun.pl "$JOB_RUN_ID" "FAILURE"
      else
        UPDATE_STATEMENT="UPDATE $NZ_HIST_DB.$NZ_HIST_SCHEMA.$parm_src_table
                          SET  M_ACTIVE_IND = 0,
                               M_LAST_UPDATE_DATETIME = '$processing_timestamp'
                          WHERE M_INSERT_DATETIME =  (SELECT MIN(M_INSERT_DATETIME)
                                                      FROM $NZ_HIST_DB.$NZ_HIST_SCHEMA.$parm_src_table
                                                      WHERE M_ACTIVE_IND = 1)"

        run_query -d $NZ_HIST_DB -q "$UPDATE_STATEMENT" -m "0013 ===> ERROR <=== Update of table $parm_src_table failed.  Script $SCRIPT_NAME finished UNSUCCESSFULLY"
        export script_return_code=$?
      fi

      export active_ind=$(nzsql -d $NZ_HIST_DB -A -t -c "$SELECT_STATEMENT")

   done

   if [[ $script_return_code != 0 ]]
   then
      print_msg "0014 ===> ERROR <=== The update of the SRC m_active_ind failed for $parm_src_table.  Script $SCRIPT_NAME and Process $process_name finished UNSUCCESSFULLY."
      perl endJobRun.pl "$JOB_RUN_ID" "FAILURE"
   else 
      perl endJobRun.pl "$JOB_RUN_ID" "SUCCESS" "${predjobs[@]}"
      print_msg "0015 Parsing of Table $source was successful"
   fi
   echo "" 


export source="profile"

   export parm_target_table="CUSTOMER_MESSAGING_"$source"_PARSE"
   export parm_src_table="SRC_EMAIL_PROFILE_UPDATE"

   export job_name=$process_name"."$parm_target_table
   export JOB_RUN_ID=`perl createJobRun.pl "$job_name" "$SCRIPT_VERSION"`

   print_msg "0020 Parsing of Table $parm_src_table started for source $source with job_run_id=$JOB_RUN_ID and $job_name"

   export active_ind=1

   SELECT_STATEMENT="SELECT MAX(M_ACTIVE_IND)
                     FROM $NZ_HIST_DB.$NZ_HIST_SCHEMA.$parm_src_table"

   INSERT_STATEMENT="INSERT INTO $parm_target_table
		       SELECT CAST(COALESCE((SELECT MAX(CUSTOMER_MESSAGING_PROFILE_PARSE_KEY) FROM CUSTOMER_MESSAGING_PROFILE_PARSE),0) + RANK() OVER (ORDER BY FILE_LOCATION) AS INTEGER) 
					AS CUSTOMER_MESSAGING_PROFILE_PARSE_KEY,
		       E.EMAIL_ADDR,
                       E.MEMBER_ID,
		       E.FIRST_NAME,
		       E.LAST_NAME,
		       E.CLIENT_UNSUBSCRIBE,
		       E.VALID_EMAIL_ADDR,
		       E.SUPPRESSED,
		       E.ADDRESS_1,
		       E.ADDRESS_2,
		       E.CITY,
		       E.STATE,
		       E.ZIP,
		       E.COUNTRY,
		       E.MEMBER_NUMBER,
		       E.CAMPING_HIKING,
		       E.CLIMBING,
		       E.CYCLING,
		       E.FOOTWEAR,
		       E.XTRAINING,
		       E.CLOTHING_MENS,
		       E.CLOTHING_WOMENS,
         	       E.CLOTHING_KIDS,
		       E.FISHING,
		       E.ENV_GIVING,
		       E.SNOW_SPORTS,
		       E.TRAVEL,
		       E.PADDING,
		       E.PREFFERED_STORE,
		       E.COUPON,
		       E.CLIENT_FIRST_JOIN_DATE,
		       E.EMAIL_ADDR_ID,
		       E.REI_UNSUBSCRIBE,
		       E.REI_SOURCE_CODE,
		       E.COOP_MEMBERSHIP_UNSUBSCRIBE,
         	       E.COOP_MEMBERSHIP_SOURCE_CODE,
		       E.RETAIL_UNSUBSCRIBE,
		       E.RETAIL_COURCE_CODE,
		       E.REI_MARKETING,
		       E.RETAIL_MASTER,
		       E.REI_MEMBERS,
                       E.TRANSMIT_DATETIME,
                       E.FILE_LOCATION,
                       S.SOURCE_DATA_KEY,
                       '$processing_timestamp' AS EFF_START_DATETIME,
                       $JOB_RUN_ID,
                       '$processing_timestamp' AS M_INSERT_DATETIME,
                       '$processing_timestamp' AS M_LAST_UPDATE_DATETIME
                     FROM $NZ_HIST_DB.$NZ_HIST_SCHEMA.V_SRC_CUSTOMER_MESSAGING_PROFILE_PARSE E,
                          $NZ_METAMART_DB.$NZ_METAMART_SCHEMA.SOURCE_DATA S
                     WHERE UPPER(E.EMAIL_ADDR) <> 'EMAIL_ADDR'
                     AND E.FILE_LOCATION NOT LIKE '%profileupdate%'
                     AND S.SOURCE_NAME LIKE '%profile%'
                     AND E.M_INSERT_DATETIME = (SELECT MIN(M_INSERT_DATETIME)
                                              FROM $NZ_HIST_DB.$NZ_HIST_SCHEMA.$parm_src_table
                                              WHERE M_ACTIVE_IND = 1) 
                     ORDER BY E.FILE_LOCATION"

   echo "INSERT_STATEMENT="
   echo $INSERT_STATEMENT
   echo ""

   while [[ $active_ind = 1 && $script_return_code = 0 ]]
   do

      export SELECT_CURRENTLY_PROCESSING="SELECT MAX(FILE_LOCATION)
                                          FROM $NZ_HIST_DB.$NZ_HIST_SCHEMA.$parm_src_table
                                          WHERE M_INSERT_DATETIME = (SELECT MIN(M_INSERT_DATETIME)
                                                                     FROM $NZ_HIST_DB.$NZ_HIST_SCHEMA.$parm_src_table
                                                                     WHERE M_ACTIVE_IND = 1)"

      export CURRENTLY_PROCESSING=$(nzsql -d $NZ_HIST_DB -A -t -c "$SELECT_CURRENTLY_PROCESSING")

      echo "Looping for unprocessed rows for $parm_target_table for $CURRENTLY_PROCESSING"

      run_query -d $NZ_STGRDW_DB -q "$INSERT_STATEMENT" -m "0021 ===> ERROR <=== Insert into table $parm_target_table failed.  Script $SCRIPT_NAME and Process $process_name finished UNSUCCESSFULLY"
      export script_return_code=$?
      if [[ $script_return_code != 0 ]]
      then
         print_msg "0022 ===> ERROR <=== Insert into table $parm_target_table failed.  Script $SCRIPT_NAME and Process $process_name finished UNSUCCESSFULLY."
         perl endJobRun.pl "$JOB_RUN_ID" "FAILURE"
      else
         UPDATE_STATEMENT="UPDATE $NZ_HIST_DB.$NZ_HIST_SCHEMA.$parm_src_table
                         SET  M_ACTIVE_IND = 0,
                              M_LAST_UPDATE_DATETIME = '$processing_timestamp'
                         WHERE M_INSERT_DATETIME =  (SELECT MIN(M_INSERT_DATETIME)
                                                     FROM $NZ_HIST_DB.$NZ_HIST_SCHEMA.$parm_src_table
                                                     WHERE M_ACTIVE_IND = 1)"

         run_query -d $NZ_HIST_DB -q "$UPDATE_STATEMENT" -m "0023 ===> ERROR <=== Update of table $parm_src_table failed.  Script $SCRIPT_NAME finished UNSUCCESSFULLY"
         export script_return_code=$?
      fi

      export active_ind=$(nzsql -d $NZ_HIST_DB -A -t -c "$SELECT_STATEMENT")

   done

   if [[ $script_return_code != 0 ]]
   then
        print_msg "0024 ===> ERROR <=== The update of the SRC m_active_ind failed for $parm_src_table.  Script $SCRIPT_NAME and Process $process_name finished UNSUCCESSFULLY."
        perl endJobRun.pl "$JOB_RUN_ID" "FAILURE"
   else
        perl endJobRun.pl "$JOB_RUN_ID" "SUCCESS" "${predjobs[@]}"
        print_msg "0025 Parsing of Table $source was successful"
   fi
   echo ""


if [[ $script_return_code != 0 ]]
then
   print_msg "9999 *****ERROR**** Processing Completed Abnormally for $SCRIPT_NAME with Return Code="$script_return_code
else
   print_msg "9999 Processing Completed Normally for $SCRIPT_NAME"

fi

echo ""

print_msg "==> WARNING:  Even if there was a failure during processing, this script will not cause Tidal To fail because these two files do not really need to be parsed.  This is just a nice-to-have <=="
#exit $script_return_code
exit 0

# Finish
