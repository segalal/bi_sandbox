#! /bin/ksh
###################################################################################
# Create the acx_validation_activity.ksh
# Purpose: Validate the count of records in build and rejects inbound file against value in control file
# Modifications :
# 1.0  2015-Sept-19  Infosys : Creation of Script
####################################################################################

ONGOING_DIR=/bicc_data/workspace/dstage/data/3CPDATA/ongoing
. $SCRIPTS_DIR/load_parameters.ksh
. $SCRIPTS_DIR/load_library.ksh
. $SCRIPTS_DIR/load_datastage_params.ksh
. $SCRIPTS_DIR/cde_env.ksh
curr_dt=`date`
echo "*****************************************************************************************" >>$SCRIPTS_DIR/log/acx_validation_activity.log
echo "*****************Started acx_validation_activity at "$curr_dt"***************" >>$LOG_DIR/acx_validation_activity.log


#****************************************************************#
# count of ACX_CUST_IN file 
#****************************************************************#
cd $ONGOING_DIR/dgraphic/current/zarchive
filename=`ls acxiom_to_rei_build_*.txt`
wc_rec_recvd=`wc -l<$ONGOING_DIR/dgraphic/current/zarchive/$filename`
echo $wc_rec_recvd>>$SCRIPTS_DIR/log/acx_validation_activity.log


#****************************************************************#
# count of ACX_CUST_REJECTS file 
#****************************************************************#
wc_rejects_recvd=`wc -l<$ONGOING_DIR/dgraphic/current/zarchive/rejects_acxiom_to_rei_build`
echo $wc_rejects_recvd>>$SCRIPTS_DIR/log/acx_validation_activity.log
#****************************************************************#
# count of ACX_CUST_IN file as per control file
#****************************************************************#
query="SELECT SUBSTR(T.RAW_COL1,INSTR(T.RAW_COL1,'value=')+7,CAST(INSTR(T.RAW_COL1,'/>')-1 AS INT) - CAST(INSTR(T.RAW_COL1,'value=')+7 AS INT))FROM
(SELECT RAW_COL1,row_number() over( partition by SD.source_data_key order by line_number)
line_no from $NZ_HIST_DB..SRC_ACX_CUSTOMER_DEMOGRAPHICS CUSTD INNER JOIN $NZ_METAMART_DB..SOURCE_DATA SD
ON SD.SOURCE_DATA_KEY =CUSTD.SOURCE_DATA_KEY WHERE SD.SOURCE_DATA_DESC='Acxiom source file for Control file' AND JOB_RUN_ID
IN (
SELECT MAX(JOB_RUN_ID) FROM $NZ_METAMART_DB..JOB_RUN WHERE JOB_NAME =
'load_src_cde_customer_demographics_control_file.CONTROL_FILE'
AND PROCESSING_STATUS = 'SUCCESS'))T where line_no=5"

REC_RECVD=$(nzsql -d $NZ_HIST_DB -A -t -c "$query")
echo $REC_RECVD>>$SCRIPTS_DIR/log/acx_validation_activity.log


# to handle in case of refresh file
if [[ "$REC_RECVD" == "" ]]
then 
query="SELECT SUBSTR(T.RAW_COL1,INSTR(T.RAW_COL1,'value=')+7,CAST(INSTR(T.RAW_COL1,'/>')-1 AS INT) - CAST(INSTR(T.RAW_COL1,'value=')+7 AS INT))FROM
(SELECT RAW_COL1,row_number() over( partition by SD.source_data_key order by line_number)
line_no from $NZ_HIST_DB..SRC_ACX_CUSTOMER_DEMOGRAPHICS CUSTD INNER JOIN $NZ_METAMART_DB..SOURCE_DATA SD
ON SD.SOURCE_DATA_KEY =CUSTD.SOURCE_DATA_KEY WHERE SD.SOURCE_DATA_DESC='Acxiom source file for Control file' AND JOB_RUN_ID
IN (
SELECT MAX(JOB_RUN_ID) FROM $NZ_METAMART_DB..JOB_RUN WHERE JOB_NAME =
'load_src_cde_customer_demographics_control_file.CONTROL_FILE'
AND PROCESSING_STATUS = 'SUCCESS'))T where line_no=3"

REC_RECVD=$(nzsql -d $NZ_HIST_DB -A -t -c "$query")
echo "Refresh file received this time:Count of record received: "$REC_RECVD>>$SCRIPTS_DIR/log/acx_validation_activity.log

fi



#****************************************************************#
# count of ACX_CUST_REJECTS file as per control file
#****************************************************************#
query="SELECT SUBSTR(T.RAW_COL1,INSTR(T.RAW_COL1,'value=')+7,CAST(INSTR(T.RAW_COL1,'/>')-1 AS INT) - CAST(INSTR(T.RAW_COL1,'value=')+7 AS INT))FROM
(SELECT RAW_COL1,row_number() over( partition by SD.source_data_key order by line_number)
line_no from $NZ_HIST_DB..SRC_ACX_CUSTOMER_DEMOGRAPHICS CUSTD INNER JOIN $NZ_METAMART_DB..SOURCE_DATA SD
ON SD.SOURCE_DATA_KEY =CUSTD.SOURCE_DATA_KEY WHERE SD.SOURCE_DATA_DESC='Acxiom source file for Control file' AND JOB_RUN_ID
IN (
SELECT MAX(JOB_RUN_ID) FROM $NZ_METAMART_DB..JOB_RUN WHERE JOB_NAME =
'load_src_cde_customer_demographics_control_file.CONTROL_FILE'
AND PROCESSING_STATUS = 'SUCCESS'))T where line_no=6"

REJCT_REC_RECVD=$(nzsql -d $NZ_HIST_DB -A -t -c "$query")
echo $REJCT_REC_RECVD>>$SCRIPTS_DIR/log/acx_validation_activity.log

# to handle rejects file not available in case of refresh file
if [[ "$REJCT_REC_RECVD" == "" ]]
then 
REJCT_REC_RECVD=0
echo "Record Count of empty reject file created for refresh: "$REJCT_REC_RECVD>>$SCRIPTS_DIR/log/acx_validation_activity.log
fi

mv $ONGOING_DIR/dgraphic/current/zwork/acxiom_to_rei_build_rejects_* $ONGOING_DIR/dgraphic/current/zarchive
rm -f $ONGOING_DIR/dgraphic/current/zarchive/rejects_acxiom_to_rei_build


#****************************************************************#
# Comparing the counts and if not matched fail the script
#****************************************************************#
touch $SCRIPTS_DIR/in_body.txt

if [[ $REJCT_REC_RECVD == $wc_rejects_recvd  && $REC_RECVD == $wc_rec_recvd ]]

then echo "0">>$SCRIPTS_DIR/log/acx_validation_activity.log
SUBJECT="Customer Data Enrichment Inbound Files Record Count Comparison Successful" 
   echo "The Inbound file counts is as below">>$SCRIPTS_DIR/in_body.txt
   
   echo "-------------------------------------------------------------------------------------------------------------------">>$SCRIPTS_DIR/in_body.txt
   echo "RECORDS IN FILE RECIEVED:  "$wc_rec_recvd" RECORD COUNT IN CONTROL FILE:  "$REC_RECVD>>$SCRIPTS_DIR/in_body.txt
   echo "  ">>$SCRIPTS_DIR/in_body.txt
   echo "RECORDS IN REJECT FILE  :  "$wc_rejects_recvd" REJECT COUNT IN CONTROL FILE:  "$REJCT_REC_RECVD>>$SCRIPTS_DIR/in_body.txt
   echo "-------------------------------------------------------------------------------------------------------------------">>$SCRIPTS_DIR/in_body.txt
   echo "Thanks,">>$SCRIPTS_DIR/in_body.txt
   echo "ETL Team ">>$SCRIPTS_DIR/in_body.txt
 
   mail -s "${SUBJECT}" -c ${CC_LIST}  ${EMAIL_DL1} <$SCRIPTS_DIR/in_body.txt

rm $SCRIPTS_DIR/in_body.txt

exit 0

else
echo "3">>$SCRIPTS_DIR/log/acx_validation_activity.log
SUBJECT="Customer Data Enrichment Inbound Files Record Count Comparison Failed" 
   echo "The Inbound file counts not matching with control file data,the current load will be discontinued">>$SCRIPTS_DIR/in_body.txt
   
   echo "---------------------------------------------------------------------------------------------------------------">>$SCRIPTS_DIR/in_body.txt
   echo "RECORDS IN FILE RECIEVED:  "$wc_rec_recvd" RECORD COUNT IN CONTROL FILE:  "$REC_RECVD>>$SCRIPTS_DIR/in_body.txt
   echo "  ">>$SCRIPTS_DIR/in_body.txt
   echo "RECORDS IN REJECT FILE  :  "$wc_rejects_recvd" REJECT COUNT IN CONTROL FILE:  "$REJCT_REC_RECVD>>$SCRIPTS_DIR/in_body.txt
   echo "---------------------------------------------------------------------------------------------------------------">>$SCRIPTS_DIR/in_body.txt
   echo "Thanks,">>$SCRIPTS_DIR/in_body.txt
   echo "ETL Team ">>$SCRIPTS_DIR/in_body.txt
mail -s "${SUBJECT}" -c ${CC_LIST}  ${EMAIL_DL1} <$SCRIPTS_DIR/in_body.txt

rm $SCRIPTS_DIR/in_body.txt
cp $ONGOING_DIR/dgraphic/current/zarchive/acxiom_to_rei_build_rejects_* $ONGOING_DIR/dgraphic/current/zarchive/rejects_acxiom_to_rei_build
mv $ONGOING_DIR/dgraphic/current/zarchive/acxiom_to_rei_build_rejects_* $ONGOING_DIR/dgraphic/current/zwork
exit 3
fi