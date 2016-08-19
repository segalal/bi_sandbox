#!/bin/ksh
######################################################################
# script :      clickstream_datafeed_ftp_delete.ksh 
# Description : This script will delete the source files from FTP server after processing.
# Created Date :2016-05-03
#######################################################################

export SCRIPT_NAME=""

export SCRIPT_VERSION=""

SCRIPT_NAME="$(basename $0)"
SCRIPT_VERSION="1.1"


export script_return_code=0
export return_code_fail=3
export source_dir="/bicc_data/workspace/dstage/data/3CPDATA/ongoing/adobe"

echo "0001 Processing started for script $SCRIPT_NAME Version $SCRIPT_VERSION"
echo "" 

echo "0002 Start processing for deleting Adobe datafeed files from Ftp server"
echo ""


cd /bicc_data/workspace/dstage/data/3CPDATA/ongoing/adobe/current/ztransmit

echo "Appending all files together with space between them for each raw files"

all_iphone_files=""
i=0
for line in `ls *reiapp1_*|sort -r`;
do  
if [ i == 0 ]; 
then all_iphone_files=$line; 
else all_iphone_files=$all_iphone_files' '$line; 
fi; 
i=`expr $i + 1`; 
done; 


all_android_files=""
i=0
for line in `ls *reiapp1android*|sort -r`;
do
if [ i == 0 ];
then all_android_files=$line;
else all_android_files=$all_android_files' '$line;
fi;
i=`expr $i + 1`;
done;


all_ipad_files=""
i=0
for line in `ls *reiapp3ipad*|sort -r`;
do
if [ i == 0 ];
then all_ipad_files=$line;
else all_ipad_files=$all_ipad_files' '$line;
fi;
i=`expr $i + 1`;
done;


all_rei_files=""
i=0
for line in `ls *reiprod*|sort -r`;
do
if [ i == 0 ];
then all_rei_files=$line;
else all_rei_files=$all_rei_files' '$line;
fi;
i=`expr $i + 1`;
done;


echo "Iphone Files to be deleted are:" $all_iphone_files
echo "Android Files to be deleted are:" $all_android_files
echo "Ipad Files to be deleted are:" $all_ipad_files
echo "Rei Files to be deleted are:" $all_rei_files

#SFTP to Omniture Server to clear the processed files

echo "Creating rm commands for removing files and write to a single file for each raw file"

for file in $all_iphone_files; do echo -e "-rm /reiapp1/$file" >> "$source_dir"/sftp_iphone_batch; done; 
sftp -b "$source_dir"/sftp_iphone_batch -o PasswordAuthentication=no rei_df_dropbox@ftp2.omniture.com
rm "$source_dir"/sftp_iphone_batch

for file in $all_android_files; do echo -e "-rm /reiapp1android/$file" >> "$source_dir"/sftp_android_batch; done; 
sftp -b "$source_dir"/sftp_android_batch -o PasswordAuthentication=no rei_df_dropbox@ftp2.omniture.com
rm "$source_dir"/sftp_android_batch

for file in $all_ipad_files; do echo -e "-rm /reiapp3ipad/$file" >> "$source_dir"/sftp_ipad_batch; done; 
sftp -b "$source_dir"/sftp_ipad_batch -o PasswordAuthentication=no rei_df_dropbox@ftp2.omniture.com
rm "$source_dir"/sftp_ipad_batch

for file in $all_rei_files; do echo -e "-rm /reiprod/$file" >> "$source_dir"/sftp_rei_batch; done; 
sftp -b "$source_dir"/sftp_rei_batch -o PasswordAuthentication=no rei_df_dropbox@ftp2.omniture.com
rm "$source_dir"/sftp_rei_batch


echo "0003 All files are deleted from source Ftp server"

echo "9999 Processing completed NORMALLY for $SCRIPT_NAME"

# Finish

