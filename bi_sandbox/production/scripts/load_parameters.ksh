#!/bin/ksh
###########################################################################
# script : load_parameters.ksh
# Description : Exports all the necessary parameters to run netezza scripts
#              ****Note make sure NZ_USER and NZ_PASSWORD variable are 
#                  defined in .bash_profile ********
# Modification
# 09-Mar-2011  Yomari                 Initial Script
# Sept-2011    Anna Segal             Added FEEDTEMP parameters
# Oct-2011     Rajesekhar Potteti     Added NZ_UNICABDB database and schema parameters
# 29-Aug-2012  Anna Segal             Added NIM and STGRDW parameters
# Jan-01-2015  Anna Segal             Added Sterling databases 
# Oct-17-2015  Sebastian Kuriakose    Modified Mako database name changes
# 2016-03-10   Juganu Chhabra         Adding SYNC_POS database as part of eReceipt Changes.
###########################################################################

###only required for users other than dsadm ###

export PATH=$PATH:/usr/local/nz/bin

#export ONGOING_DIR="/workspace/dstage/data/3CPDATA/ongoing"
#export SCRIPTS_DIR="$ONGOING_DIR/scripts"
export LOG_DIR="$SCRIPTS_DIR/log"

export NZ_HOST=netezza.rei.com

export NZ_FEEDTEMP_DB="FEEDTEMP"
export NZ_PRSNT_DB="CDME"
export NZ_STG_DB="STGCDM"
export NZ_HIST_DB="DI_HISTORY"
export NZ_RDW_DB="RDWP"
export NZ_UNICADB_DB="UNICADB"
export NZ_SDR_DB="SDRPA"
export NZ_ARCH_OMNI_DB="ARCH_OMNI"
export NZ_MART_OMNI_DB="CSMP"
export NZ_ER_DB="ERMP"
export NZ_STGRDW_DB="STGRDW"
export NZ_NIM_DB="NIMPA"
export NZ_METAMART_DB="METAMART"
export NZ_STAGE_DB="DI_STAGE"
export NZ_IBM_DB="IBM"
export NZ_STLG_DB="NFMP"
export NZ_STLG_AUDIT_NF_DB="AUDIT_NFPROD"
export NZ_STLG_SYNC_NF_DB="SYNC_NFPROD"
export NZ_POS_SYNC_DB="SYNC_POS"
export NZ_PRODUCT_DB="PRODUCT"
  
export SCHEMA_ONOFF="OFF"
export NZ_PRSNT_SCHEMA=""
export NZ_STG_SCHEMA=""
export NZ_RDW_SCHEMA=""
export NZ_HIST_SCHEMA=""
export NZ_UNICADB_SCHEMA=""
export NZ_FEEDTEMP_SCHEMA=""
export NZ_SDR_SCHEMA=""
export NZ_ARCH_OMNI_SCHEMA=""
export NZ_MART_OMNI_SCHEMA=""
export NZ_STGRDW_SCHEMA=""
export NZ_NIM_SCHEMA=""
export NZ_METAMART_SCHEMA=""
export NZ_STAGE_SCHEMA=""
export NZ_IBM_SCHEMA="ADMIN"
export NZ_STLG_SCHEMA=""
export NZ_STLG_AUDIT_NF_SCHEMA=""
export NZ_STLG_SYNC_NF_SCHEMA=""
export NZ_POS_SYNC_SCHEMA_=""
export NZ_PRODUCT_SCHEMA="PRODUCT_DATA"

# Attribution
export NZ_ATTRIBUTION_STAGE_DB="DI_STAGE"
export NZ_ATTRIBUTION_STAGE_SCHEMA=""
export NZ_ATTRIBUTION_CDM_DB="CDMA"
export NZ_ATTRIBUTION_CDM_SCHEMA=""

export PARAMS_X="_X"
export PARAMS_Z="_Z"

export NZ_DATABASE=$NZ_STG_DB
. $SCRIPTS_DIR/nz_db_profile

export LOG_FILE_NAME=${SCRIPT_NAME%.*}
export RUN_DATE=$(date +"%Y%m%d")
export LOG_FILE=$LOG_DIR/$LOG_FILE_NAME.$RUN_DATE.log

exec 1>>$LOG_FILE
exec 2>>$LOG_FILE

##change this to 1 if you want to echo All Queries executed against the DB 
export DEBUG_FLAG=0
