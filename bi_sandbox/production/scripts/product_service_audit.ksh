#!/bin/ksh
######################################################################################################
# script :      product_service_audit.ksh
# description : This script send a Product Service Audit details.
# Modifications :
# 1.0  2016-Mar-22  Infosys : Creation of Script
#####################################################################################################
. $SCRIPTS_DIR/load_parameters.ksh
. $SCRIPTS_DIR/load_library.ksh
. $SCRIPTS_DIR/load_datastage_params.ksh

curr_dt=`date`
curr_dt_only=`date +%m/%d/%Y`

echo "********************************************************************" >>$LOG_DIR/product_service_audit.log
echo "Script product_service_audit.ksh started at "$curr_dt >>$LOG_DIR/product_service_audit.log
echo "*********************************************************************************" >>$LOG_DIR/product_service_audit.log

export EMAIL_DL="rcyriac@rei.com,ksudha@rei.com,nthanga@rei.com,huwilli@rei.com,vjha@rei.com,sekuria@rei.com,kdaniel@rei.com,jgeorge@rei.com,jlang@rei.com,smohamm@rei.com"


query="
SELECT count(1)
FROM (
	SELECT DISTINCT sku
		,sku_key
		,FIRST_VALUE(sku_history_key) OVER (
			PARTITION BY sku ORDER BY effective_datetime DESC
			) AS max_sku_history_key
	FROM $NZ_PRODUCT_SCHEMA.service_sku_history
	) service_sku_history
WHERE sku_key = - 1"

SERVICE_SKU_KEY=$(nzsql -d $NZ_PRODUCT_DB -A -t -c "$query")


query="
SELECT COUNT(1) FROM (SELECT srv_article FROM $NZ_PRSNT_DB..SRV_ARTICLE_DIM
WHERE srv_article NOT IN (8069180019,
8069190018,
8069200015,
8069210014,
8069220013,
8069230012,
8069240011,
8069250010,
8078020016,
8730700002,-1,-2)
MINUS
SELECT CAST(sku as BIGINT) as sku FROM $NZ_PRODUCT_SCHEMA.SERVICE_SKU) service_sku"

SRV_ARTICLE_EXTRAS=$(nzsql -d $NZ_PRODUCT_DB -A -t -c "$query")


query="
select count(1) from (select sku from (
select sku,sku_key from $NZ_PRODUCT_SCHEMA.SERVICE_SKU
where sku_key <> -1
MINUS
select srv_article , SAD_KEY from $NZ_PRSNT_DB..SRV_ARTICLE_DIM) sku_key
UNION 
select sku from (
select sku,sku_name from $NZ_PRODUCT_SCHEMA.SERVICE_SKU
where sku_key <> -1
MINUS
select srv_article , srv_article_desc from $NZ_PRSNT_DB..SRV_ARTICLE_DIM) sku_name
UNION
select sku from (
select sku,product from $NZ_PRODUCT_SCHEMA.SERVICE_SKU
where sku_key <> -1
MINUS
select srv_article, SUBSTR(srv_article,1,6) AS product from $NZ_PRSNT_DB..SRV_ARTICLE_DIM) product
UNION
select sku from (
select sku,subclass_id from $NZ_PRODUCT_SCHEMA.SERVICE_SKU
where sku_key <> -1
MINUS
select srv_article, sub_class_id from $NZ_PRSNT_DB..SRV_ARTICLE_DIM) subclassid
UNION
select sku from (
select sku,subclass_desc from $NZ_PRODUCT_SCHEMA.SERVICE_SKU
where sku_key <> -1
MINUS
select srv_article, sub_class_desc from $NZ_PRSNT_DB..SRV_ARTICLE_DIM) subclass_desc)service_sku"

SERVICE_SKU_MISMATCH_CNT=$(nzsql -d $NZ_PRODUCT_DB -A -t -c "$query")


query="
select count(1) from (
select sku,sku_key from $NZ_PRODUCT_SCHEMA.SERVICE_SKU
where sku_key <> -1
MINUS
select srv_article , SAD_KEY from $NZ_PRSNT_DB..SRV_ARTICLE_DIM) sku_key"

SERVICE_SKU_KEY_MISMATCH=$(nzsql -d $NZ_PRODUCT_DB -A -t -c "$query")


query="
select count(1) from (
select sku,sku_name from $NZ_PRODUCT_SCHEMA.SERVICE_SKU
where sku_key <> -1
MINUS
select srv_article , srv_article_desc from $NZ_PRSNT_DB..SRV_ARTICLE_DIM) sku_name"

SERVICE_SKU_NAME_MISMATCH=$(nzsql -d $NZ_PRODUCT_DB -A -t -c "$query")




query="
select count(1) from (
select sku,product from $NZ_PRODUCT_SCHEMA.SERVICE_SKU
where sku_key <> -1
MINUS
select srv_article, SUBSTR(srv_article,1,6) AS product from $NZ_PRSNT_DB..SRV_ARTICLE_DIM) product"

SERVICE_PRODUCT_MISMATCH=$(nzsql -d $NZ_PRODUCT_DB -A -t -c "$query")




query="
select count(1) from (
select sku,subclass_id from $NZ_PRODUCT_SCHEMA.SERVICE_SKU
where sku_key <> -1
MINUS
select srv_article, sub_class_id from $NZ_PRSNT_DB..SRV_ARTICLE_DIM) subclassid"

SERVICE_SUB_CLASS_ID_MISMATCH=$(nzsql -d $NZ_PRODUCT_DB -A -t -c "$query")




query="
select count(1) from (
select sku,subclass_desc from $NZ_PRODUCT_SCHEMA.SERVICE_SKU
where sku_key <> -1
MINUS
select srv_article, sub_class_desc from $NZ_PRSNT_DB..SRV_ARTICLE_DIM) subclass_desc"

SERVICE_SUB_CLASS_DESC_MISMATCH=$(nzsql -d $NZ_PRODUCT_DB -A -t -c "$query")


query="
select sku from (
select sku,sku_key from $NZ_PRODUCT_SCHEMA.SERVICE_SKU
where sku_key <> -1
MINUS
select srv_article , SAD_KEY from $NZ_PRSNT_DB..SRV_ARTICLE_DIM) sku_key
UNION 
select sku from (
select sku,sku_name from $NZ_PRODUCT_SCHEMA.SERVICE_SKU
where sku_key <> -1
MINUS
select srv_article , srv_article_desc from $NZ_PRSNT_DB..SRV_ARTICLE_DIM) sku_name
UNION
select sku from (
select sku,product from $NZ_PRODUCT_SCHEMA.SERVICE_SKU
where sku_key <> -1
MINUS
select srv_article, SUBSTR(srv_article,1,6) AS product from $NZ_PRSNT_DB..SRV_ARTICLE_DIM) product
UNION
select sku from (
select sku,subclass_id from $NZ_PRODUCT_SCHEMA.SERVICE_SKU
where sku_key <> -1
MINUS
select srv_article, sub_class_id from $NZ_PRSNT_DB..SRV_ARTICLE_DIM) subclassid
UNION
select sku from (
select sku,subclass_desc from $NZ_PRODUCT_SCHEMA.SERVICE_SKU
where sku_key <> -1
MINUS
select srv_article, sub_class_desc from $NZ_PRSNT_DB..SRV_ARTICLE_DIM) subclass_desc"

SERVICE_SKU_MISMATCH=$(nzsql -d $NZ_PRODUCT_DB -A -t -c "$query")

nzsql -o $SCRIPTS_DIR/SERVICE_SKU_MISMATCH_IDs.txt -d $NZ_PRODUCT_DB -A -t -c "$query"


#The count is sent through an email

touch $SCRIPTS_DIR/body_service.html

   SUBJECT="Product Service Audit" 
echo "              
Hello!

Please find below the details of the Audits on the Product Service tables. 


SERVICE_PRODUCT 
-----------------------
Total Service SKU Records with SKU_KEY as -1							:$SERVICE_SKU_KEY
Total Service SKU Records in Legacy and not in New Product Model				:$SRV_ARTICLE_EXTRAS
Total Service SKU Records content mismatch in Legacy and New Product Model		:$SERVICE_SKU_MISMATCH_CNT

Content Mismatch details
-------------------------

Total SERVICE SKU Records content mismatch - SKU_KEY		:$SERVICE_SKU_KEY_MISMATCH
Total SERVICE SKU Records content mismatch - SKU_NAME		:$SERVICE_SKU_NAME_MISMATCH
Total SERVICE SKU Records content mismatch - PRODUCT		:$SERVICE_PRODUCT_MISMATCH
Total SERVICE SKU Records content mismatch - SUBCLASSID		:$SERVICE_SUB_CLASS_ID_MISMATCH
Total SERVICE SKU Records content mismatch - SUBCLASS_DESC	:$SERVICE_SUB_CLASS_DESC_MISMATCH


Please find the attached file with the Native IDs for the records with content mismatch in Legacy and New Product Model.
Please contact ITBISupport@rei.com in case of any questions    
    
Thanks    
REI BI Support Team   

">>body_service.html


mailx -s "Product Service Audit" -a "$SCRIPTS_DIR/SERVICE_SKU_MISMATCH_IDs.txt" $EMAIL_DL <$SCRIPTS_DIR/body_service.html;


rm $SCRIPTS_DIR/body_service.html
rm $SCRIPTS_DIR/SERVICE_SKU_MISMATCH_IDs.txt

if [[ $? != 0 ]] 
then 

echo "Script DID NOT complete successfully" >>$LOG_DIR/product_service_audit.log
exit 1

else
echo "Script completed successfully" >>$LOG_DIR/product_service_audit.log
fi

echo "************************End of Script********************************************" >>$LOG_DIR/product_service_audit.log

