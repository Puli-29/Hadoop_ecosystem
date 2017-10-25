1.DDL:
----
CREATE TABLE IF NOT EXISTS ca_willis_edw_sales_store_wreturn_kc
(trn_sls_dte STRING
,salary STRING
,destination STRING)
PARTITIONED BY(DT STRING) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION 'gs://dataprep-staging-54c37e40-3c71-42bd-b706-278a3cc91a11/work/eipdb_sandbox/ca_willis_edw_sales_store_wreturn_kc';
-------------------------------------------------------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS EIV_C_POS_PAYT_TRN
(
EI_STR_ID INT
,TRN_SLS_DTE STRING
,tndr_typ_cde STRING
,tndr_amt INT) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION 'gs://dataprep-staging-54c37e40-3c71-42bd-b706-278a3cc91a11/work/eipdb_slv/EIV_C_POS_PAYT_TRN';

CREATE TABLE IF NOT EXISTS EIV_C_LOC_DIM
(EI_LOC_ID INT
,E_BUS_IND STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION 'gs://dataprep-staging-54c37e40-3c71-42bd-b706-278a3cc91a11/work/eipdb_slv/EIV_C_LOC_DIM';

CREATE TABLE IF NOT EXISTS EIV_H_STR_CHARTC_DIM
(ei_str_id INT
,tm_dim_ky_dte STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION 'gs://dataprep-staging-54c37e40-3c71-42bd-b706-278a3cc91a11/work/eipdb_slv/EIV_H_STR_CHARTC_DIM';

CREATE TABLE IF NOT EXISTS EIV_H_STR_CHARTC_DIM
(ei_str_id INT
,tm_dim_ky_dte STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION 'gs://dataprep-staging-54c37e40-3c71-42bd-b706-278a3cc91a11/work/eipdb_slv/EIV_H_STR_CHARTC_DIM';

CREATE TABLE IF NOT EXISTS eiv_sls_trn_sku_agg
(trn_nbr INT
,ei_str_id INT
,pos_rgst_id INT
,trn_sls_dte STRING
,sls_corr_seq_nbr INT
,trn_strt_tm STRING
,ecom_ord_dte STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION 'gs://dataprep-staging-54c37e40-3c71-42bd-b706-278a3cc91a11/work/eipdb_slv/eiv_sls_trn_sku_agg';

-------------------------------------------------------------------------------------------------------------------------------------
INSERT INTO TABLE ca_willis_edw_sales_store_wreturn_kc
SELECT a11.trn_sls_dte,sum( case when tndr_typ_cde = '04' then tndr_amt end) as compstores_net_chrgd_amt_kc,sum(case when tndr_typ_cde <> '04' then tndr_amt end) as compstores_net_chrgd_amt_nkc
from eipdb_slv.EIV_C_POS_PAYT_TRN a11 JOIN eipdb_slv.EIV_C_LOC_DIM a12 on(a11.EI_STR_ID = a12.EI_LOC_ID)
JOIN eipdb_slv.EIV_H_STR_CHARTC_DIM Locl on(a11.ei_str_id = Locl.ei_str_id) 
JOIN eipdb_slv.eiv_sls_trn_sku_agg agg121
on a11.trn_nbr=agg121.trn_nbr 
and a11.ei_str_id=agg121.ei_str_id 
and a11.pos_rgst_id=agg121.pos_rgst_id 
and a11.trn_sls_dte=agg121.trn_sls_dte 
and a11.sls_corr_seq_nbr=agg121.sls_corr_seq_nbr
and a11.trn_strt_tm = agg121.trn_strt_tm
where (a12.E_BUS_IND in ('N') and a11.trn_sls_dte between DATE '2011-01-30' and DATE '2017-07-29')
and agg121.MNLY_COMP_SLS_SUM_IND = 'Y' and agg121.ecom_ord_dte is NULL
AND Locl.tm_dim_ky_dte ='2017-05-08'
group by a11.trn_sls_dte
,agg121.trn_nbr
,agg121.ei_str_id
,agg121.pos_rgst_id 
,agg121.trn_sls_dte 
,agg121.sls_corr_seq_nbr 
,agg121.trn_strt_tm
,agg121.ecom_ord_dte;
========================================================================================================================================
2.ca_willis_edw_store_sales_po_wreturn_june
-----------------------------------------
CREATE TABLE IF NOT EXISTS ca_willis_edw_store_sales_po_wreturn_june
(trn_sls_dte DATE
,fscl_mn_id INT
,fscl_qtr_id INT
,comp_date DATE 
,po_date DATE
,net_chrgd_amt DECIMAL(15,2)
,compstores_net_chrgd_amt DECIMAL(15,2)
,compstores_net_chrgd_amt_kc DECIMAL(15,2)
,compstores_net_chrgd_amt_nkc DECIMAL(15,2)
,comp_net_chrgd_amt DECIMAL(15,2)
,compstores_comp_net_chrgd_amt DECIMAL(15,2)
,compstores_comp_net_chrgd_amt_kc DECIMAL(15,2)
,compstores_comp_net_chrgd_amt_nkc DECIMAL(15,2)
,po_net_chrgd_amt DECIMAL(15,2)
,compstores_po_net_chrgd_amt DECIMAL(15,2)
,compstores_po_net_chrgd_amt_kc DECIMAL(15,2)
,compstores_po_net_chrgd_amt_nkc DECIMAL(15,2)
,comp_diff_avg_avg_temp_terr1 DECIMAL(6,2)
,comp_diff_avg_avg_temp_terr2 DECIMAL(6,2)
,comp_diff_avg_avg_temp_terr3 DECIMAL(6,2)
,comp_diff_avg_avg_temp_terr4 DECIMAL(6,2)
,comp_diff_avg_precip_terr1 DECIMAL(6,2)
,comp_diff_avg_precip_terr2 DECIMAL(6,2)
,comp_diff_avg_precip_terr3 DECIMAL(6,2)
,comp_diff_avg_precip_terr4 DECIMAL(6,2)
,comp_diff_avg_snow_terr1 DECIMAL(6,2)
,comp_diff_avg_snow_terr2 DECIMAL(6,2)
,comp_diff_avg_snow_terr3 DECIMAL(6,2)
,comp_diff_avg_snow_terr4 DECIMAL(6,2)
,po_diff_avg_avg_temp_terr1 DECIMAL(6,2)
,po_diff_avg_avg_temp_terr2 DECIMAL(6,2)
,po_diff_avg_avg_temp_terr3 DECIMAL(6,2)
,po_diff_avg_avg_temp_terr4 DECIMAL(6,2)
,po_diff_avg_precip_terr1 DECIMAL(6,2)
,po_diff_avg_precip_terr2 DECIMAL(6,2)
,po_diff_avg_precip_terr3 DECIMAL(6,2)
,po_diff_avg_precip_terr4 DECIMAL(6,2)
,po_diff_avg_snow_terr1 DECIMAL(6,2)
,po_diff_avg_snow_terr2 DECIMAL(6,2)
,po_diff_avg_snow_terr3 DECIMAL(6,2)
,po_diff_avg_snow_terr4 DECIMAL(6,2)
,VALENTINES_DAY SMALLINT
,PRESIDENTS_DAY SMALLINT
,EASTER SMALLINT
,MOTHERS_DAY SMALLINT
,MEMORIAL_DAY SMALLINT
,FATHERS_DAY SMALLINT
,INDEPENDENCE_DAY SMALLINT
,LABOR_DAY SMALLINT
,COLUMBUS_DAY SMALLINT
,HALLOWEEN SMALLINT
,VETERANS_DAY SMALLINT
,THANKSGIVING SMALLINT
,CHRISTMAS SMALLINT
,NEW_YEARS SMALLINT
,MLK_DAY SMALLINT
,SUPER_BOWL SMALLINT
,HANUKKAH SMALLINT
,TAB_SUPPORT_FLAG SMALLINT
,DIRECTMAIL_SUPPORT_FLAG SMALLINT
,RADIO_SUPPORT_FLAG SMALLINT
,TV_SUPPORT_FLAG SMALLINT
,FULLFILE_EMAIL_SUPPORT_FLAG SMALLINT
,ONLINE_SUPPORT_FLAG SMALLINT
,KOHLSCASHEARN SMALLINT
,KOHLSCASHREDEEM SMALLINT
,LTO SMALLINT
,FRIENDSFAMILY SMALLINT
,ASSOCIATE_SHOP SMALLINT
,CREDIT_EVENT SMALLINT
,PAD_MVC_BURG SMALLINT
,PAD_MVC SMALLINT
,KC_SHOPPING_PASS SMALLINT
,BC_SHOPPING_PASS SMALLINT
,GPO SMALLINT
,LOYALTY_TRIPLE_POINTS SMALLINT
,EMAIL_MYSTERY_OFFER SMALLINT
,comp_VALENTINES_DAY SMALLINT
,comp_PRESIDENTS_DAY SMALLINT
,comp_EASTER SMALLINT
,comp_MOTHERS_DAY SMALLINT
,comp_MEMORIAL_DAY SMALLINT
,comp_FATHERS_DAY SMALLINT
,comp_INDEPENDENCE_DAY SMALLINT
,comp_LABOR_DAY SMALLINT
,comp_COLUMBUS_DAY SMALLINT
,comp_HALLOWEEN SMALLINT
,comp_VETERANS_DAY SMALLINT
,comp_THANKSGIVING SMALLINT
,comp_CHRISTMAS SMALLINT
,comp_NEW_YEARS SMALLINT
,comp_MLK_DAY SMALLINT
,comp_SUPER_BOWL SMALLINT
,comp_HANUKKAH SMALLINT
,comp_TAB_SUPPORT_FLAG SMALLINT
,comp_DIRECTMAIL_SUPPORT_FLAG SMALLINT
,comp_RADIO_SUPPORT_FLAG SMALLINT
,comp_TV_SUPPORT_FLAG SMALLINT
,comp_FULLFILE_EMAIL_SUPPORT_FLAG SMALLINT
,comp_ONLINE_SUPPORT_FLAG SMALLINT
,comp_KOHLSCASHEARN SMALLINT
,comp_KOHLSCASHREDEEM SMALLINT
,comp_LTO SMALLINT
,comp_FRIENDSFAMILY SMALLINT
,comp_ASSOCIATE_SHOP SMALLINT
,comp_CREDIT_EVENT SMALLINT
,comp_PAD_MVC_BURG SMALLINT
,comp_PAD_MVC SMALLINT
,comp_KC_SHOPPING_PASS SMALLINT
,comp_BC_SHOPPING_PASS SMALLINT
,comp_GPO SMALLINT
,comp_LOYALTY_TRIPLE_POINTS SMALLINT
,comp_EMAIL_MYSTERY_OFFER SMALLINT
,po_VALENTINES_DAY SMALLINT
,po_PRESIDENTS_DAY SMALLINT
,po_EASTER SMALLINT
,po_MOTHERS_DAY SMALLINT
,po_MEMORIAL_DAY SMALLINT
,po_FATHERS_DAY SMALLINT
,po_INDEPENDENCE_DAY SMALLINT
,po_LABOR_DAY SMALLINT
,po_COLUMBUS_DAY SMALLINT
,po_HALLOWEEN SMALLINT
,po_VETERANS_DAY SMALLINT
,po_THANKSGIVING SMALLINT
,po_CHRISTMAS SMALLINT
,po_NEW_YEARS SMALLINT
,po_MLK_DAY SMALLINT
,po_SUPER_BOWL SMALLINT
,po_HANUKKAH SMALLINT
,po_TAB_SUPPORT_FLAG SMALLINT
,po_DIRECTMAIL_SUPPORT_FLAG SMALLINT
,po_RADIO_SUPPORT_FLAG SMALLINT
,po_TV_SUPPORT_FLAG SMALLINT
,po_FULLFILE_EMAIL_SUPPORT_FLAG SMALLINT
,po_ONLINE_SUPPORT_FLAG SMALLINT
,po_KOHLSCASHEARN SMALLINT
,po_KOHLSCASHREDEEM SMALLINT
,po_LTO SMALLINT
,po_FRIENDSFAMILY SMALLINT
,po_ASSOCIATE_SHOP SMALLINT
,po_CREDIT_EVENT SMALLINT
,po_PAD_MVC_BURG SMALLINT
,po_PAD_MVC SMALLINT
,po_KC_SHOPPING_PASS SMALLINT
,po_BC_SHOPPING_PASS SMALLINT
,po_GPO SMALLINT
,po_LOYALTY_TRIPLE_POINTS SMALLINT
,po_EMAIL_MYSTERY_OFFER SMALLINT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION 'gs://dataprep-staging-54c37e40-3c71-42bd-b706-278a3cc91a11/work/eipdb_sandbox/ca_willis_edw_store_sales_po_wreturn_june';
-----------------------------------------------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ca_willis_edw_sales_po_wreturn_weather
(trn_sls_dte DATE
,fscl_mn_id INT
,fscl_qtr_id INT
,comp_date DATE
,po_date DATE
,net_chrgd_amt DECIMAL(15,2)
,compstores_net_chrgd_amt DECIMAL(15,2)
,compstores_net_chrgd_amt_kc DECIMAL(15,2)
,compstores_net_chrgd_amt_nkc DECIMAL(15,2)
,comp_net_chrgd_amt DECIMAL(15,2)
,compstores_comp_net_chrgd_amt DECIMAL(15,2)
,compstores_comp_net_chrgd_amt_kc DECIMAL(15,2)
,compstores_comp_net_chrgd_amt_nkc DECIMAL(15,2)
,po_net_chrgd_amt DECIMAL(15,2)
,compstores_po_net_chrgd_amt DECIMAL(15,2)
,compstores_po_net_chrgd_amt_kc DECIMAL(15,2)
,compstores_po_net_chrgd_amt_nkc DECIMAL(15,2)
,comp_diff_avg_avg_temp_terr1 DECIMAL(6,2)
,comp_diff_avg_avg_temp_terr2 DECIMAL(6,2)
,comp_diff_avg_avg_temp_terr3 DECIMAL(6,2)
,comp_diff_avg_avg_temp_terr4 DECIMAL(6,2)
,comp_diff_avg_precip_terr1 DECIMAL(6,2)
,comp_diff_avg_precip_terr2 DECIMAL(6,2)
,comp_diff_avg_precip_terr3 DECIMAL(6,2)
,comp_diff_avg_precip_terr4 DECIMAL(6,2)
,comp_diff_avg_snow_terr1 DECIMAL(6,2)
,comp_diff_avg_snow_terr2 DECIMAL(6,2)
,comp_diff_avg_snow_terr3 DECIMAL(6,2)
,comp_diff_avg_snow_terr4 DECIMAL(6,2)
,po_diff_avg_avg_temp_terr1 DECIMAL(6,2)
,po_diff_avg_avg_temp_terr2 DECIMAL(6,2)
,po_diff_avg_avg_temp_terr3 DECIMAL(6,2)
,po_diff_avg_avg_temp_terr4 DECIMAL(6,2)
,po_diff_avg_precip_terr1 DECIMAL(6,2)
,po_diff_avg_precip_terr2 DECIMAL(6,2)
,po_diff_avg_precip_terr3 DECIMAL(6,2)
,po_diff_avg_precip_terr4 DECIMAL(6,2)
,po_diff_avg_snow_terr1 DECIMAL(6,2)
,po_diff_avg_snow_terr2 DECIMAL(6,2)
,po_diff_avg_snow_terr3 DECIMAL(6,2)
,po_diff_avg_snow_terr4 DECIMAL(6,2)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION 'gs://dataprep-staging-54c37e40-3c71-42bd-b706-278a3cc91a11/work/eipdb_sandbox/ca_willis_edw_sales_po_wreturn_weather';
------------------------------------------------------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS eit_mktg_cal_v2
(mktg_cal_dte DATE)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION 'gs://dataprep-staging-54c37e40-3c71-42bd-b706-278a3cc91a11/work/eipdb_sandbox/eit_mktg_cal_v2';

---------------------------------------------------------------------------------------------------------------------------------
INSERT INTO TABLE ca_willis_edw_store_sales_po_wreturn_june
select 
trn_sls_dte
,agg.fscl_mn_id
,agg.fscl_qtr_id
,agg.comp_date 
,agg.po_date
,agg.net_chrgd_amt
,agg.compstores_net_chrgd_amt
,agg.compstores_net_chrgd_amt_kc
,agg.compstores_net_chrgd_amt_nkc
,agg.comp_net_chrgd_amt
,agg.compstores_comp_net_chrgd_amt
,agg.compstores_comp_net_chrgd_amt_kc
,agg.compstores_comp_net_chrgd_amt_nkc
,agg.po_net_chrgd_amt
,agg.compstores_po_net_chrgd_amt
,agg.compstores_po_net_chrgd_amt_kc
,agg.compstores_po_net_chrgd_amt_nkc
,agg.comp_diff_avg_avg_temp_terr1
,agg.comp_diff_avg_avg_temp_terr2
,agg.comp_diff_avg_avg_temp_terr3
,agg.comp_diff_avg_avg_temp_terr4
,agg.comp_diff_avg_precip_terr1
,agg.comp_diff_avg_precip_terr2
,agg.comp_diff_avg_precip_terr3
,agg.comp_diff_avg_precip_terr4
,agg.comp_diff_avg_snow_terr1
,agg.comp_diff_avg_snow_terr2
,agg.comp_diff_avg_snow_terr3
,agg.comp_diff_avg_snow_terr4
,agg.po_diff_avg_avg_temp_terr1
,agg.po_diff_avg_avg_temp_terr2
,agg.po_diff_avg_avg_temp_terr3
,agg.po_diff_avg_avg_temp_terr4
,agg.po_diff_avg_precip_terr1
,agg.po_diff_avg_precip_terr2
,agg.po_diff_avg_precip_terr3
,agg.po_diff_avg_precip_terr4
,agg.po_diff_avg_snow_terr1
,agg.po_diff_avg_snow_terr2
,agg.po_diff_avg_snow_terr3
,agg.po_diff_avg_snow_terr4
,mktg_cal.VALENTINES_DAY
,mktg_cal.PRESIDENTS_DAY
,mktg_cal.EASTER
,mktg_cal.MOTHERS_DAY
,mktg_cal.MEMORIAL_DAY
,mktg_cal.FATHERS_DAY
,mktg_cal.INDEPENDENCE_DAY
,mktg_cal.LABOR_DAY
,mktg_cal.COLUMBUS_DAY
,mktg_cal.HALLOWEEN
,mktg_cal.VETERANS_DAY
,mktg_cal.THANKSGIVING
,mktg_cal.CHRISTMAS
,mktg_cal.NEW_YEARS
,mktg_cal.MLK_DAY
,mktg_cal.SUPER_BOWL
,mktg_cal.HANUKKAH
,mktg_cal.TAB_SUPPORT_FLAG
,mktg_cal.DIRECTMAIL_SUPPORT_FLAG
,mktg_cal.RADIO_SUPPORT_FLAG
,mktg_cal.TV_SUPPORT_FLAG
,mktg_cal.FULLFILE_EMAIL_SUPPORT_FLAG
,mktg_cal.ONLINE_SUPPORT_FLAG
,mktg_cal.KOHLSCASHEARN
,mktg_cal.KOHLSCASHREDEEM
,mktg_cal.LTO
,mktg_cal.FRIENDSFAMILY
,mktg_cal.ASSOCIATE_SHOP
,mktg_cal.CREDIT_EVENT
,mktg_cal.PAD_MVC_BURG
,mktg_cal.PAD_MVC
,mktg_cal.KC_SHOPPING_PASS
,mktg_cal.BC_SHOPPING_PASS
,mktg_cal.GPO
,mktg_cal.LOYALTY_TRIPLE_POINTS
,mktg_cal.EMAIL_MYSTERY_OFFER
,mktg_cal2.VALENTINES_DAY				
,mktg_cal2.PRESIDENTS_DAY				
,mktg_cal2.EASTER						
,mktg_cal2.MOTHERS_DAY					
,mktg_cal2.MEMORIAL_DAY					
,mktg_cal2.FATHERS_DAY					
,mktg_cal2.INDEPENDENCE_DAY				
,mktg_cal2.LABOR_DAY					
,mktg_cal2.COLUMBUS_DAY					
,mktg_cal2.HALLOWEEN					
,mktg_cal2.VETERANS_DAY					
,mktg_cal2.THANKSGIVING					
,mktg_cal2.CHRISTMAS					
,mktg_cal2.NEW_YEARS					
,mktg_cal2.MLK_DAY						
,mktg_cal2.SUPER_BOWL					
,mktg_cal2.HANUKKAH						
,mktg_cal2.TAB_SUPPORT_FLAG				
,mktg_cal2.DIRECTMAIL_SUPPORT_FLAG		
,mktg_cal2.RADIO_SUPPORT_FLAG			
,mktg_cal2.TV_SUPPORT_FLAG				
,mktg_cal2.FULLFILE_EMAIL_SUPPORT_FLAG	
,mktg_cal2.ONLINE_SUPPORT_FLAG			
,mktg_cal2.KOHLSCASHEARN				
,mktg_cal2.KOHLSCASHREDEEM				
,mktg_cal2.LTO							
,mktg_cal2.FRIENDSFAMILY				
,mktg_cal2.ASSOCIATE_SHOP				
,mktg_cal2.CREDIT_EVENT					
,mktg_cal2.PAD_MVC_BURG					
,mktg_cal2.PAD_MVC						
,mktg_cal2.KC_SHOPPING_PASS				
,mktg_cal2.BC_SHOPPING_PASS				
,mktg_cal2.GPO							
,mktg_cal2.LOYALTY_TRIPLE_POINTS		
,mktg_cal2.EMAIL_MYSTERY_OFFER			
,mktg_cal3.VALENTINES_DAY				
,mktg_cal3.PRESIDENTS_DAY				
,mktg_cal3.EASTER						
,mktg_cal3.MOTHERS_DAY					
,mktg_cal3.MEMORIAL_DAY					
,mktg_cal3.FATHERS_DAY					
,mktg_cal3.INDEPENDENCE_DAY				
,mktg_cal3.LABOR_DAY					
,mktg_cal3.COLUMBUS_DAY					
,mktg_cal3.HALLOWEEN					
,mktg_cal3.VETERANS_DAY					
,mktg_cal3.THANKSGIVING					
,mktg_cal3.CHRISTMAS					
,mktg_cal3.NEW_YEARS					
,mktg_cal3.MLK_DAY						
,mktg_cal3.SUPER_BOWL					
,mktg_cal3.HANUKKAH						
,mktg_cal3.TAB_SUPPORT_FLAG				
,mktg_cal3.DIRECTMAIL_SUPPORT_FLAG		
,mktg_cal3.RADIO_SUPPORT_FLAG			
,mktg_cal3.TV_SUPPORT_FLAG				
,mktg_cal3.FULLFILE_EMAIL_SUPPORT_FLAG	
,mktg_cal3.ONLINE_SUPPORT_FLAG			
,mktg_cal3.KOHLSCASHEARN				
,mktg_cal3.KOHLSCASHREDEEM				
,mktg_cal3.LTO							
,mktg_cal3.FRIENDSFAMILY				
,mktg_cal3.ASSOCIATE_SHOP				
,mktg_cal3.CREDIT_EVENT					
,mktg_cal3.PAD_MVC_BURG					
,mktg_cal3.PAD_MVC						
,mktg_cal3.KC_SHOPPING_PASS				
,mktg_cal3.BC_SHOPPING_PASS				
,mktg_cal3.GPO							
,mktg_cal3.LOYALTY_TRIPLE_POINTS		
,mktg_cal3.EMAIL_MYSTERY_OFFER			
from eipdb_sandbox.ca_willis_edw_sales_po_wreturn_weather as agg
LEFT JOIN (select * from EIPDB_SANDBOX.eit_mktg_cal_v2) mktg_cal
on mktg_cal.mktg_cal_dte = agg.trn_sls_dte
LEFT JOIN (select * from EIPDB_SANDBOX.eit_mktg_cal_v2) mktg_cal2
on mktg_cal2.mktg_cal_dte = agg.comp_date
LEFT JOIN (select * from EIPDB_SANDBOX.eit_mktg_cal_v2) mktg_cal3
on mktg_cal3.mktg_cal_dte = agg.po_date;
-------------------------------------------------------------------------------------------
Sample data :
------------
INSERT INTO TABLE ca_willis_edw_sales_po_wreturn_weather VALUES('2017-10-16','1234','12345','2017-10-16','2017-10-16','120000','1230000','124500','134500','2345000','56000','45089','87607','89605','789078','678509','867540','32','24','45','54','54','32','43','56','54','56','45','55','87','65','66','76','76','45','55','34','33','44','56','76');INSERT INTO TABLE ca_willis_edw_sales_po_wreturn_weather VALUES('2017-10-16','1234','12345','2017-10-16','2017-10-16','120000','1230000','124500','134500','2345000','56000','45089','87607','89605','789078','678509','867540','32','24','45','54','54','32','43','56','54','56','45','55','87','65','66','76','76','45','55','34','33','44','56','76');
INSERT INTO TABLE ca_willis_edw_sales_po_wreturn_weather VALUES('2017-10-15','1234','12345','2017-10-16','2017-10-16','120000','1230000','124500','134500','2345000','56000','45089','87607','89605','789078','678509','867540','32','24','45','54','54','32','43','56','54','56','45','55','87','65','66','76','76','45','55','34','33','44','56','76');
INSERT INTO TABLE ca_willis_edw_sales_po_wreturn_weather VALUES('2017-10-14','1234','12345','2017-10-16','2017-10-16','120000','1230000','124500','134500','2345000','56000','45089','87607','89605','789078','678509','867540','32','24','45','54','54','32','43','56','54','56','45','55','87','65','66','76','76','45','55','34','33','44','56','76');
INSERT INTO TABLE ca_willis_edw_sales_po_wreturn_weather VALUES('2017-10-13','1234','12345','2017-10-16','2017-10-16','120000','1230000','124500','134500','2345000','56000','45089','87607','89605','789078','678509','867540','32','24','45','54','54','32','43','56','54','56','45','55','87','65','66','76','76','45','55','34','33','44','56','76');
INSERT INTO TABLE ca_willis_edw_sales_po_wreturn_weather VALUES('2017-10-12','1234','12345','2017-10-16','2017-10-16','120000','1230000','124500','134500','2345000','56000','45089','87607','89605','789078','678509','867540','32','24','45','54','54','32','43','56','54','56','45','55','87','65','66','76','76','45','55','34','33','44','56','76');

INSERT INTO TABLE eit_mktg_cal_v2 VALUES('2017-10-16','1','1','1','1','1' ,'1','1','1','1','1','1','1','1','1','1','1','1','1','1','1','1','1','1','1','1','1','1','1','1','1','1','1','1','1','1','1');
INSERT INTO TABLE eit_mktg_cal_v2 VALUES('2017-10-15','1','1','1','1','1' ,'1','1','1','1','1','1','1','1','1','1','1','1','1','1','1','1','1','1','1','1','1','1','1','1','1','1','1','1','1','1','1');
INSERT INTO TABLE eit_mktg_cal_v2 VALUES('2017-10-14','1','1','1','1','1' ,'1','1','1','1','1','1','1','1','1','1','1','1','1','1','1','1','1','1','1','1','1','1','1','1','1','1','1','1','1','1','1');
INSERT INTO TABLE eit_mktg_cal_v2 VALUES('2017-10-13','1','1','1','1','1' ,'1','1','1','1','1','1','1','1','1','1','1','1','1','1','1','1','1','1','1','1','1','1','1','1','1','1','1','1','1','1','1');
INSERT INTO TABLE eit_mktg_cal_v2 VALUES('2017-10-12','1','1','1','1','1' ,'1','1','1','1','1','1','1','1','1','1','1','1','1','1','1','1','1','1','1','1','1','1','1','1','1','1','1','1','1','1','1');
