
--============================================================================================

--FUTURE COMPONENT OF THE LONG TERM DONOR VALUE:
--BLOCK 1: CREATE THE BASIC ATTRIBUTES OF THE DATASET THAT CONSTITUTES THE SET OF FEATURES FOR THE DURATION (SURVIVAL MODEL)
--=============================================================================================

--THIS QUERY PRODUCES THE BASIC ATTRIBUTES OF THE DATASET FOR DURATION MODEL
--this might be applied to all relevant FY21 LTDV Project donors, which are sponsors and pledgers
--differently
--==============================================================================================
--sponsors identified under 
SELECT a.Donation_Donor_Id FROM  [SPSS_Sandbox].[dbo].[LTDV_SCRIPT_FY21_25072022_DNR_LABEL] a WHERE A.FY21_DNR_LABEL = 'SPONSOR'

SELECT a.Donation_Donor_Id FROM  [SPSS_Sandbox].[dbo].[LTDV_SCRIPT_FY21_25072022_DNR_LABEL] a WHERE A.FY21_DNR_LABEL = 'PLEDGER'


--CODE TO FEED the SURVIVAL MODEL FOR PREDICTION PURPOSES

--SPONSORS

CREATE OR REPLACE TABLE PRED_MODEL_FEATURE.LTDV_SP_DB_FOR_TENURE_ESTIMATION AS
select 
--00 - 'Donor_Id'
dm.DM_Donor_Id as Donor_Id
--01 G1_to_S1
, datediff(dd,dm.DM_Dt_First_Gift, dm.DM_Dt_First_Sponsorship_Gift) as G1_to_S1
--02 SN_to_GN
, datediff(dd,dm.DM_Dt_Most_Recent_Sponsorship_Gift, dm.DM_Dt_Most_Recent_Gift) as SN_to_GN
--03 HV_NSPP
, dm.DM_Amt_Total_Giving_Lifetime - dm.DM_Amt_Total_Sponsorship_Giving_Lifetime as HV_NSPP
--04 NOSP_Perc
, iff(dm.DM_Amt_Total_Giving_Lifetime = dm.DM_Amt_Total_Sponsorship_Giving_Lifetime, 0, (dm.DM_Amt_Total_Giving_Lifetime - dm.DM_Amt_Total_Sponsorship_Giving_Lifetime)/dm.DM_Amt_Total_Giving_Lifetime) as NOSP_Perc
--05 Mail_Elig_In
, iff(d.Donor_Mail_Eligible_Ind = 'Y', 1, 0) as Mail_Elig_Ind
--06 TM_Ind
, iff(d.Donor_Telemarketing_Ind = 'Y', 1, 0) as TM_Ind
--07 MWVP_Ind
, iff(d.Donor_My_World_Vision_Profile_Ind = 'Y', 1, 0) as MWVP_Ind
--08 Time_No_Email
, iff(d.Donor_No_Email_Dt is null or datediff(dd,d.Donor_No_Email_Dt, dm.DM_Dt_Most_Recent_Gift) < 0, 0, datediff(dd,d.Donor_No_Email_Dt, dm.DM_Dt_Most_Recent_Gift)) as Time_No_Email
--09 Time_No_Mail
, iff(d.Donor_No_Mail_Dt is null or datediff(dd,d.Donor_No_Mail_Dt, dm.DM_Dt_Most_Recent_Gift) < 0, 0, datediff(dd,d.Donor_No_Mail_Dt, dm.DM_Dt_Most_Recent_Gift)) as Time_No_Mail
--10 Time_No_TM
, iff(d.Donor_No_Telemarketing_Dt is null or datediff(dd,d.Donor_No_Telemarketing_Dt, dm.DM_Dt_Most_Recent_Gift) < 0, 0, datediff(dd,d.Donor_No_Telemarketing_Dt, dm.DM_Dt_Most_Recent_Gift)) as Time_No_TM
--11 Time_in_MyWV
, iff(d.Donor_My_World_Vision_Profile_Add_Dt is null or datediff(dd,d.Donor_My_World_Vision_Profile_Add_Dt, dm.DM_Dt_Most_Recent_Gift) < 0, 0, datediff(dd,d.Donor_My_World_Vision_Profile_Add_Dt, dm.DM_Dt_Most_Recent_Gift)) as Time_in_MyWVW
--12 Prt_Lg
--ENGLISH AS PRINT_LANGUAGE CONDITION (102: ENGLISH id 1 ; 103 IS FRENCH is 0)
, iff (D.Donor_Printing_Language_Sid = 102 , 1, 0) as Prt_Lg
--13 Valid_Phn_In
, iff(d.Donor_Valid_Telephone_Ind = 'Y' , 1, 0) as Valid_Phn_Ind
--14 FIN_SEG_AGG
, CASE WHEN DS.Segment_Id = 'FIN*GENERAL' THEN 0
WHEN DS.Segment_id = 'FIN*2GENERA' then 1 
else 2 end as FIN_SEG_AGG
--15 MTCat_DIGI
, IFF(dm.DM_Mot_Type_Of_Original_Sponsorship_Gift IN ('E2','EM','ES','MO','W2','WS') , 1, 0) AS MTCat_DIGI
--16 MTCat_F2F
, IFF(dm.DM_Mot_Type_Of_Original_Sponsorship_Gift IN ('AA', 'DD', 'MA', 'SF', 'AR', 'AM', 'DC' , 'F2' , 'FM' , 'PP' , 'RF' , 'SE' , 'V1' , 'VL') , 1, 0) AS MTCat_F2F
--17 REG_Abroad
, IFF( d.Donor_Country_Sid <> 148, 1, 0) AS REG_Abroad
--18 REG_Atlantic
, IFF(d.Donor_Prov_State in ( 'NB' , 'NL' , 'NS' , 'PE' ) and d.Donor_Country_Sid = 148 , 1, 0) AS REG_Atlantic
--19 REG_Northern
, IFF(d.Donor_Prov_State in ( 'NT' , 'NU' , 'YT' ) and d.Donor_Country_Sid = 148 , 1, 0) AS REG_Northern
--20 REG_Western
, IFF(d.Donor_Prov_State in ( 'AB' , 'BC' , 'MB' , 'SK' ) and d.Donor_Country_Sid = 148 , 1, 0) AS REG_Western
--21 PMT_BWP
, IFF(dm.DM_Pay_Method_Most_Recent_Sponsorship_Payment = 'BWP' , 1, 0) AS PMT_BWP
--22 PMT_CASHOT
, IFF(dm.DM_Pay_Method_Most_Recent_Sponsorship_Payment IN ( 'CASH' , 'OTHER' ) , 1, 0) AS PMT_CASHOT

from ADOBE.RAW.DONOR_METRICS dm
join ADOBE.RAW.D_DONOR d on d.donor_id = dm.DM_Donor_Id and d.Current_Ind = 1 and D.DONOR_TYPE_SID IN ( 100 , 120)
join ADOBE.RAW.D_DONOR_SEGMENT DSG on DSG.Donor_Segment_Donor_Id = DM.DM_Donor_Id and DSG.Donor_Segment_Current_Record = 'Y'
join ADOBE.RAW.D_SEGMENT_BI_DW DS on DS.segment_sid = DSG.Donor_Segment_Segment_Sid and DS.Segment_type_Sid = 100 and DS.Segment_Id in ( 'FIN*GENERAL' , 'FIN*2GENERA', 'FIN*CHINESE' , 'FIN*KOREAN', 'FIN*PORTUG' ,'FIN*SASIA') 
join ADOBE.RAW.D_SEGMENT_TYPE DST on DST.Segment_Type_Sid = DS.Segment_Type_Sid and DST.Segment_Type_Id = 'FIN'

WHERE dm.DM_Donor_Id in (SELECT a.Donation_Donor_Id FROM  PRED_MODEL_FEATURE.LTDV_SCRIPT_DNR_LABEL a WHERE A.DNR_LABEL = 'SPONSOR');


--verify/inspect: select * from [SPSS_Sandbox].[dbo].[LTDV_FY21_SP_DB_FOR_TENURE_ESTIMATION] order by 1


--PLEDGERS


--VARIABLES TO LOOK FOR EQUIVALENCE
-- dm.DM_Dt_First_Sponsorship_Gift ==> First 101 donation
-- dm.DM_Amt_Total_Sponsorship_Giving_Lifetime sum(101 donations) (from query)
-- dm.DM_Pay_Method_Most_Recent_Sponsorship_Payment   ==> USE DM_Pay_Method_First_101_Payment (from query)
-- dm.DM_Mot_Type_Of_Original_Sponsorship_Gift get acquisition type from ltdv table or run code as in script for LTDV table


select 
--00 - 'Donor_Id'
dm.DM_Donor_Id as 'Donor_Id'
--01 G1_to_S1
, datediff(dd,dm.DM_Dt_First_Gift, feat0.Date_First_101_Payment) as 'G1_to_S1'
--02 SN_to_GN
, datediff(dd,feat0.Date_Most_Recent_101_Payment, dm.DM_Dt_Most_Recent_Gift) as 'SN_to_GN'
--03 HV_NSPP
, dm.DM_Amt_Total_Giving_Lifetime - feat0.DM_Amt_Total_101_Giving_Lifetime as 'HV_NSPP'
--04 NOSP_Perc
, iif(dm.DM_Amt_Total_Giving_Lifetime = feat0.DM_Amt_Total_101_Giving_Lifetime, 0, (dm.DM_Amt_Total_Giving_Lifetime - feat0.DM_Amt_Total_101_Giving_Lifetime)/dm.DM_Amt_Total_Giving_Lifetime) as 'NOSP_Perc'
--05 Mail_Elig_In
, iif(d.Donor_Mail_Eligible_Ind = 'Y', 1, 0) as 'Mail_Elig_Ind'
--06 TM_Ind
, iif(d.Donor_Telemarketing_Ind = 'Y', 1, 0) as 'TM_Ind'
--07 MWVP_Ind
, iif(d.Donor_My_World_Vision_Profile_Ind = 'Y', 1, 0) as 'MWVP_Ind'
--08 Time_No_Email
, iif(d.Donor_No_Email_Dt is null or datediff(dd,d.Donor_No_Email_Dt, dm.DM_Dt_Most_Recent_Gift) < 0, 0, datediff(dd,d.Donor_No_Email_Dt, dm.DM_Dt_Most_Recent_Gift)) as 'Time_No_Email'
--09 Time_No_Mail
, iif(d.Donor_No_Mail_Dt is null or datediff(dd,d.Donor_No_Mail_Dt, dm.DM_Dt_Most_Recent_Gift) < 0, 0, datediff(dd,d.Donor_No_Mail_Dt, dm.DM_Dt_Most_Recent_Gift)) as 'Time_No_Mail'
--10 Time_No_TM
, iif(d.Donor_No_Telemarketing_Dt is null or datediff(dd,d.Donor_No_Telemarketing_Dt, dm.DM_Dt_Most_Recent_Gift) < 0, 0, datediff(dd,d.Donor_No_Telemarketing_Dt, dm.DM_Dt_Most_Recent_Gift)) as 'Time_No_TM'
--11 Time_in_MyWV
, iif(d.Donor_My_World_Vision_Profile_Add_Dt is null or datediff(dd,d.Donor_My_World_Vision_Profile_Add_Dt, dm.DM_Dt_Most_Recent_Gift) < 0, 0, datediff(dd,d.Donor_My_World_Vision_Profile_Add_Dt, dm.DM_Dt_Most_Recent_Gift)) as 'Time_in_MyWVW'
--12 Prt_Lg
--ENGLISH AS PRINT_LANGUAGE CONDITION (102: ENGLISH id 1 ; 103 IS FRENCH is 0)
, iif (D.Donor_Printing_Language_Sid = 102 , 1, 0) as 'Prt_Lg' 
--13 Valid_Phn_In
, iif(d.Donor_Valid_Telephone_Ind = 'Y' , 1, 0) as 'Valid_Phn_Ind'
--14 FIN_SEG_AGG
, CASE WHEN DS.Segment_Id = 'FIN*GENERAL' THEN 0
WHEN DS.Segment_id = 'FIN*2GENERA' then 1 
else 2 end as 'FIN_SEG_AGG'
--15 MTCat_DIGI
, feat0.MTCat_DIGI
--16 MTCat_F2F
, feat0.MTCat_F2F
--17 REG_Abroad
, IIF( d.Donor_Country_Sid <> 148, 1, 0) AS 'REG_Abroad'
--18 REG_Atlantic
, IIF(d.Donor_Prov_State in ( 'NB' , 'NL' , 'NS' , 'PE' ) and d.Donor_Country_Sid = 148 , 1, 0) AS 'REG_Atlantic'
--19 REG_Northern
, IIF(d.Donor_Prov_State in ( 'NT' , 'NU' , 'YT' ) and d.Donor_Country_Sid = 148 , 1, 0) AS 'REG_Northern'
--20 REG_Western
, IIF(d.Donor_Prov_State in ( 'AB' , 'BC' , 'MB' , 'SK' ) and d.Donor_Country_Sid = 148 , 1, 0) AS 'REG_Western'
--21 PMT_BWP
, feat0.PMT_BWP
--22 PMT_CASHOT
, feat0.PMT_CASHOT

INTO #spf_plr--[SPSS_Sandbox].[dbo].[LTDV_FY21_PL_DB_FOR_TENURE_ESTIMATION]
from [BI_DW].[DBO].[Donor_Metrics] dm
join ADOBE.RAW.D_DONOR d on d.donor_id = dm.DM_Donor_Id and d.Current_Ind = 1 and D.DONOR_TYPE_SID IN ( 100 , 120)
join ADOBE.RAW.D_DONOR_SEGMENT DSG on DSG.Donor_Segment_Donor_Id = DM.DM_Donor_Id and DSG.Donor_Segment_Current_Record = 'Y'
join ADOBE.RAW.D_SEGMENT_BI_DW DS on DS.segment_sid = DSG.Donor_Segment_Segment_Sid and DS.Segment_type_Sid = 100 and DS.Segment_Id in ( 'FIN*GENERAL' , 'FIN*2GENERA', 'FIN*CHINESE' , 'FIN*KOREAN', 'FIN*PORTUG' ,'FIN*SASIA') 
join ADOBE.RAW.D_SEGMENT_TYPE DST on DST.Segment_Type_Sid = DS.Segment_Type_Sid and DST.Segment_Type_Id = 'FIN'

join (select c.Donation_Donor_Id as 'Donor_id'
, c.DM_Amt_Total_101_Giving_Lifetime
, c.Date_First_101_Payment
, c.Date_Most_Recent_101_Payment
, c.PMT_BWP
, c.PMT_CASHOT
, IIF(mot.Motivation_Type_Cd  IN ('E2','EM','ES','MO','W2','WS') , 1, 0) AS 'MTCat_DIGI'
, IIF(mot.Motivation_Type_Cd IN ('AA', 'DD', 'MA', 'SF', 'AR', 'AM', 'DC' , 'F2' , 'FM' , 'PP' , 'RF' , 'SE' , 'V1' , 'VL') , 1, 0) AS 'MTCat_F2F'

from
(
select b.Donation_Donor_Id
, b.DM_Amt_Total_101_Giving_Lifetime
, min(b.DM_Date_First_101_Payment) as 'Date_First_101_Payment'
, max(b.DM_Date_Most_Recent_101_Payment) as 'Date_Most_Recent_101_Payment'
, iif(max(b.DM_Pay_Method_Most_Recent_101_Payment) = 103, 1, 0) as 'PMT_BWP'
, iif(max(b.DM_Pay_Method_Most_Recent_101_Payment) not in (102, 103, 105, 108 ) , 1, 0) as 'PMT_CASHOT'
, max(b.Motiv_sid_First_101_dn) as 'Motiv_sid_First_101_dn'

from 
(
select  
a.Donation_Donor_Id
, Dn.donation_id
, dn.Donation_Deposit_Date
, iif(dn.donation_id = a.min_dn_id, dn.Donation_Deposit_Date, null) as 'DM_Date_First_101_Payment'
, iif(dn.donation_id = a.max_dn_id, dn.Donation_Deposit_Date, null) as 'DM_Date_Most_Recent_101_Payment'
, a.DM_Amt_Total_101_Giving_Lifetime
, iif(dn.donation_id = a.max_dn_id, dn.donation_method_sid, null) as 'DM_Pay_Method_Most_Recent_101_Payment'
, iif(dn.donation_id = a.min_dn_id, dn.Donation_motivation_sid, null) as 'Motiv_sid_First_101_dn'
from (
select dn.donation_donor_id 
, max (dn.donation_deposit_date) as 'last_101_pmt_dt'
, min (dn.Donation_Deposit_Date)  as 'first_101_pmt_dt'
, sum (dn.Donation_Amount) as 'DM_Amt_Total_101_Giving_Lifetime'
, max (dn.donation_id) as 'max_dn_id'
, min (dn.DOnation_id) as 'min_dn_id'

from ADOBE.RAW.F_DONATION Dn WHERE dn.Donation_Adjustment_Reason_Sid = 0
and dn.donation_income_type_for_donor_metrics_sid = 101
and Dn.Donation_Donor_Id IN  (SELECT a.Donation_Donor_Id FROM  [SPSS_Sandbox].[dbo].[LTDV_SCRIPT_FY21_25072022_DNR_LABEL] a WHERE A.FY21_DNR_LABEL = 'PLEDGER') --14351 donors
group by dn.Donation_Donor_Id) a
join ADOBE.RAW.F_DONATION Dn on (dn.donation_id in (a.min_dn_id , a.max_dn_id))
) b
group by b.Donation_Donor_Id , b.DM_Amt_Total_101_Giving_Lifetime
) c
join ADOBE.RAW.D_MOTIVATION mot on mot.Motivation_Sid = c.Motiv_sid_First_101_dn
) feat0 on feat0.Donor_id = d.donor_id

WHERE dm.DM_Donor_Id in (SELECT a.Donation_Donor_Id FROM  [SPSS_Sandbox].[dbo].[LTDV_SCRIPT_FY21_25072022_DNR_LABEL] a WHERE A.FY21_DNR_LABEL = 'PLEDGER')


--Warning: Null value is eliminated by an aggregate or other SET operation.
--(14254 rows affected)
--Completion time: 2022-10-27T17:36:19.2601699-04:00


--in summary: 
select * from #spf_spr 
select * into [SPSS_Sandbox].[dbo].[LTDV_FY21_PL_DB_FOR_TENURE_ESTIMATION] from #spf_plr order by Donor_Id
--(14254 rows affected)
--Completion time: 2022-10-28T04:10:25.2925905-04:00

select top 0 * from #spf_spr
select top 0 * from #spf_plr

--=======================================================================================================================================
--after running the python script for generating the predictions for future tenure 
--results are stored in 
--copy of dataframes into sql tables took hours but the script did the job ...
-- the time it takes is very much related with the size of the table ... 

--=======================================================================================================================================
  select * from [SPSS_Sandbox].[WORLDVISION\Hidalgo].[FY21_PLR_tenure_Q001_Q100] order by Donor_Id --14254 records

  select * from [SPSS_Sandbox].[WORLDVISION\Hidalgo].[FY21_SPR_tenure_Q001_Q100] order by Donor_Id --271504 records


--=======================================================================================================================================
--the oncoming code must be run only after verifying the initial block of code has been run successfully and both 
-- [SPSS_Sandbox].[WORLDVISION\USER].[FYNN_SPR_tenure_Q001_Q100] 
-- [SPSS_Sandbox].[WORLDVISION\USER].[FYNN_PLR_tenure_Q001_Q100] 
-- have been correctly generated
--=======================================================================================================================================


--======================================================================================================================================
--BLOCK 2: CALCULATION OF PAST TENURE FOR SPONSORS, PLEDGERS AND SINGLE GIFT DONORS (HISTORICAL UNADJUSTED BY STATUS OF NEW, REACtivated, old, upgraded 
--======================================================================================================================================

-- SPONSORS AND PLEDGERS

-- INGREDIENTS ARE 
--     SPONSORS
-- (1) LABEL OF SPONSOR / PLEDGER
-- (2) SPR_CATEGORY
-- (3) DATE LAST SPONSORSHIP / PLDG PAYMENT
-- (4) SP_XLED_BY_EOP_FLG / PL_XLED_BY_EOP_FLG
-- (5) MIN FULFILLMENT DATE EVER (OLD, UPGRADED)
-- (6) MIN FULFILLMENT DATE IN FY21 (NEW, REACTIVATED)

--TABLES FOR CALCULATION

-- (1) select * from [spss_sandbox].[dbo].[LTDV_SCRIPT_FY21_25072022_DNR_LABEL] WHERE FY21_DNR_LABEL IN ('SPONSOR' , 'PLEDGER')

-- (2) SELECT  DONATION_DONOR_ID , FY21_SPR_CAT_07_2022 from [SPSS_Sandbox].[dbo].[LTDV_SCRIPT_25072022_FY21_SPR_CAT_demo] 
--     SELECT DONATION_DONOR_ID , FY21_PLR_CAT_07_2022 from [SPSS_Sandbox].[dbo].[LTDV_SCRIPT_25072022_FY21_PLR_CAT_demo] 
--     SELECT DONATION_DONOR_ID , LTSV_FY21_SG_CAT from [SPSS_Sandbox].[dbo].[LTDV_SCRIPT_25072022_FY21_SGR_CAT_DEMO]

-- 3)   

--THIS PROVIDES  DATE LAST SPONSORSHIP PAYMENT

CREATE OR REPLACE TABLE SP_PL_LAST_DN_F AS 
select distinct dn.donation_donor_id
, CAST(max(dn.donation_deposit_date) as DATE) AS Last_DN,
A.DNR_LABEL
from ADOBE.RAW.F_DONATION dn
JOIN pred_model_feature.LTDV_SCRIPT_DNR_LABEL A
ON dn.DONATION_DONOR_ID=A.Donation_Donor_Id
WHERE dn.Donation_Adjustment_Reason_Sid = 0 
and  dn.donation_deposit_date <= '2021-09-30'
and ((dn.Donation_Income_Type_for_Donor_Metrics_Sid =102 and A.DNR_LABEL='SPONSOR')
OR (dn.Donation_Income_Type_for_Donor_Metrics_Sid =101 and A.DNR_LABEL='PLEDGER'))
group by dn.Donation_Donor_Id ,DNR_LABEL

-- (4) SP_XLED_BY_EOP_FLG / PL_XLED_BY_EOP_FLG COMES FROM 
CREATE OR REPLACE TABLE  SP_PL_DM_EXTRACT_F AS
SELECT A.DONATION_DONOR_ID  
--, DM.DM_DONOR_ID
, A.DNR_LABEL
, DM.DM_Qty_Fulfilled_and_Active_Non_Sponsorship_Ongoing_Pledges
, IFF(DM.DM_Qty_Fulfilled_and_Active_Sponsorship_Pledges = 0 , 1, 0 ) AS SP_XLED_BY_EOP_FLG
, IFF(DM.DM_Qty_Fulfilled_and_Active_Non_Sponsorship_Ongoing_Pledges = 0 , 1, 0 ) AS PL_XLED_BY_EOP_FLG
, cast(DM.DM_Dt_Most_Recent_Sponsorship_Gift as DATE) AS Last_SPP_DN
FROM  pred_model_feature.LTDV_SCRIPT_DNR_LABEL A 
JOIN ADOBE.RAW.DONOR_METRICS_HISTORY DM ON A.Donation_Donor_Id = DM.DM_Donor_Id 
WHERE DM.DM_Snapshot_Dt = '2021-09-30'
AND A.DNR_LABEL IN ('SPONSOR' , 'PLEDGER')   --273135 + 14350 --9 MISSING RECORDS DUE TO PAYMENT ON THE LAST DAY NOT PROCESSED ON TIME
--(287485 rows affected)
--Completion time: 2022-10-31T12:59:01.0787770-04:00


-- (5) MIN FULFILLMENT DATE EVER (OLD, UPGRADED)
-- (6) MIN FULFILLMENT DATE IN FY21 (NEW, REACTIVATED)
create or replace  table SP_PL_FULF_DATES_F as  select DONATION_DONOR_ID
,dnr_labEL
, MIN_PLEDGE_FULFILLMENT_DATE AS MIN_FULF_DT_EVER
, IFF(MIN_PLEDGE_FULFILLMENT_DATE BETWEEN '2020-10-01' AND '2021-09-30', MIN_PLEDGE_FULFILLMENT_DATE, NULL) AS MIN_FULF_DT

from pred_model_feature.LTDV_ADDITIONAL_FEATURES A
GROUP BY DONATION_DONOR_ID,dnr_label,MIN_PLEDGE_FULFILLMENT_DATE

--APPLYING THE INFORMATION TO GET THE historical TENURE (IN DAYS) FOR SPONSORS AND PLEDGERS ... 

CREATE OR REPLACE TABLE SPR_PLR_PAST_TNR_HISTORICAL as
SELECT A.DONATION_DONOR_ID,A.DNR_LABEL
, CASE
WHEN A.DNR_LABEL = 'SPONSOR' AND B.SP_XLED_BY_EOP_FLG = 0 THEN  DATEDIFF(DD,  D.MIN_FULF_DT_EVER , '2021-10-09')
WHEN A.DNR_LABEL = 'SPONSOR' AND B.SP_XLED_BY_EOP_FLG = 1 THEN  DATEDIFF(DD,  D.MIN_FULF_DT_EVER , E.Last_DN)
WHEN A.DNR_LABEL = 'PLEDGER' AND B.PL_XLED_BY_EOP_FLG = 0 THEN  DATEDIFF(DD,  D.MIN_FULF_DT_EVER , '2021-10-09')
WHEN A.DNR_LABEL = 'PLEDGER' AND B.PL_XLED_BY_EOP_FLG = 1 THEN  DATEDIFF(DD,  D.MIN_FULF_DT_EVER , E.Last_DN)
ELSE DATEDIFF(DD, D.MIN_FULF_DT_EVER , '2021-10-09') END AS PAST_TNR_DD_HISTORICAL

--INTO QUARTERS
, ROUND(4 * (
 CASE
WHEN A.DNR_LABEL = 'SPONSOR' AND B.SP_XLED_BY_EOP_FLG = 0 THEN  DATEDIFF(DD,  D.MIN_FULF_DT_EVER , '2021-10-09')
WHEN A.DNR_LABEL = 'SPONSOR' AND B.SP_XLED_BY_EOP_FLG = 1 THEN  DATEDIFF(DD,  D.MIN_FULF_DT_EVER , E.Last_DN)
WHEN A.DNR_LABEL = 'PLEDGER' AND B.PL_XLED_BY_EOP_FLG = 0 THEN  DATEDIFF(DD,  D.MIN_FULF_DT_EVER , '2021-10-09')
WHEN A.DNR_LABEL = 'PLEDGER' AND B.PL_XLED_BY_EOP_FLG = 1 THEN  DATEDIFF(DD,  D.MIN_FULF_DT_EVER , E.Last_DN)
ELSE DATEDIFF(DD, D.MIN_FULF_DT_EVER , '2021-10-09') END 
)/365.25 , 1) AS PAST_TNR_QQ_HISTORICAL

-- INTO YEARS (3YEAR OF 364=1YR 0F 365)/4 = 365.25
, ROUND( 
(CASE
WHEN A.DNR_LABEL = 'SPONSOR' AND B.SP_XLED_BY_EOP_FLG = 0 THEN  DATEDIFF(DD,  D.MIN_FULF_DT_EVER , '2021-10-09')
WHEN A.DNR_LABEL = 'SPONSOR' AND B.SP_XLED_BY_EOP_FLG = 1 THEN  DATEDIFF(DD,  D.MIN_FULF_DT_EVER , E.Last_DN)
WHEN A.DNR_LABEL = 'PLEDGER' AND B.PL_XLED_BY_EOP_FLG = 0 THEN  DATEDIFF(DD,  D.MIN_FULF_DT_EVER , '2021-10-09')
WHEN A.DNR_LABEL = 'PLEDGER' AND B.PL_XLED_BY_EOP_FLG = 1 THEN  DATEDIFF(DD,  D.MIN_FULF_DT_EVER , E.Last_DN)
ELSE DATEDIFF(DD, D.MIN_FULF_DT_EVER , '2021-10-09') END 
)/365.25 , 1) AS PAST_TNR_YY_HISTORICAL

from pred_model_feature.LTDV_ADDITIONAL_FEATURES A
LEFT JOIN SP_PL_DM_EXTRACT_F B ON B.DONATION_DONOR_ID = A.DONATION_DONOR_ID                       
JOIN SP_PL_FULF_DATES_F D ON D.DONATION_DONOR_ID = A.DONATION_DONOR_ID
JOIN SP_PL_LAST_DN_F E ON E.DONATION_DONOR_ID = A.DONATION_DONOR_ID
WHERE A.DNR_LABEL IN ('PLEDGER','SPONSOR')

--(14350 rows affected)
--Completion time: 2022-10-31T13:02:38.3723555-04:00

--VERIFYING/INSPECTING:
--SELECT * FROM #FY21_SPR_PAST_TNR_HISTORICAL
--SELECT * FROM #FY21_PLR_PAST_TNR_HISTORICAL

--SINGLE GIFT DONORS 
--historical tenure for single givers is calculated as the number of DAYS between his/her first SG and the last day of FY21
--without A reference period of 3 years. 
--then the REFERENCE period goes as dn.Donation_Deposit_Date <='2021-09-30'
--the number of donations can not be used as it is since a unique gift catalogue order is broken in the table as their different components.
--TWO METRICS ARE CALCULATED BUT IT IS THE SECOND ONE DEFINED AS  DATEDIFF(qq,   Min(dn.donation_deposit_date) , '2021-09-30') THAT GETS SELECTED
create or replace table SGR_PAST_TNR_HISTORICAL as
select dn.Donation_Donor_Id,a.DNR_LABEL
, Min(dn.donation_deposit_date) as Oldest_SG
, Max(dn.donation_deposit_date) as Newest_SG
, DATEDIFF(DD, Min(dn.donation_deposit_date) , Max(dn.donation_deposit_date)) as Tnr_1
--TENURE IN DAYS
, DATEDIFF(DD, Min(dn.donation_deposit_date) , '2021-09-30') as PAST_TNR_DD_HISTORICAL

--TENURE IN QUARTERS
, ROUND(4 * DATEDIFF(DD,   Min(dn.donation_deposit_date) , '2021-09-30')/365.25 , 1) as PAST_TNR_QQ_HISTORICAL

--TENURE IN YEARS
, ROUND(DATEDIFF(DD, Min(dn.donation_deposit_date) , '2021-09-30')/365.25 , 1) as PAST_TNR_YY_HISTORICAL

, count(dn.donation_deposit_date) as Q_SG

from adobe.raw.F_Donation dn
join pred_model_feature.LTDV_SCRIPT_DNR_LABEL A
where dn.Donation_Adjustment_Reason_Sid = 0 
and dn.Donation_Income_Type_for_Donor_Metrics_Sid in (103,104,105,106,107,108)
and dn.Donation_Deposit_Date <= '2021-09-30'
AND A.DNR_LABEL = 'SINGLE_GIVER' AND A.FINANCIAL_YEAR='2021'
group by dn.Donation_Donor_Id,DNR_LABEL

--(45647 rows affected)
--Completion time: 2022-10-31T13:15:00.7935785-04:00

--NOW ALL PAST_TENURE_HISTORICAL DATA WILL BE PUT IN A SPSS_SANDBOX PERMANENT TABLE 


CREATE OR REPLACE TABLE LTDV_SCRIPT_PAST_TNR_HISTORICAL_F AS
SELECT H.Donation_Donor_Id AS Donor_ID
,H.DNR_LABEL
, H.PAST_TNR_DD_HISTORICAL
, H.PAST_TNR_QQ_HISTORICAL
, H.PAST_TNR_YY_HISTORICAL
FROM (
SELECT A.Donation_Donor_Id,A.DNR_LABEL, A.PAST_TNR_DD_HISTORICAL, A.PAST_TNR_QQ_HISTORICAL, A.PAST_TNR_YY_HISTORICAL FROM SPR_PLR_PAST_TNR_HISTORICAL A
UNION
SELECT B.DONATION_DONOR_ID,B.DNR_LABEL, B.PAST_TNR_DD_HISTORICAL, B.PAST_TNR_QQ_HISTORICAL, B.PAST_TNR_YY_HISTORICAL FROM SGR_PAST_TNR_HISTORICAL B) H
ORDER BY DONATION_DONOR_ID,DNR_LABEL

--(333141 rows affected)
--Completion time: 2022-10-31T13:33:09.9316609-04:00

--verify/inspect: select * from  [SPSS_Sandbox].[dbo].[LTDV_SCRIPT_FY21_25072022_PAST_TNR_HISTORICAL_DEMO] 

--======================================================================================================================================
--======================================================================================================================================
--CALCULATION OF FUTURE KNOWN TENURE FOR SPONSORS, PLEDGERS AND SINGLE GIFT DONORS (HISTORICAL UNADJUSTED BY STATUS OF NEW, REACtivated, old, upgraded 
--======================================================================================================================================
-- THE CONCEPT OF FUTURE KNOWN TENURE IS DEPENDENT ON WHEN THE CALCULATIONS ARE PERFORMED ... 
--THE FUTURE KNOWN PERIOD COVERS THE PERIOD FROM THE START OF THE fy UNDER STUDY UNTIL THE END OF THE LAST AVAILABLE COMPLETE QUARTER OF DATA.
-- FOR FY21 THIS CORRESPONDS TO THE 4 QUARTERS OF FY22
-- 

-- SPONSORS AND PLEDGERS

-- INGREDIENTS ARE 
--     SPONSORS
-- (1) LABEL OF SPONSOR / PLEDGER
-- (2) SPR_CATEGORY
-- (3) DATE LAST SPONSORSHIP / PLDG PAYMENT
-- (4) SP_XLED_BY_EOP_FLG / PL_XLED_BY_EOP_FLG
-- (5) MIN FULFILLMENT DATE EVER (OLD, UPGRADED)
-- (6) MIN FULFILLMENT DATE IN FY21 (NEW, REACTIVATED)

--TABLES FOR CALCULATION

-- (1) select * from [spss_sandbox].[dbo].[LTDV_SCRIPT_FY21_25072022_DNR_LABEL] WHERE FY21_DNR_LABEL IN ('SPONSOR' , 'PLEDGER')

-- (2) SELECT  DONATION_DONOR_ID , FY21_SPR_CAT_07_2022 from [SPSS_Sandbox].[dbo].[LTDV_SCRIPT_25072022_FY21_SPR_CAT_demo] 
--     SELECT DONATION_DONOR_ID , FY21_PLR_CAT_07_2022 from [SPSS_Sandbox].[dbo].[LTDV_SCRIPT_25072022_FY21_PLR_CAT_demo] 
--     SELECT DONATION_DONOR_ID , LTSV_FY21_SG_CAT from [SPSS_Sandbox].[dbo].[LTDV_SCRIPT_25072022_FY21_SGR_CAT_DEMO]

-- 3)   
-- CHECK FOR CANCELLATION DURING FUTURE KNOWN PERIOD ... 

CREATE OR REPLACE TABLE SP_PL_DM_EXTRACT_FKP AS
SELECT A.DONATION_DONOR_ID 
, A.DNR_LABEL
, DM.DM_Qty_Fulfilled_and_Active_Sponsorship_Pledges
, IFF(DM.DM_Qty_Fulfilled_and_Active_Sponsorship_Pledges = 0 , 1, 0 ) AS SP_XLED_BY_EOP_FLG
, CAST(DM.DM_Dt_Most_Recent_Sponsorship_Gift AS DATE) AS Last_SPP_DN
, DM.DM_Qty_Fulfilled_and_Active_Non_Sponsorship_Ongoing_Pledges
, IFF(DM.DM_Qty_Fulfilled_and_Active_Non_Sponsorship_Ongoing_Pledges = 0 , 1, 0 ) AS PL_XLED_BY_EOP_FLG
FROM  PRED_MODEL_FEATURE.LTDV_SCRIPT_DNR_LABEL A 
JOIN adobe.raw.DONOR_METRICS_HISTORY DM ON A.Donation_Donor_Id = DM.DM_Donor_Id 
WHERE DM.DM_Snapshot_Dt = '2022-09-30'
AND A.DNR_LABEL IN ('SPONSOR' , 'PLEDGER')  

--(287494 rows affected)
--Completion time: 2022-11-17T03:02:20.1904752-05:00

SELECT * FROM #SP_PL_DM_EXTRACT_FKP WHERE FY21_DNR_LABEL = 'SPONSOR'

--THIS PROVIDES  DATE LAST SPONSORSHIP PAYMENT OF FY22 (THERE IS THE POSSIBILITY OF NO DONATIONS DURING FY22)

CREATE OR REPLACE TABLE SP_PL_KNOWN_FUTURE_TNR AS (
select dn.donation_donor_id
, A.DNR_LABEL
,D.SP_XLED_BY_EOP_FLG  --1 MEANS DONOR DID NOT HAVE SPONSORSHIPS BY THE END OF FY22
,D.PL_XLED_BY_EOP_FLG
,CAST(max(dn.donation_deposit_date) AS DATE) AS Last_SP_PL_DN
, IFF (max(dn.donation_deposit_date) <= '2021-10-01' , 1,0) AS DID_NOT_GAVE_IN_FY22 --NO FY22 SPP DNTN IN RECORD

--TENURE_KNOWN_FUTURE
, CASE
WHEN max(dn.donation_deposit_date) < '2021-10-01' THEN 0   --NO FY22 SPP DNTN IN RECORD? THEN TENURE = 0
WHEN A.DNR_LABEL='SPONSOR' AND max(dn.donation_deposit_date) >= '2021-10-01' AND D.SP_XLED_BY_EOP_FLG = 1 THEN DATEDIFF(q, '2021-10-01', max(dn.donation_deposit_date)) --DNTN IN RECORD BUT XLED? THEN START TO LAST DNTN
WHEN A.DNR_LABEL='PLEDGER' AND max(dn.donation_deposit_date) >= '2021-10-01' AND D.PL_XLED_BY_EOP_FLG = 1 THEN DATEDIFF(q, '2021-10-01', max(dn.donation_deposit_date)) --DNTN IN RECORD BUT XLED? THEN START TO LAST DNTN
ELSE DATEDIFF(q, '2021-10-01', '2022-10-01') END AS TNR_KNOWN_FUTURE   --DNTN IN RECORD AND DID NOT CANCEL THEN 4

--UNIT SUPPORT COST PER DONOR PER QQ IS 2.71
, CASE
WHEN max(dn.donation_deposit_date) < '2021-10-01' THEN 0   --NO FY22 PLD DNTN IN RECORD? THEN TENURE = 0
WHEN A.DNR_LABEL='SPONSOR' AND max(dn.donation_deposit_date) >= '2021-10-01' AND D.SP_XLED_BY_EOP_FLG = 1 
THEN ROUND((SELECT UNIT_SUPPORT_COST_PER_QQ FROM PRED_MODEL_FEATURE.LTDV_SCRIPT_FINANCE_SUPPORT_COST_PER_DONOR_PER_QUARTER)*DATEDIFF(q, '2021-10-01', max(dn.donation_deposit_date)),0) --DNTN IN RECORD BUT XLED? THEN START TO LAST DNTN
WHEN A.DNR_LABEL='PLEDGER' AND max(dn.donation_deposit_date) >= '2021-10-01' AND D.PL_XLED_BY_EOP_FLG = 1
THEN ROUND((SELECT UNIT_SUPPORT_COST_PER_QQ FROM PRED_MODEL_FEATURE.LTDV_SCRIPT_FINANCE_SUPPORT_COST_PER_DONOR_PER_QUARTER)*DATEDIFF(q, '2021-10-01', max(dn.donation_deposit_date)),0)
ELSE ROUND((SELECT UNIT_SUPPORT_COST_PER_QQ FROM PRED_MODEL_FEATURE.LTDV_SCRIPT_FINANCE_SUPPORT_COST_PER_DONOR_PER_QUARTER)*DATEDIFF(q, '2021-10-01', '2022-10-01'),0) END AS SUPPORT_COST_KNOWN_FUTURE  --DNTN IN RECORD AND DID NOT CANCEL THEN 4
----UNIT RETENTION COST PER SPONSOR PER QQ IS 2.71
, CASE WHEN A.DNR_LABEL='SPONSOR' THEN (
CASE WHEN max(dn.donation_deposit_date) < '2021-10-01' THEN 0   
WHEN max(dn.donation_deposit_date) >= '2021-10-01' AND D.SP_XLED_BY_EOP_FLG = 1 
THEN ROUND((SELECT UNIT_SP_RETENTION_COST_PER_QQ  FROM  PRED_MODEL_FEATURE.LTDV_SCRIPT_FINANCE_SP_PL_RETENTION_COST_PER_DONOR_PER_QUARTER)*DATEDIFF(q, '2021-10-01', max(dn.donation_deposit_date)),0)
ELSE  ROUND((SELECT UNIT_SP_RETENTION_COST_PER_QQ  FROM  PRED_MODEL_FEATURE.LTDV_SCRIPT_FINANCE_SP_PL_RETENTION_COST_PER_DONOR_PER_QUARTER)*DATEDIFF(q, '2021-10-01', '2022-10-01'),0)
END)
WHEN A.DNR_LABEL='PLEDGER' THEN (
CASE WHEN max(dn.donation_deposit_date) < '2021-10-01' THEN 0   
WHEN max(dn.donation_deposit_date) >= '2021-10-01' AND D.PL_XLED_BY_EOP_FLG = 1 
THEN ROUND((SELECT UNIT_PL_RETENTION_COST_PER_QQ  FROM  PRED_MODEL_FEATURE.LTDV_SCRIPT_FINANCE_SP_PL_RETENTION_COST_PER_DONOR_PER_QUARTER)*DATEDIFF(q, '2021-10-01', max(dn.donation_deposit_date)),0)
ELSE  ROUND((SELECT UNIT_PL_RETENTION_COST_PER_QQ  FROM  PRED_MODEL_FEATURE.LTDV_SCRIPT_FINANCE_SP_PL_RETENTION_COST_PER_DONOR_PER_QUARTER)*DATEDIFF(q, '2021-10-01', '2022-10-01'),0)
END) END AS RET_COST_KNOWN_FUTURE
from ADOBE.RAW.F_DONATION dn
JOIN PRED_MODEL_FEATURE.LTDV_SCRIPT_DNR_LABEL A ON A.Donation_Donor_Id = DN.Donation_Donor_Id
JOIN (SELECT DONATION_DONOR_ID, SP_XLED_BY_EOP_FLG,PL_XLED_BY_EOP_FLG FROM SP_PL_DM_EXTRACT_FKP) D ON D.Donation_Donor_Id = A.Donation_Donor_Id
where dn.Donation_Adjustment_Reason_Sid = 0
and dn.donation_deposit_date BETWEEN '2020-10-01' AND '2022-09-30'
and ((dn.Donation_Income_Type_for_Donor_Metrics_Sid =102 and A.DNR_LABEL='SPONSOR')
OR (dn.Donation_Income_Type_for_Donor_Metrics_Sid =101 and A.DNR_LABEL='PLEDGER'))
group by dn.donation_donor_id, D.SP_XLED_BY_EOP_FLG,D.PL_XLED_BY_EOP_FLG , A.DNR_LABEL
order by dN.Donation_Donor_id )

--==========================================================================================

--NOW WE NEED A CALCULATION OF THE INCOME PER SPP, PL AND SG PER DONOR DURING THE KNOWN FUTURE PERIOD (FY22)

--THE NUMBER OF GIFTS PER DONOR WILL BE MULTIPLIED BY THE UNIT COST PER SG (22.12) TO GET THE ESTIMATION OF SG COST PER DONOR
--UNIT SINGLE  GIFT COST IN ==> FY21_UNIT_COST_SG = 22.12


CREATE OR REPLACE TABLE FUTURE_KNOWN_INC AS
SELECT DN.Donation_Donor_Id
, LAB.DNR_LABEL
, SUM(IFF(DN.Donation_Income_Type_for_Donor_Metrics_Sid IN (102),DN.DONATION_AMOUNT,0)) AS FUT_KNOWN_INC_SP
, SUM(IFF(DN.Donation_Income_Type_for_Donor_Metrics_Sid IN (101),DN.DONATION_AMOUNT,0)) AS FUT_KNOWN_INC_PL
, SUM(IFF(DN.Donation_Income_Type_for_Donor_Metrics_Sid IN (103,104,105,106,107,108),DN.DONATION_AMOUNT,0)) AS FUT_KNOWN_INC_SG
, SUM(IFF(DN.Donation_Income_Type_for_Donor_Metrics_Sid IN (103,104,105,106,107,108),1,0)) AS NBR_FUT_KNOWN_SG

--SG_COST_PER_DONOR
,  ROUND(SUM(IFF(DN.Donation_Income_Type_for_Donor_Metrics_Sid IN (103,104,105,106,107,108),1,0)) 
   * (SELECT A.UNIT_COST_SG FROM PRED_MODEL_FEATURE.LTDV_SCRIPT_FINANCE_SG_COST_TOTAL_AND_UNIT A),0) AS SG_RET_COST
FROM ADOBE.RAW.F_DONATION DN
JOIN PRED_MODEL_FEATURE.LTDV_SCRIPT_DNR_LABEL LAB ON LAB.Donation_Donor_Id = DN.Donation_Donor_Id
WHERE DN.Donation_Adjustment_Reason_Sid = 0
AND DN.Donation_Deposit_Date BETWEEN '2021-10-01' AND '2022-10-01' 
AND DN.Donation_Income_Type_for_Donor_Metrics_Sid IN (101,102,103,104,105,106,107,108)
AND LAB.DNR_LABEL IN ('SPONSOR' , 'PLEDGER', 'SINGLE_GIVER')
GROUP BY DN.Donation_Donor_Id , LAB.DNR_LABEL


--(276053 rows affected)
--Completion time: 2022-11-17T06:08:59.6112234-05:00

--select * from #FUTURE_KNOWN_INC
--PUTTING THIS INFORMATION ALTOGETHER TO BE ADDED TO THE GENERAL TABLE WHEN THE TIME COES ... 


CREATE OR REPLACE TABLE LTDV_SCRIPT_Known_Future_Metrics as
SELECT A.Donation_Donor_Id	
, A.DNR_LABEL	AS Block
, B.TNR_KNOWN_FUTURE AS TNR_KNOWN_FUTURE_QQ
, ROUND(B.TNR_KNOWN_FUTURE/4,0) AS TNR_KNOWN_FUTURE_YY
, IFNULL(A.FUT_KNOWN_INC_SP	, 0) AS FUT_KNOWN_INCOME_SP
, IFNULL(A.FUT_KNOWN_INC_PL	, 0) AS FUT_KNOWN_INCOME_PL
, IFNULL(A.FUT_KNOWN_INC_SG , 0) AS FUT_KNOWN_INCOME_SG
, IFNULL(A.SG_RET_COST, 0) AS SG_RET_COST
, B.RET_COST_KNOWN_FUTURE
, B.SUPPORT_COST_KNOWN_FUTURE
, IFNULL(A.FUT_KNOWN_INC_SP	, 0)  +  IFNULL(A.FUT_KNOWN_INC_PL	, 0)  +  IFNULL(A.FUT_KNOWN_INC_SG , 0) AS FUT_KNOWN_INCOME
, IFNULL(A.SG_RET_COST, 0) + IFNULL(B.RET_COST_KNOWN_FUTURE,0) + IFNULL(B.SUPPORT_COST_KNOWN_FUTURE,0) AS FUT_KNOWN_COST
, IFNULL(A.NBR_FUT_KNOWN_SG , 0) AS NBR_FUT_KNOWN_SG
FROM FUTURE_KNOWN_INC A
LEFT JOIN SP_PL_TNR_KNOWN_FUTURE B ON B.Donation_Donor_Id = A.Donation_Donor_Id

--========================================================================================================================================

-- BLOCK 3: ESTIMATION OF FUTURE TENURE FOR SPONSORS, PLEDGERS AND SINGLE GIFT DONORS (HISTORICAL UNADJUSTED BY STATUS OF NEW, REACtivated, old, upgraded 
--================================================================================================================================

--ESTIMATING FUTURE TENURE FOR ALL SPONSORS IN THE FY21 LTDV PROJECT
--THE PREDICTIONS COMING FROM THE APPLICATION OF THE SURVIVAL MODEL TO THE SPONSORS AND PLEDGERS IN THE LTDV
--ARE NOT OTHER THING THAT THE HAZARD RATES PER DONORS PER PERIOD ... 

--SPONSORS: bIG TABLE 101 FEATURES FOR 271504 DONORS = 27421904 RECORDS!! 27.4 MILLION RECORDS!
select count(Donor_id) from [SPSS_Sandbox].[WORLDVISION\Hidalgo].[FY21_SPR_tenure_Q001_Q100] --271504 records ...

select top 10 *	from [SPSS_Sandbox].[WORLDVISION\Hidalgo].[FY21_SPR_tenure_Q001_Q100] TDB order by Donor_Id
--

--PLEDGERS: SAME 101 FEATURES FOR 14254 DONORS = 1439654 RECORDS 1.44M RECORDS!
select * from [SPSS_Sandbox].[WORLDVISION\Hidalgo].[FY21_PLR_tenure_Q001_Q100] order by Donor_Id

--The original estimation does not incorporate any weight to account for the different historical LOF experienced
-- by sponsors ... a simple factor based on an max tenure expectancy of 180 quarters is defined to account for 
-- such event ... time passes and the limited amount of time periods is consumed increasing the probability of a negative event.
-- THIS FACT COMES FROM THE OBSERVATION OF THE BUSINESS RATHER THAN THE DATA THAT GENERATED THE PREDICTIONS.
-- a weight factor equivalent to tenure/180 must be applied to any observation
-- what is the best way to do it in sql is the point
-- every donor has an specific tenure ... the factor does not apply to the whole column but to the row
-- Weight seems to be to high to allow for scoring and selection ...
--========================================================
--ADJUSTING THE RAW HAZARD RATES FROM THE MODEL: /180 WEIGHT APPLIED TO RAW HAZARDS
--========================================================
--SPONSORS:
CREATE OR REPLACE TABLE SP_PL_ATenure_001_100 AS
SELECT * FROM (SELECT tdb.Donor_Id,s.DNR_LABEL
, tdb.Q_001*179/180 as AQ001
, tdb.Q_002*178/180 as AQ002
, tdb.Q_003*177/180 as AQ003
, tdb.Q_004*176/180 as AQ004
, tdb.Q_005*175/180 as AQ005
, tdb.Q_006*174/180 as AQ006
, tdb.Q_007*173/180 as AQ007
, tdb.Q_008*172/180 as AQ008
, tdb.Q_009*171/180 as AQ009
, tdb.Q_010*170/180 as AQ010
, tdb.Q_011*169/180 as AQ011
, tdb.Q_012*168/180 as AQ012
, tdb.Q_013*167/180 as AQ013
, tdb.Q_014*166/180 as AQ014
, tdb.Q_015*165/180 as AQ015
, tdb.Q_016*164/180 as AQ016
, tdb.Q_017*163/180 as AQ017
, tdb.Q_018*162/180 as AQ018
, tdb.Q_019*161/180 as AQ019
, tdb.Q_020*160/180 as AQ020
, tdb.Q_021*159/180 as AQ021
, tdb.Q_022*158/180 as AQ022
, tdb.Q_023*157/180 as AQ023
, tdb.Q_024*156/180 as AQ024
, tdb.Q_025*155/180 as AQ025
, tdb.Q_026*154/180 as AQ026
, tdb.Q_027*153/180 as AQ027
, tdb.Q_028*152/180 as AQ028
, tdb.Q_029*151/180 as AQ029
, tdb.Q_030*150/180 as AQ030
, tdb.Q_031*149/180 as AQ031
, tdb.Q_032*148/180 as AQ032
, tdb.Q_033*147/180 as AQ033
, tdb.Q_034*146/180 as AQ034
, tdb.Q_035*145/180 as AQ035
, tdb.Q_036*144/180 as AQ036
, tdb.Q_037*143/180 as AQ037
, tdb.Q_038*142/180 as AQ038
, tdb.Q_039*141/180 as AQ039
, tdb.Q_040*140/180 as AQ040
, tdb.Q_041*139/180 as AQ041
, tdb.Q_042*138/180 as AQ042
, tdb.Q_043*137/180 as AQ043
, tdb.Q_044*136/180 as AQ044
, tdb.Q_045*135/180 as AQ045
, tdb.Q_046*134/180 as AQ046
, tdb.Q_047*133/180 as AQ047
, tdb.Q_048*132/180 as AQ048
, tdb.Q_049*131/180 as AQ049
, tdb.Q_050*130/180 as AQ050
, tdb.Q_051*129/180 as AQ051
, tdb.Q_052*128/180 as AQ052
, tdb.Q_053*127/180 as AQ053
, tdb.Q_054*126/180 as AQ054
, tdb.Q_055*125/180 as AQ055
, tdb.Q_056*124/180 as AQ056
, tdb.Q_057*123/180 as AQ057
, tdb.Q_058*122/180 as AQ058
, tdb.Q_059*121/180 as AQ059
, tdb.Q_060*120/180 as AQ060
, tdb.Q_061*119/180 as AQ061
, tdb.Q_062*118/180 as AQ062
, tdb.Q_063*117/180 as AQ063
, tdb.Q_064*116/180 as AQ064
, tdb.Q_065*115/180 as AQ065
, tdb.Q_066*114/180 as AQ066
, tdb.Q_067*113/180 as AQ067
, tdb.Q_068*112/180 as AQ068
, tdb.Q_069*111/180 as AQ069
, tdb.Q_070*110/180 as AQ070
, tdb.Q_071*109/180 as AQ071
, tdb.Q_072*108/180 as AQ072
, tdb.Q_073*107/180 as AQ073
, tdb.Q_074*106/180 as AQ074
, tdb.Q_075*105/180 as AQ075
, tdb.Q_076*104/180 as AQ076
, tdb.Q_077*103/180 as AQ077
, tdb.Q_078*102/180 as AQ078
, tdb.Q_079*101/180 as AQ079
, tdb.Q_080*100/180 as AQ080
, tdb.Q_081*99/180 as AQ081
, tdb.Q_082*98/180 as AQ082
, tdb.Q_083*97/180 as AQ083
, tdb.Q_084*96/180 as AQ084
, tdb.Q_085*95/180 as AQ085
, tdb.Q_086*94/180 as AQ086
, tdb.Q_087*93/180 as AQ087
, tdb.Q_088*92/180 as AQ088
, tdb.Q_089*91/180 as AQ089
, tdb.Q_090*90/180 as AQ090
, tdb.Q_091*89/180 as AQ091
, tdb.Q_092*88/180 as AQ092
, tdb.Q_093*87/180 as AQ093
, tdb.Q_094*86/180 as AQ094
, tdb.Q_095*85/180 as AQ095
, tdb.Q_096*84/180 as AQ096
, tdb.Q_097*83/180 as AQ097
, tdb.Q_098*82/180 as AQ098
, tdb.Q_099*81/180 as AQ099
, tdb.Q_100*80/180 as AQ100
from PRED_MODEL_SCORE.SPR_TENURE_Q001_Q100 tdb
join PRED_MODEL_FEATURE.LTDV_SCRIPT_PAST_TNR_HISTORICAL_F s on s.Donor_ID = tdb.Donor_id

UNION ALL 

SELECT tdb.Donor_Id,s.DNR_LABEL
, tdb.Q_001*179/180 as AQ001
, tdb.Q_002*178/180 as AQ002
, tdb.Q_003*177/180 as AQ003
, tdb.Q_004*176/180 as AQ004
, tdb.Q_005*175/180 as AQ005
, tdb.Q_006*174/180 as AQ006
, tdb.Q_007*173/180 as AQ007
, tdb.Q_008*172/180 as AQ008
, tdb.Q_009*171/180 as AQ009
, tdb.Q_010*170/180 as AQ010
, tdb.Q_011*169/180 as AQ011
, tdb.Q_012*168/180 as AQ012
, tdb.Q_013*167/180 as AQ013
, tdb.Q_014*166/180 as AQ014
, tdb.Q_015*165/180 as AQ015
, tdb.Q_016*164/180 as AQ016
, tdb.Q_017*163/180 as AQ017
, tdb.Q_018*162/180 as AQ018
, tdb.Q_019*161/180 as AQ019
, tdb.Q_020*160/180 as AQ020
, tdb.Q_021*159/180 as AQ021
, tdb.Q_022*158/180 as AQ022
, tdb.Q_023*157/180 as AQ023
, tdb.Q_024*156/180 as AQ024
, tdb.Q_025*155/180 as AQ025
, tdb.Q_026*154/180 as AQ026
, tdb.Q_027*153/180 as AQ027
, tdb.Q_028*152/180 as AQ028
, tdb.Q_029*151/180 as AQ029
, tdb.Q_030*150/180 as AQ030
, tdb.Q_031*149/180 as AQ031
, tdb.Q_032*148/180 as AQ032
, tdb.Q_033*147/180 as AQ033
, tdb.Q_034*146/180 as AQ034
, tdb.Q_035*145/180 as AQ035
, tdb.Q_036*144/180 as AQ036
, tdb.Q_037*143/180 as AQ037
, tdb.Q_038*142/180 as AQ038
, tdb.Q_039*141/180 as AQ039
, tdb.Q_040*140/180 as AQ040
, tdb.Q_041*139/180 as AQ041
, tdb.Q_042*138/180 as AQ042
, tdb.Q_043*137/180 as AQ043
, tdb.Q_044*136/180 as AQ044
, tdb.Q_045*135/180 as AQ045
, tdb.Q_046*134/180 as AQ046
, tdb.Q_047*133/180 as AQ047
, tdb.Q_048*132/180 as AQ048
, tdb.Q_049*131/180 as AQ049
, tdb.Q_050*130/180 as AQ050
, tdb.Q_051*129/180 as AQ051
, tdb.Q_052*128/180 as AQ052
, tdb.Q_053*127/180 as AQ053
, tdb.Q_054*126/180 as AQ054
, tdb.Q_055*125/180 as AQ055
, tdb.Q_056*124/180 as AQ056
, tdb.Q_057*123/180 as AQ057
, tdb.Q_058*122/180 as AQ058
, tdb.Q_059*121/180 as AQ059
, tdb.Q_060*120/180 as AQ060
, tdb.Q_061*119/180 as AQ061
, tdb.Q_062*118/180 as AQ062
, tdb.Q_063*117/180 as AQ063
, tdb.Q_064*116/180 as AQ064
, tdb.Q_065*115/180 as AQ065
, tdb.Q_066*114/180 as AQ066
, tdb.Q_067*113/180 as AQ067
, tdb.Q_068*112/180 as AQ068
, tdb.Q_069*111/180 as AQ069
, tdb.Q_070*110/180 as AQ070
, tdb.Q_071*109/180 as AQ071
, tdb.Q_072*108/180 as AQ072
, tdb.Q_073*107/180 as AQ073
, tdb.Q_074*106/180 as AQ074
, tdb.Q_075*105/180 as AQ075
, tdb.Q_076*104/180 as AQ076
, tdb.Q_077*103/180 as AQ077
, tdb.Q_078*102/180 as AQ078
, tdb.Q_079*101/180 as AQ079
, tdb.Q_080*100/180 as AQ080
, tdb.Q_081*99/180 as AQ081
, tdb.Q_082*98/180 as AQ082
, tdb.Q_083*97/180 as AQ083
, tdb.Q_084*96/180 as AQ084
, tdb.Q_085*95/180 as AQ085
, tdb.Q_086*94/180 as AQ086
, tdb.Q_087*93/180 as AQ087
, tdb.Q_088*92/180 as AQ088
, tdb.Q_089*91/180 as AQ089
, tdb.Q_090*90/180 as AQ090
, tdb.Q_091*89/180 as AQ091
, tdb.Q_092*88/180 as AQ092
, tdb.Q_093*87/180 as AQ093
, tdb.Q_094*86/180 as AQ094
, tdb.Q_095*85/180 as AQ095
, tdb.Q_096*84/180 as AQ096
, tdb.Q_097*83/180 as AQ097
, tdb.Q_098*82/180 as AQ098
, tdb.Q_099*81/180 as AQ099
, tdb.Q_100*80/180 as AQ100
from PRED_MODEL_SCORE.PLR_TENURE_Q001_Q100 tdb
join PRED_MODEL_FEATURE.LTDV_SCRIPT_PAST_TNR_HISTORICAL_F s on s.Donor_ID = tdb.Donor_id
order by 1,2)
--(14254 rows affected)
--Completion time: 2022-10-31T18:09:45.9024439-04:00

select * from [SPSS_Sandbox].[dbo].[FY21_PL_ATenure_001_100] order by Donor_id

--==============================================================================
--IDENTIFYING THE EXPECTED TENURES GIVEN CUT RATES OF 0.50, 0.60

--==============================================================================


 --========================
 --Standard Procedure for getting the Tenure Number per donor
 --Logic is simple and no adjustments for previous LOF taken
 --Cut-off is HZ <= 0.50 OR
 --Cut-off is HZ <= 0.60 OR
 --=======================
CREATE OR REPLACE TABLE SP_PL_ETenure_01 AS
 SELECT t1.Donor_Id,t1.DNR_LABEL,
 GREATEST (
 IFF(AQ001 between 0.4 and 0.999 ,1,0) ,
 IFF(AQ002 between 0.4 and 0.999 ,2,0) ,
 IFF(AQ003 between 0.4 and 0.999 ,3,0) ,
 IFF(AQ004 between 0.4 and 0.999 ,4,0) ,
 IFF(AQ005 between 0.4 and 0.999 ,5,0) ,
 IFF(AQ006 between 0.4 and 0.999 ,6,0) ,
 IFF(AQ007 between 0.4 and 0.999 ,7,0) ,
 IFF(AQ008 between 0.4 and 0.999 ,8,0) ,
 IFF(AQ009 between 0.4 and 0.999 ,9,0) ,
 IFF(AQ010 between 0.4 and 0.999 ,10,0) ,
 IFF(AQ011 between 0.4 and 0.999 ,11,0) ,
 IFF(AQ012 between 0.4 and 0.999 ,12,0) ,
 IFF(AQ013 between 0.4 and 0.999 ,13,0) ,
 IFF(AQ014 between 0.4 and 0.999 ,14,0) ,
 IFF(AQ015 between 0.4 and 0.999 ,15,0) ,
 IFF(AQ016 between 0.4 and 0.999 ,16,0) ,
 IFF(AQ017 between 0.4 and 0.999 ,17,0) ,
 IFF(AQ018 between 0.4 and 0.999 ,18,0) ,
 IFF(AQ019 between 0.4 and 0.999 ,19,0) ,
 IFF(AQ020 between 0.4 and 0.999 ,20,0) ,
 IFF(AQ021 between 0.4 and 0.999 ,21,0) ,
 IFF(AQ022 between 0.4 and 0.999 ,22,0) ,
 IFF(AQ023 between 0.4 and 0.999 ,23,0) ,
 IFF(AQ024 between 0.4 and 0.999 ,24,0) ,
 IFF(AQ025 between 0.4 and 0.999 ,25,0) ,
 IFF(AQ026 between 0.4 and 0.999 ,26,0) ,
 IFF(AQ027 between 0.4 and 0.999 ,27,0) ,
 IFF(AQ028 between 0.4 and 0.999 ,28,0) ,
 IFF(AQ029 between 0.4 and 0.999 ,29,0) ,
 IFF(AQ030 between 0.4 and 0.999 ,30,0) ,
 IFF(AQ031 between 0.4 and 0.999 ,31,0) ,
 IFF(AQ032 between 0.4 and 0.999 ,32,0) ,
 IFF(AQ033 between 0.4 and 0.999 ,33,0) ,
 IFF(AQ034 between 0.4 and 0.999 ,34,0) ,
 IFF(AQ035 between 0.4 and 0.999 ,35,0) ,
 IFF(AQ036 between 0.4 and 0.999 ,36,0) ,
 IFF(AQ037 between 0.4 and 0.999 ,37,0) ,
 IFF(AQ038 between 0.4 and 0.999 ,38,0) ,
 IFF(AQ039 between 0.4 and 0.999 ,39,0) ,
 IFF(AQ040 between 0.4 and 0.999 ,40,0) ,
 IFF(AQ041 between 0.4 and 0.999 ,41,0) ,
 IFF(AQ042 between 0.4 and 0.999 ,42,0) ,
 IFF(AQ043 between 0.4 and 0.999 ,43,0) ,
 IFF(AQ044 between 0.4 and 0.999 ,44,0) ,
 IFF(AQ045 between 0.4 and 0.999 ,45,0) ,
 IFF(AQ046 between 0.4 and 0.999 ,46,0) ,
 IFF(AQ047 between 0.4 and 0.999 ,47,0) ,
 IFF(AQ048 between 0.4 and 0.999 ,48,0) ,
 IFF(AQ049 between 0.4 and 0.999 ,49,0) ,
 IFF(AQ050 between 0.4 and 0.999 ,50,0) ,
 IFF(AQ051 between 0.4 and 0.999 ,51,0) ,
 IFF(AQ052 between 0.4 and 0.999 ,52,0) ,
 IFF(AQ053 between 0.4 and 0.999 ,53,0) ,
 IFF(AQ054 between 0.4 and 0.999 ,54,0) ,
 IFF(AQ055 between 0.4 and 0.999 ,55,0) ,
 IFF(AQ056 between 0.4 and 0.999 ,56,0) ,
 IFF(AQ057 between 0.4 and 0.999 ,57,0) ,
 IFF(AQ058 between 0.4 and 0.999 ,58,0) ,
 IFF(AQ059 between 0.4 and 0.999 ,59,0) ,
 IFF(AQ060 between 0.4 and 0.999 ,60,0) ,
 IFF(AQ061 between 0.4 and 0.999 ,61,0) ,
 IFF(AQ062 between 0.4 and 0.999 ,62,0) ,
 IFF(AQ063 between 0.4 and 0.999 ,63,0) ,
 IFF(AQ064 between 0.4 and 0.999 ,64,0) ,
 IFF(AQ065 between 0.4 and 0.999 ,65,0) ,
 IFF(AQ066 between 0.4 and 0.999 ,66,0) ,
 IFF(AQ067 between 0.4 and 0.999 ,67,0) ,
 IFF(AQ068 between 0.4 and 0.999 ,68,0) ,
 IFF(AQ069 between 0.4 and 0.999 ,69,0) ,
 IFF(AQ070 between 0.4 and 0.999 ,70,0) ,
 IFF(AQ071 between 0.4 and 0.999 ,71,0) ,
 IFF(AQ072 between 0.4 and 0.999 ,72,0) ,
 IFF(AQ073 between 0.4 and 0.999 ,73,0) ,
 IFF(AQ074 between 0.4 and 0.999 ,74,0) ,
 IFF(AQ075 between 0.4 and 0.999 ,75,0) ,
 IFF(AQ076 between 0.4 and 0.999 ,76,0) ,
 IFF(AQ077 between 0.4 and 0.999 ,77,0) ,
 IFF(AQ078 between 0.4 and 0.999 ,78,0) ,
 IFF(AQ079 between 0.4 and 0.999 ,79,0) ,
 IFF(AQ080 between 0.4 and 0.999 ,80,0) ,
 IFF(AQ081 between 0.4 and 0.999 ,81,0) ,
 IFF(AQ082 between 0.4 and 0.999 ,82,0) ,
 IFF(AQ083 between 0.4 and 0.999 ,83,0) ,
 IFF(AQ084 between 0.4 and 0.999 ,84,0) ,
 IFF(AQ085 between 0.4 and 0.999 ,85,0) ,
 IFF(AQ086 between 0.4 and 0.999 ,86,0) ,
 IFF(AQ087 between 0.4 and 0.999 ,87,0) ,
 IFF(AQ088 between 0.4 and 0.999 ,88,0) ,
 IFF(AQ089 between 0.4 and 0.999 ,89,0) ,
 IFF(AQ090 between 0.4 and 0.999 ,90,0) ,
 IFF(AQ091 between 0.4 and 0.999 ,91,0) ,
 IFF(AQ092 between 0.4 and 0.999 ,92,0) ,
 IFF(AQ093 between 0.4 and 0.999 ,93,0) ,
 IFF(AQ094 between 0.4 and 0.999 ,94,0) ,
 IFF(AQ095 between 0.4 and 0.999 ,95,0) ,
 IFF(AQ096 between 0.4 and 0.999 ,96,0) ,
 IFF(AQ097 between 0.4 and 0.999 ,97,0) ,
 IFF(AQ098 between 0.4 and 0.999 ,98,0) ,
 IFF(AQ099 between 0.4 and 0.999 ,99,0) ,
 IFF(AQ100 between 0.4 and 0.999 ,100,0) 
) as ETen_40,
GREATEST(
 IFF(AQ001 between 0.5 and 0.999 ,1,0) ,
 IFF(AQ002 between 0.5 and 0.999 ,2,0) ,
 IFF(AQ003 between 0.5 and 0.999 ,3,0) ,
 IFF(AQ004 between 0.5 and 0.999 ,4,0) ,
 IFF(AQ005 between 0.5 and 0.999 ,5,0) ,
 IFF(AQ006 between 0.5 and 0.999 ,6,0) ,
 IFF(AQ007 between 0.5 and 0.999 ,7,0) ,
 IFF(AQ008 between 0.5 and 0.999 ,8,0) ,
 IFF(AQ009 between 0.5 and 0.999 ,9,0) ,
 IFF(AQ010 between 0.5 and 0.999 ,10,0) ,
 IFF(AQ011 between 0.5 and 0.999 ,11,0) ,
 IFF(AQ012 between 0.5 and 0.999 ,12,0) ,
 IFF(AQ013 between 0.5 and 0.999 ,13,0) ,
 IFF(AQ014 between 0.5 and 0.999 ,14,0) ,
 IFF(AQ015 between 0.5 and 0.999 ,15,0) ,
 IFF(AQ016 between 0.5 and 0.999 ,16,0) ,
 IFF(AQ017 between 0.5 and 0.999 ,17,0) ,
 IFF(AQ018 between 0.5 and 0.999 ,18,0) ,
 IFF(AQ019 between 0.5 and 0.999 ,19,0) ,
 IFF(AQ020 between 0.5 and 0.999 ,20,0) ,
 IFF(AQ021 between 0.5 and 0.999 ,21,0) ,
 IFF(AQ022 between 0.5 and 0.999 ,22,0) ,
 IFF(AQ023 between 0.5 and 0.999 ,23,0) ,
 IFF(AQ024 between 0.5 and 0.999 ,24,0) ,
 IFF(AQ025 between 0.5 and 0.999 ,25,0) ,
 IFF(AQ026 between 0.5 and 0.999 ,26,0) ,
 IFF(AQ027 between 0.5 and 0.999 ,27,0) ,
 IFF(AQ028 between 0.5 and 0.999 ,28,0) ,
 IFF(AQ029 between 0.5 and 0.999 ,29,0) ,
 IFF(AQ030 between 0.5 and 0.999 ,30,0) ,
 IFF(AQ031 between 0.5 and 0.999 ,31,0) ,
 IFF(AQ032 between 0.5 and 0.999 ,32,0) ,
 IFF(AQ033 between 0.5 and 0.999 ,33,0) ,
 IFF(AQ034 between 0.5 and 0.999 ,34,0) ,
 IFF(AQ035 between 0.5 and 0.999 ,35,0) ,
 IFF(AQ036 between 0.5 and 0.999 ,36,0) ,
 IFF(AQ037 between 0.5 and 0.999 ,37,0) ,
 IFF(AQ038 between 0.5 and 0.999 ,38,0) ,
 IFF(AQ039 between 0.5 and 0.999 ,39,0) ,
 IFF(AQ040 between 0.5 and 0.999 ,40,0) ,
 IFF(AQ041 between 0.5 and 0.999 ,41,0) ,
 IFF(AQ042 between 0.5 and 0.999 ,42,0) ,
 IFF(AQ043 between 0.5 and 0.999 ,43,0) ,
 IFF(AQ044 between 0.5 and 0.999 ,44,0) ,
 IFF(AQ045 between 0.5 and 0.999 ,45,0) ,
 IFF(AQ046 between 0.5 and 0.999 ,46,0) ,
 IFF(AQ047 between 0.5 and 0.999 ,47,0) ,
 IFF(AQ048 between 0.5 and 0.999 ,48,0) ,
 IFF(AQ049 between 0.5 and 0.999 ,49,0) ,
 IFF(AQ050 between 0.5 and 0.999 ,50,0) ,
 IFF(AQ051 between 0.5 and 0.999 ,51,0) ,
 IFF(AQ052 between 0.5 and 0.999 ,52,0) ,
 IFF(AQ053 between 0.5 and 0.999 ,53,0) ,
 IFF(AQ054 between 0.5 and 0.999 ,54,0) ,
 IFF(AQ055 between 0.5 and 0.999 ,55,0) ,
 IFF(AQ056 between 0.5 and 0.999 ,56,0) ,
 IFF(AQ057 between 0.5 and 0.999 ,57,0) ,
 IFF(AQ058 between 0.5 and 0.999 ,58,0) ,
 IFF(AQ059 between 0.5 and 0.999 ,59,0) ,
 IFF(AQ060 between 0.5 and 0.999 ,60,0) ,
 IFF(AQ061 between 0.5 and 0.999 ,61,0) ,
 IFF(AQ062 between 0.5 and 0.999 ,62,0) ,
 IFF(AQ063 between 0.5 and 0.999 ,63,0) ,
 IFF(AQ064 between 0.5 and 0.999 ,64,0) ,
 IFF(AQ065 between 0.5 and 0.999 ,65,0) ,
 IFF(AQ066 between 0.5 and 0.999 ,66,0) ,
 IFF(AQ067 between 0.5 and 0.999 ,67,0) ,
 IFF(AQ068 between 0.5 and 0.999 ,68,0) ,
 IFF(AQ069 between 0.5 and 0.999 ,69,0) ,
 IFF(AQ070 between 0.5 and 0.999 ,70,0) ,
 IFF(AQ071 between 0.5 and 0.999 ,71,0) ,
 IFF(AQ072 between 0.5 and 0.999 ,72,0) ,
 IFF(AQ073 between 0.5 and 0.999 ,73,0) ,
 IFF(AQ074 between 0.5 and 0.999 ,74,0) ,
 IFF(AQ075 between 0.5 and 0.999 ,75,0) ,
 IFF(AQ076 between 0.5 and 0.999 ,76,0) ,
 IFF(AQ077 between 0.5 and 0.999 ,77,0) ,
 IFF(AQ078 between 0.5 and 0.999 ,78,0) ,
 IFF(AQ079 between 0.5 and 0.999 ,79,0) ,
 IFF(AQ080 between 0.5 and 0.999 ,80,0) ,
 IFF(AQ081 between 0.5 and 0.999 ,81,0) ,
 IFF(AQ082 between 0.5 and 0.999 ,82,0) ,
 IFF(AQ083 between 0.5 and 0.999 ,83,0) ,
 IFF(AQ084 between 0.5 and 0.999 ,84,0) ,
 IFF(AQ085 between 0.5 and 0.999 ,85,0) ,
 IFF(AQ086 between 0.5 and 0.999 ,86,0) ,
 IFF(AQ087 between 0.5 and 0.999 ,87,0) ,
 IFF(AQ088 between 0.5 and 0.999 ,88,0) ,
 IFF(AQ089 between 0.5 and 0.999 ,89,0) ,
 IFF(AQ090 between 0.5 and 0.999 ,90,0) ,
 IFF(AQ091 between 0.5 and 0.999 ,91,0) ,
 IFF(AQ092 between 0.5 and 0.999 ,92,0) ,
 IFF(AQ093 between 0.5 and 0.999 ,93,0) ,
 IFF(AQ094 between 0.5 and 0.999 ,94,0) ,
 IFF(AQ095 between 0.5 and 0.999 ,95,0) ,
 IFF(AQ096 between 0.5 and 0.999 ,96,0) ,
 IFF(AQ097 between 0.5 and 0.999 ,97,0) ,
 IFF(AQ098 between 0.5 and 0.999 ,98,0) ,
 IFF(AQ099 between 0.5 and 0.999 ,99,0) ,
 IFF(AQ100 between 0.5 and 0.999 ,100,0) 
)as ETen_50,
GREATEST(
 IFF(AQ001 between 0.6 and 0.999 ,1,0) ,
 IFF(AQ002 between 0.6 and 0.999 ,2,0) ,
 IFF(AQ003 between 0.6 and 0.999 ,3,0) ,
 IFF(AQ004 between 0.6 and 0.999 ,4,0) ,
 IFF(AQ005 between 0.6 and 0.999 ,5,0) ,
 IFF(AQ006 between 0.6 and 0.999 ,6,0) ,
 IFF(AQ007 between 0.6 and 0.999 ,7,0) ,
 IFF(AQ008 between 0.6 and 0.999 ,8,0) ,
 IFF(AQ009 between 0.6 and 0.999 ,9,0) ,
 IFF(AQ010 between 0.6 and 0.999 ,10,0) ,
 IFF(AQ011 between 0.6 and 0.999 ,11,0) ,
 IFF(AQ012 between 0.6 and 0.999 ,12,0) ,
 IFF(AQ013 between 0.6 and 0.999 ,13,0) ,
 IFF(AQ014 between 0.6 and 0.999 ,14,0) ,
 IFF(AQ015 between 0.6 and 0.999 ,15,0) ,
 IFF(AQ016 between 0.6 and 0.999 ,16,0) ,
 IFF(AQ017 between 0.6 and 0.999 ,17,0) ,
 IFF(AQ018 between 0.6 and 0.999 ,18,0) ,
 IFF(AQ019 between 0.6 and 0.999 ,19,0) ,
 IFF(AQ020 between 0.6 and 0.999 ,20,0) ,
 IFF(AQ021 between 0.6 and 0.999 ,21,0) ,
 IFF(AQ022 between 0.6 and 0.999 ,22,0) ,
 IFF(AQ023 between 0.6 and 0.999 ,23,0) ,
 IFF(AQ024 between 0.6 and 0.999 ,24,0) ,
 IFF(AQ025 between 0.6 and 0.999 ,25,0) ,
 IFF(AQ026 between 0.6 and 0.999 ,26,0) ,
 IFF(AQ027 between 0.6 and 0.999 ,27,0) ,
 IFF(AQ028 between 0.6 and 0.999 ,28,0) ,
 IFF(AQ029 between 0.6 and 0.999 ,29,0) ,
 IFF(AQ030 between 0.6 and 0.999 ,30,0) ,
 IFF(AQ031 between 0.6 and 0.999 ,31,0) ,
 IFF(AQ032 between 0.6 and 0.999 ,32,0) ,
 IFF(AQ033 between 0.6 and 0.999 ,33,0) ,
 IFF(AQ034 between 0.6 and 0.999 ,34,0) ,
 IFF(AQ035 between 0.6 and 0.999 ,35,0) ,
 IFF(AQ036 between 0.6 and 0.999 ,36,0) ,
 IFF(AQ037 between 0.6 and 0.999 ,37,0) ,
 IFF(AQ038 between 0.6 and 0.999 ,38,0) ,
 IFF(AQ039 between 0.6 and 0.999 ,39,0) ,
 IFF(AQ040 between 0.6 and 0.999 ,40,0) ,
 IFF(AQ041 between 0.6 and 0.999 ,41,0) ,
 IFF(AQ042 between 0.6 and 0.999 ,42,0) ,
 IFF(AQ043 between 0.6 and 0.999 ,43,0) ,
 IFF(AQ044 between 0.6 and 0.999 ,44,0) ,
 IFF(AQ045 between 0.6 and 0.999 ,45,0) ,
 IFF(AQ046 between 0.6 and 0.999 ,46,0) ,
 IFF(AQ047 between 0.6 and 0.999 ,47,0) ,
 IFF(AQ048 between 0.6 and 0.999 ,48,0) ,
 IFF(AQ049 between 0.6 and 0.999 ,49,0) ,
 IFF(AQ050 between 0.6 and 0.999 ,50,0) ,
 IFF(AQ051 between 0.6 and 0.999 ,51,0) ,
 IFF(AQ052 between 0.6 and 0.999 ,52,0) ,
 IFF(AQ053 between 0.6 and 0.999 ,53,0) ,
 IFF(AQ054 between 0.6 and 0.999 ,54,0) ,
 IFF(AQ055 between 0.6 and 0.999 ,55,0) ,
 IFF(AQ056 between 0.6 and 0.999 ,56,0) ,
 IFF(AQ057 between 0.6 and 0.999 ,57,0) ,
 IFF(AQ058 between 0.6 and 0.999 ,58,0) ,
 IFF(AQ059 between 0.6 and 0.999 ,59,0) ,
 IFF(AQ060 between 0.6 and 0.999 ,60,0) ,
 IFF(AQ061 between 0.6 and 0.999 ,61,0) ,
 IFF(AQ062 between 0.6 and 0.999 ,62,0) ,
 IFF(AQ063 between 0.6 and 0.999 ,63,0) ,
 IFF(AQ064 between 0.6 and 0.999 ,64,0) ,
 IFF(AQ065 between 0.6 and 0.999 ,65,0) ,
 IFF(AQ066 between 0.6 and 0.999 ,66,0) ,
 IFF(AQ067 between 0.6 and 0.999 ,67,0) ,
 IFF(AQ068 between 0.6 and 0.999 ,68,0) ,
 IFF(AQ069 between 0.6 and 0.999 ,69,0) ,
 IFF(AQ070 between 0.6 and 0.999 ,70,0) ,
 IFF(AQ071 between 0.6 and 0.999 ,71,0) ,
 IFF(AQ072 between 0.6 and 0.999 ,72,0) ,
 IFF(AQ073 between 0.6 and 0.999 ,73,0) ,
 IFF(AQ074 between 0.6 and 0.999 ,74,0) ,
 IFF(AQ075 between 0.6 and 0.999 ,75,0) ,
 IFF(AQ076 between 0.6 and 0.999 ,76,0) ,
 IFF(AQ077 between 0.6 and 0.999 ,77,0) ,
 IFF(AQ078 between 0.6 and 0.999 ,78,0) ,
 IFF(AQ079 between 0.6 and 0.999 ,79,0) ,
 IFF(AQ080 between 0.6 and 0.999 ,80,0) ,
 IFF(AQ081 between 0.6 and 0.999 ,81,0) ,
 IFF(AQ082 between 0.6 and 0.999 ,82,0) ,
 IFF(AQ083 between 0.6 and 0.999 ,83,0) ,
 IFF(AQ084 between 0.6 and 0.999 ,84,0) ,
 IFF(AQ085 between 0.6 and 0.999 ,85,0) ,
 IFF(AQ086 between 0.6 and 0.999 ,86,0) ,
 IFF(AQ087 between 0.6 and 0.999 ,87,0) ,
 IFF(AQ088 between 0.6 and 0.999 ,88,0) ,
 IFF(AQ089 between 0.6 and 0.999 ,89,0) ,
 IFF(AQ090 between 0.6 and 0.999 ,90,0) ,
 IFF(AQ091 between 0.6 and 0.999 ,91,0) ,
 IFF(AQ092 between 0.6 and 0.999 ,92,0) ,
 IFF(AQ093 between 0.6 and 0.999 ,93,0) ,
 IFF(AQ094 between 0.6 and 0.999 ,94,0) ,
 IFF(AQ095 between 0.6 and 0.999 ,95,0) ,
 IFF(AQ096 between 0.6 and 0.999 ,96,0) ,
 IFF(AQ097 between 0.6 and 0.999 ,97,0) ,
 IFF(AQ098 between 0.6 and 0.999 ,98,0) ,
 IFF(AQ099 between 0.6 and 0.999 ,99,0) ,
 IFF(AQ100 between 0.6 and 0.999 ,100,0) 
) as ETen_60
from PRED_MODEL_FEATURE.SP_PL_ATenure_001_100 t1
order by 1
--(14254 rows affected)
--Completion time: 2022-10-31T18:19:21.3814820-04:00
SELECT * FROM [SPSS_Sandbox].[dbo].[FY21_PL_ETenure_01] ORDER BY 1
--14254 RECORDS

--=====================================================================
-- FINAL VERSIONS FINAL VERSIONS FINAL VERSIONS AS OCTOBER 31 2022
--======================================================================
-- so far i will go for ETenR_60
--======================================================================
--SPONSORS:

select * from  [SPSS_Sandbox].[dbo].[FY21_SP_ETenure_01] w --Sponsor File with expected residual years
order by w.Donor_Id

select * from [SPSS_Sandbox].[dbo].[FY21_SP_ATenure_001_100]  t1 -- Sponsor file with adjusted hazard rates
order by t1.Donor_Id

--PLEDGERS:

select * from  [SPSS_Sandbox].[dbo].[FY21_PL_ETenure_01] w --Sponsor File with expected residual years
order by w.Donor_Id

select * from [SPSS_Sandbox].[dbo].[FY21_PL_ATenure_001_100]  t1 -- Sponsor file with adjusted hazard rates
order by t1.Donor_Id

--======================================================================
--======================================================================

select top 10 *
from [SPSS_Sandbox].[dbo].[FY21_PL_ETenure_01]

select max(ETen_40) AS 'MAX_TNR' --=====> 100Qrt ==> 100/4 = 25yrs <== PRETTY STABLE RESULT! SIMILAR TO THE ONE FOR FY19 - FY20
, AVG(ETen_40) AS 'AVG_TNR'  --=====> 66qRT ==> 66/4 = 16.5YRS
, MIN(ETen_40) AS 'MIN_TNR' --==> 1
from [SPSS_Sandbox].[dbo].[FY21_SP_ETenure_01]              

select max(ETen_50) AS 'MAX_TNR' --=====> 90Qrt ==> 90/4 = 22.5yrs <== PRETTY STABLE RESULT! SIMILAR TO THE ONE FOR FY19 - FY20
, AVG(ETen_50) AS 'AVG_TNR'  --=====> 55qRT ==> 55/4 = 13.75YRS
, MIN(ETen_50) AS 'MIN_TNR' --==> 1
from [SPSS_Sandbox].[dbo].[FY21_SP_ETenure_01]  

select max(ETen_60) AS 'MAX_TNR' --=====> 72Qrt ==> 72/4 = 18yrs <== PRETTY STABLE RESULT! SIMILAR TO THE ONE FOR FY19 - FY20
, AVG(ETen_60) AS 'AVG_TNR'  --=====> 42qRT ==> 42/4 = 10.5YRS
, MIN(ETen_60) AS 'MIN_TNR' --==> 0
from [SPSS_Sandbox].[dbo].[FY21_SP_ETenure_01]  

--=================================
--SAME IS OBSERVED FOR PLEDGERS:

select max(ETen_40) AS 'MAX_TNR' --=====> 100Qrtt ==> 100/4 = 25yrs <== PRETTY STABLE RESULT! SIMILAR TO THE ONE FOR FY19 - FY20
, AVG(ETen_40) AS 'AVG_TNR'  --=====> 63QRT ==> 63/4 = 15.8YRS
, MIN(ETen_40) AS 'MIN_TNR'  --==> 1
from [SPSS_Sandbox].[dbo].[FY21_PL_ETenure_01]

select max(ETen_50) AS 'MAX_TNR' --=====> 90Qrt ==> 90/4 = 22.5yrs <== PRETTY STABLE RESULT! SIMILAR TO THE ONE FOR FY19 - FY20
, AVG(ETen_50) AS 'AVG_TNR'  --=====> 52QRT ==> 52/4 = 13YRS
, MIN(ETen_50) AS 'MIN_TNR'  --==> 0
from [SPSS_Sandbox].[dbo].[FY21_PL_ETenure_01]

select max(ETen_60) AS 'MAX_TNR' --=====> 72Qrt ==> 72/4 = 18yrs <== PRETTY STABLE RESULT! SIMILAR TO THE ONE FOR FY19 - FY20
, AVG(ETen_60) AS 'AVG_TNR'  --=====> 40QRT ==> 40/4 = 10YRS
, MIN(ETen_60) AS 'MIN_TNR'  --==> 0
from [SPSS_Sandbox].[dbo].[FY21_PL_ETenure_01]

--=========================================================

--THEREFORE FUTURE TENURE IN QUARTERS 'FUTURE_TENURE_QQ' = ETen_60
--TO GET THE VALUE IN YRS THE CALCULATION IS 'FUTURE_TENURE_YY' = ETen_60/4

--=====================================================================
-- ADJUSTING THE TENURE TO A MAXIMUM OF 180 quarters (45 yrs of sp max)
--=====================================================================

--some donors are predicted to have more than 180 (in total DurGS_QQs + Future Tenure) quarters (180/4 = 45 yrs) which
-- is ok for the model (the model does not know it is dealing with humans that have a limited physical time constraint, in theory machines can last forever)
-- a manual restriction must be added in order to find the final number of periods relevant to any sponsor
-- all donor for which (DurGS_QQs + Future Tenure) > 180 will be rounded down to 180 quarters (45 yrs of sp max)

--HISTORICAL TENURES FOR DONORS RE STORED IN [SPSS_Sandbox].[dbo].[LTDV_SCRIPT_FY21_25072022_PAST_TNR_HISTORICAL_DEMO] 

SELECT * FROM [SPSS_Sandbox].[dbo].[LTDV_SCRIPT_FY21_25072022_PAST_TNR_HISTORICAL_DEMO] ORDER BY 1 ---333141 RECORDS

--then the code above will aggregate the past historical tenure with the expected tenure to get the total tenure and apply the restriction if/when necessary

CREATE OR REPLACE TABLE SP_PL_ETenure_02 AS
select t1.Donor_Id,t1.DNR_LABEL
, round(t1.ETen_60,0) as ETen_60
, t2.PAST_TNR_QQ_HISTORICAL
, round(t1.ETen_60,0) + t2.PAST_TNR_QQ_HISTORICAL AS HIST_AND_RAW_PRD_TNR
, IFF( t1.ETen_60 + t2.PAST_TNR_QQ_HISTORICAL > 180 , 1, 0) AS TNR_EXCEEDS_180QQ 
, IFF((t2.PAST_TNR_QQ_HISTORICAL + t1.Eten_60) > 180, round(180 - t2.PAST_TNR_QQ_HISTORICAL,0), round(t1.Eten_60,0)) as EtenR_60
from PRED_MODEL_FEATURE.SP_PL_ETenure_01 t1
join PRED_MODEL_FEATURE.LTDV_SCRIPT_PAST_TNR_HISTORICAL_F t2 on t1.Donor_Id = t2.Donor_Id
order by 1
--(14254 rows affected)
--Completion time: 2022-10-31T19:08:46.7680040-04:00

--=========================================================================================================================================
--final results : future tenures to be used in the project for futUre income calculation:
--THE EXPECTED TENURE HERE IS THE NUMBER OF QUARTERS THAT MATCHES A 0.60 NORMALIZED TO 180 qq HAZARD RATE THAT MEETS THE CONDITION OF HISTORICAL + PREDICTED <= 180QQ

--SPONSORS
select * from [SPSS_Sandbox].[dbo].[FY21_SP_ETenure_02] -- 271504 RECORDS
--PLEDGERS
select * from [SPSS_Sandbox].[dbo].[FY21_PL_ETenure_02] --14254 records

--===============================================================================================================================================

-- BLOCK 4: LTDV_FY21_CODE_FOR_PROB_VECTOR_SP_PL_SG_80_QUARTERS

--================================================================================================================================================


--==============================================================
 --SPONSORS: getting the prob vector for FY21 propensity to give 
 --==============================================================

 --getting flags per donor
 select dn.Donation_Donor_Id
--Detail by Quarter
, SUM(iif(cal.Tri_Fin_Ds_Lg = '2021 Q1' ,1,0)) as 'CN_2021_Q1'
, SUM(iif(cal.Tri_Fin_Ds_Lg = '2021 Q2' ,1,0)) as 'CN_2021_Q2'
, SUM(iif(cal.Tri_Fin_Ds_Lg = '2021 Q3' ,1,0)) as 'CN_2021_Q3'
, SUM(iif(cal.Tri_Fin_Ds_Lg = '2021 Q4' ,1,0)) as 'CN_2021_Q4'
, SUM(iif(cal.Tri_Fin_Ds_Lg = '2022 Q1' ,1,0)) as 'CN_2022_Q1'

into #FY21Q1_FY22Q1_PS
from ADOBE.RAW.F_DONATION dn
join [BI_DW].[dbo].[D_Cal] cal on cal.Dt = dn.Donation_Deposit_Date
where dn.Donation_Adjustment_Reason_Sid = 0
and dn.Donation_Income_Type_for_Donor_Metrics_Sid = 102
and dn.Donation_Deposit_Date between '2020-10-01' and '2021-12-31'
group by dn.Donation_Donor_Id
order by 1

--(280683 rows affected)
--Completion time: 2022-11-07T23:59:07.9662017-05:00


 select c.donation_donor_id 
 , a.FY21_DNR_CAT
--Flag by Quarter
, iif (c.CN_2021_Q1  > 0 ,1,0) as 'CF_2021_Q1'
, iif (c.CN_2021_Q2  > 0 ,1,0) as 'CF_2021_Q2'
, iif (c.CN_2021_Q3  > 0 ,1,0) as 'CF_2021_Q3'
, iif (c.CN_2021_Q4  > 0 ,1,0) as 'CF_2021_Q4'
, iif (c.CN_2022_Q1  > 0 ,1,0) as 'CF_2022_Q1'
into #FY21Q1_FY22Q1_QF
from  #FY21Q1_FY22Q1_PS c
join [SPSS_Sandbox].[dbo].[LTDV_SCRIPT_25072022_FY21_SPR_PLR_SGR_CAT_DEMO] a on a.DONATION_DONOR_ID = c.Donation_Donor_Id
order by 1
--(273318 rows affected)
--Completion time: 2022-11-08T00:01:19.3956728-05:00


--getting the pattern and pattern number
SELECT Donation_Donor_Id
, FY21_DNR_CAT 
, CASE
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1111' then 'P1111'
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0111' then 'P0111'
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1011' then 'P1011'
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1101' then 'P1101'
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0011' then 'P0011'
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0101' then 'P0101'
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1001' then 'P1001'
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0001' then 'P0001'
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1110' then 'P1110'
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0110' then 'P0110'
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1010' then 'P1010'
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1100' then 'P1100'
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0010' then 'P0010'
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0100' then 'P0100'
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1000' then 'P1000'
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0000' then 'P0000'
else 'UNDEFINED' end as 'Pattern_Q'

--64 COMBINATIONS OF PATTERN AND DONOR CATEGORY AND A OTHER : 65 GROUPS
, CASE
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1111' and FY21_DNR_CAT = 'NEW' then 1
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1111' and FY21_DNR_CAT = 'UPGRADED' then 2
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1111' and FY21_DNR_CAT = 'REACTIVATED' then 3
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1111' and FY21_DNR_CAT = 'OLD' then 4
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0111' and FY21_DNR_CAT = 'NEW' then 5
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0111' and FY21_DNR_CAT = 'UPGRADED' then 6
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0111' and FY21_DNR_CAT = 'REACTIVATED' then 7
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0111' and FY21_DNR_CAT = 'OLD' then 8
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1011' and FY21_DNR_CAT = 'NEW' then 9
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1011' and FY21_DNR_CAT = 'UPGRADED' then 10
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1011' and FY21_DNR_CAT = 'REACTIVATED' then 11
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1011' and FY21_DNR_CAT = 'OLD' then 12
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1101' and FY21_DNR_CAT = 'NEW' then 13
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1101' and FY21_DNR_CAT = 'UPGRADED' then 14
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1101' and FY21_DNR_CAT = 'REACTIVATED' then 15
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1101' and FY21_DNR_CAT = 'OLD' then 16
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0011' and FY21_DNR_CAT = 'NEW' then 17
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0011' and FY21_DNR_CAT = 'UPGRADED' then 18
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0011' and FY21_DNR_CAT = 'REACTIVATED' then 19
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0011' and FY21_DNR_CAT = 'OLD' then 20
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0101' and FY21_DNR_CAT = 'NEW' then 21
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0101' and FY21_DNR_CAT = 'UPGRADED' then 22
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0101' and FY21_DNR_CAT = 'REACTIVATED' then 23
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0101' and FY21_DNR_CAT = 'OLD' then 24
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1001' and FY21_DNR_CAT = 'NEW' then 25
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1001' and FY21_DNR_CAT = 'UPGRADED' then 26
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1001' and FY21_DNR_CAT = 'REACTIVATED' then 27
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1001' and FY21_DNR_CAT = 'OLD' then 28
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0001' and FY21_DNR_CAT = 'NEW' then 29
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0001' and FY21_DNR_CAT = 'UPGRADED' then 30
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0001' and FY21_DNR_CAT = 'REACTIVATED' then 31
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0001' and FY21_DNR_CAT = 'OLD' then 32
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1110' and FY21_DNR_CAT = 'NEW' then 33
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1110' and FY21_DNR_CAT = 'UPGRADED' then 34
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1110' and FY21_DNR_CAT = 'REACTIVATED' then 35
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1110' and FY21_DNR_CAT = 'OLD' then 36
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0110' and FY21_DNR_CAT = 'NEW' then 37
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0110' and FY21_DNR_CAT = 'UPGRADED' then 38
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0110' and FY21_DNR_CAT = 'REACTIVATED' then 39
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0110' and FY21_DNR_CAT = 'OLD' then 40
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1010' and FY21_DNR_CAT = 'NEW' then 41
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1010' and FY21_DNR_CAT = 'UPGRADED' then 42
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1010' and FY21_DNR_CAT = 'REACTIVATED' then 43
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1010' and FY21_DNR_CAT = 'OLD' then 44
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1100' and FY21_DNR_CAT = 'NEW' then 45
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1100' and FY21_DNR_CAT = 'UPGRADED' then 46
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1100' and FY21_DNR_CAT = 'REACTIVATED' then 47
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1100' and FY21_DNR_CAT = 'OLD' then 48
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0010' and FY21_DNR_CAT = 'NEW' then 49
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0010' and FY21_DNR_CAT = 'UPGRADED' then 50
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0010' and FY21_DNR_CAT = 'REACTIVATED' then 51
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0010' and FY21_DNR_CAT = 'OLD' then 52
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0100' and FY21_DNR_CAT = 'NEW' then 53
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0100' and FY21_DNR_CAT = 'UPGRADED' then 54
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0100' and FY21_DNR_CAT = 'REACTIVATED' then 55
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0100' and FY21_DNR_CAT = 'OLD' then 56
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1000' and FY21_DNR_CAT = 'NEW' then 57
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1000' and FY21_DNR_CAT = 'UPGRADED' then 58
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1000' and FY21_DNR_CAT = 'REACTIVATED' then 59
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1000' and FY21_DNR_CAT = 'OLD' then 60
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0000' and FY21_DNR_CAT = 'NEW'  then 61
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0000' and FY21_DNR_CAT = 'UPGRADED' then 62
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0000' and FY21_DNR_CAT = 'REACTIVATED' then 63
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0000' and FY21_DNR_CAT = 'OLD' then 64
else 65 end as 'Pattern_Number'

, iif(CF_2022_Q1 > 0 , 1 , 0) as 'Gave_in_2022_Q1'  --==> needed to relate with patterns to create vector of prior probabilities 
                                                   --scenario is time is set up by the end of 2021 ... Q1FY21 can be used as a bonus
INTO #FY21Q1_FY22Q1_md 
from #FY21Q1_FY22Q1_QF 
ORDER BY 1
--(273318 rows affected)
--Completion time: 2022-11-13T22:30:35.1811959-05:00

select b.Pattern_Number 
, B.Pattern_Q
, B.FY21_DNR_CAT
, count (b.Donation_Donor_Id) as 'N_Sponsors'  -- ==> Input for P(E) Number of donor in Pattern i / Divided by N_Donors
, sum (b.Gave_in_2022_Q1) as 'N_Sponsors_Gave_102' -- ==> Input for P(H) Number of donors that Gave / Divided by N_Donors
, convert(decimal(5,4) ,sum (b.Gave_in_2022_Q1)* 1./ count (b.Donation_Donor_Id)) as 'Prob_SPR_Gave_SPP_NextQ' -- ==> BAYESIAN ESTIMATION OF P(H/E)
--into [SPSS_Sandbox].[dbo].[LTSV_FY21_Prior_Prob_21_22]
into #LTSV_FY21_Prior_Prob_21_22 
FROM #FY21Q1_FY22Q1_md B
group by  b.Pattern_Number , B.Pattern_Q , B.FY21_DNR_CAT
order by 1

--(61 rows affected)
--Completion time: 2022-11-13T22:31:04.6662985-05:00

select * from #LTSV_FY21_Prior_Prob_21_22  B order by 1
--join [SPSS_Sandbox].[dbo].[LTSV_FY19_20_PR_PROB_4Q] c on c.Pattern_N = b.Pattern_Number order by c.Pattern_N

--==============================================================
--PLEDGERS: getting the prob vector for FY21 propensity to give 
 --==============================================================

 --getting flags per donor
 select dn.Donation_Donor_Id
--Detail by Quarter
, SUM(iif(cal.Tri_Fin_Ds_Lg = '2021 Q1' ,1,0)) as 'CN_2021_Q1'
, SUM(iif(cal.Tri_Fin_Ds_Lg = '2021 Q2' ,1,0)) as 'CN_2021_Q2'
, SUM(iif(cal.Tri_Fin_Ds_Lg = '2021 Q3' ,1,0)) as 'CN_2021_Q3'
, SUM(iif(cal.Tri_Fin_Ds_Lg = '2021 Q4' ,1,0)) as 'CN_2021_Q4'
, SUM(iif(cal.Tri_Fin_Ds_Lg = '2022 Q1' ,1,0)) as 'CN_2022_Q1'

into #FY21Q1_FY22Q1_PS_PL
from ADOBE.RAW.F_DONATION dn
join [BI_DW].[dbo].[D_Cal] cal on cal.Dt = dn.Donation_Deposit_Date
where dn.Donation_Adjustment_Reason_Sid = 0
and dn.Donation_Income_Type_for_Donor_Metrics_Sid = 101
and dn.Donation_Deposit_Date between '2020-10-01' and '2021-12-31'
group by dn.Donation_Donor_Id
order by 1

--(22418 rows affected)
--Completion time: 2022-11-08T00:50:47.9534438-05:00


 select c.donation_donor_id 
 , a.FY21_DNR_CAT
--Flag by Quarter
, iif (c.CN_2021_Q1  > 0 ,1,0) as 'CF_2021_Q1'
, iif (c.CN_2021_Q2  > 0 ,1,0) as 'CF_2021_Q2'
, iif (c.CN_2021_Q3  > 0 ,1,0) as 'CF_2021_Q3'
, iif (c.CN_2021_Q4  > 0 ,1,0) as 'CF_2021_Q4'
, iif (c.CN_2022_Q1  > 0 ,1,0) as 'CF_2022_Q1'
into #FY21Q1_FY22Q1_QF_PL
from  #FY21Q1_FY22Q1_PS_PL c
join [SPSS_Sandbox].[dbo].[LTDV_SCRIPT_25072022_FY21_SPR_PLR_SGR_CAT_DEMO] a on a.DONATION_DONOR_ID = c.Donation_Donor_Id
join [spss_sandbox].[dbo].[LTDV_SCRIPT_0422_DNR_LABEL] b on b.Donation_Donor_Id = a.DONATION_DONOR_ID
where b.FY21_DNR_LABEL = 'PLEDGER'
order by 1
--(14350 rows affected)
--Completion time: 2022-11-08T00:57:26.7232651-05:00


--getting the pattern and pattern number
SELECT Donation_Donor_Id
, FY21_DNR_CAT
, CASE
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1111' then 'P1111'
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0111' then 'P0111'
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1011' then 'P1011'
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1101' then 'P1101'
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0011' then 'P0011'
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0101' then 'P0101'
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1001' then 'P1001'
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0001' then 'P0001'
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1110' then 'P1110'
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0110' then 'P0110'
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1010' then 'P1010'
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1100' then 'P1100'
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0010' then 'P0010'
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0100' then 'P0100'
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1000' then 'P1000'
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0000' then 'P0000'
else 'UNDEFINED' end as 'Pattern_Q'

--64 COMBINATIONS OF PATTERN AND DONOR CATEGORY AND A OTHER : 65 GROUPS
, CASE
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1111' and FY21_DNR_CAT = 'NEW' then 1
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1111' and FY21_DNR_CAT = 'UPGRADED' then 2
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1111' and FY21_DNR_CAT = 'REACTIVATED' then 3
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1111' and FY21_DNR_CAT = 'OLD' then 4
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0111' and FY21_DNR_CAT = 'NEW' then 5
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0111' and FY21_DNR_CAT = 'UPGRADED' then 6
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0111' and FY21_DNR_CAT = 'REACTIVATED' then 7
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0111' and FY21_DNR_CAT = 'OLD' then 8
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1011' and FY21_DNR_CAT = 'NEW' then 9
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1011' and FY21_DNR_CAT = 'UPGRADED' then 10
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1011' and FY21_DNR_CAT = 'REACTIVATED' then 11
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1011' and FY21_DNR_CAT = 'OLD' then 12
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1101' and FY21_DNR_CAT = 'NEW' then 13
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1101' and FY21_DNR_CAT = 'UPGRADED' then 14
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1101' and FY21_DNR_CAT = 'REACTIVATED' then 15
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1101' and FY21_DNR_CAT = 'OLD' then 16
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0011' and FY21_DNR_CAT = 'NEW' then 17
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0011' and FY21_DNR_CAT = 'UPGRADED' then 18
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0011' and FY21_DNR_CAT = 'REACTIVATED' then 19
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0011' and FY21_DNR_CAT = 'OLD' then 20
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0101' and FY21_DNR_CAT = 'NEW' then 21
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0101' and FY21_DNR_CAT = 'UPGRADED' then 22
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0101' and FY21_DNR_CAT = 'REACTIVATED' then 23
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0101' and FY21_DNR_CAT = 'OLD' then 24
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1001' and FY21_DNR_CAT = 'NEW' then 25
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1001' and FY21_DNR_CAT = 'UPGRADED' then 26
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1001' and FY21_DNR_CAT = 'REACTIVATED' then 27
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1001' and FY21_DNR_CAT = 'OLD' then 28
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0001' and FY21_DNR_CAT = 'NEW' then 29
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0001' and FY21_DNR_CAT = 'UPGRADED' then 30
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0001' and FY21_DNR_CAT = 'REACTIVATED' then 31
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0001' and FY21_DNR_CAT = 'OLD' then 32
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1110' and FY21_DNR_CAT = 'NEW' then 33
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1110' and FY21_DNR_CAT = 'UPGRADED' then 34
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1110' and FY21_DNR_CAT = 'REACTIVATED' then 35
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1110' and FY21_DNR_CAT = 'OLD' then 36
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0110' and FY21_DNR_CAT = 'NEW' then 37
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0110' and FY21_DNR_CAT = 'UPGRADED' then 38
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0110' and FY21_DNR_CAT = 'REACTIVATED' then 39
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0110' and FY21_DNR_CAT = 'OLD' then 40
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1010' and FY21_DNR_CAT = 'NEW' then 41
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1010' and FY21_DNR_CAT = 'UPGRADED' then 42
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1010' and FY21_DNR_CAT = 'REACTIVATED' then 43
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1010' and FY21_DNR_CAT = 'OLD' then 44
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1100' and FY21_DNR_CAT = 'NEW' then 45
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1100' and FY21_DNR_CAT = 'UPGRADED' then 46
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1100' and FY21_DNR_CAT = 'REACTIVATED' then 47
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1100' and FY21_DNR_CAT = 'OLD' then 48
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0010' and FY21_DNR_CAT = 'NEW' then 49
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0010' and FY21_DNR_CAT = 'UPGRADED' then 50
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0010' and FY21_DNR_CAT = 'REACTIVATED' then 51
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0010' and FY21_DNR_CAT = 'OLD' then 52
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0100' and FY21_DNR_CAT = 'NEW' then 53
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0100' and FY21_DNR_CAT = 'UPGRADED' then 54
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0100' and FY21_DNR_CAT = 'REACTIVATED' then 55
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0100' and FY21_DNR_CAT = 'OLD' then 56
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1000' and FY21_DNR_CAT = 'NEW' then 57
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1000' and FY21_DNR_CAT = 'UPGRADED' then 58
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1000' and FY21_DNR_CAT = 'REACTIVATED' then 59
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1000' and FY21_DNR_CAT = 'OLD' then 60
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0000' and FY21_DNR_CAT = 'NEW'  then 61
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0000' and FY21_DNR_CAT = 'UPGRADED' then 62
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0000' and FY21_DNR_CAT = 'REACTIVATED' then 63
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0000' and FY21_DNR_CAT = 'OLD' then 64
else 65 end as 'Pattern_Number'

, iif(CF_2022_Q1 > 0 , 1 , 0) as 'Gave_in_2022_Q1'  --==> needed to relate with patterns to create vector of prior probabilities 
                                                   --scenario is time is set up by the end of 2021 ... Q1FY21 can be used as a bonus
INTO #FY21Q1_FY22Q1_md_pl 
from #FY21Q1_FY22Q1_QF_PL 
ORDER BY 1
--(14350 rows affected)
--Completion time: 2022-11-08T00:59:56.8184310-05:00


select b.Pattern_Number 
, B.Pattern_Q
, B.FY21_DNR_CAT
, count (b.Donation_Donor_Id) as 'N_Pledgers'  -- ==> Input for P(E) Number of donor in Pattern i / Divided by N_Donors
, sum (b.Gave_in_2022_Q1) as 'N_Pledgers_Gave_101' -- ==> Input for P(H) Number of donors that Gave / Divided by N_Donors
, convert(decimal(5,4) ,sum (b.Gave_in_2022_Q1)* 1./ count (b.Donation_Donor_Id)) as 'Prob_PLR_Gave_PL_NextQ' -- ==> BAYESIAN ESTIMATION OF P(H/E)
--into [SPSS_Sandbox].[dbo].[LTSV_FY21_Prior_Prob_21_22]
into #LTPV_FY21_Prior_Prob_21_22 
FROM #FY21Q1_FY22Q1_md_pl B
group by  b.Pattern_Number , B.Pattern_Q , B.FY21_DNR_CAT
order by 1

--(44 rows affected)
--Completion time: 2022-11-13T22:35:30.3962836-05:00

select * from #LTPV_FY21_Prior_Prob_21_22  B order by 1
--right join [SPSS_Sandbox].[dbo].[LTSV_FY19_20_PR_PROB_4Q] c on c.Pattern_N = b.Pattern_Number order by c.Pattern_N


--============================================================================================
--SINGLE GIFT DONORS: getting the prob vector for FY21 propensity to give 
 --==============================================================

 --getting flags per donor
 select dn.Donation_Donor_Id
--Detail by Quarter
, SUM(iif(cal.Tri_Fin_Ds_Lg = '2021 Q1' ,1,0)) as 'CN_2021_Q1'
, SUM(iif(cal.Tri_Fin_Ds_Lg = '2021 Q2' ,1,0)) as 'CN_2021_Q2'
, SUM(iif(cal.Tri_Fin_Ds_Lg = '2021 Q3' ,1,0)) as 'CN_2021_Q3'
, SUM(iif(cal.Tri_Fin_Ds_Lg = '2021 Q4' ,1,0)) as 'CN_2021_Q4'
, SUM(iif(cal.Tri_Fin_Ds_Lg = '2022 Q1' ,1,0)) as 'CN_2022_Q1'

into #FY21Q1_FY22Q1_PS_SG
from ADOBE.RAW.F_DONATION dn
join [BI_DW].[dbo].[D_Cal] cal on cal.Dt = dn.Donation_Deposit_Date
where dn.Donation_Adjustment_Reason_Sid = 0
and dn.Donation_Income_Type_for_Donor_Metrics_Sid IN (103,104,105,106,107,108)
and dn.Donation_Deposit_Date between '2020-10-01' and '2021-12-31'
group by dn.Donation_Donor_Id
order by 1

--(137192 rows affected)
--Completion time: 2022-11-08T01:16:47.8934088-05:00


 select c.donation_donor_id 
 , a.FY21_DNR_CAT
--Flag by Quarter
, iif (c.CN_2021_Q1  > 0 ,1,0) as 'CF_2021_Q1'
, iif (c.CN_2021_Q2  > 0 ,1,0) as 'CF_2021_Q2'
, iif (c.CN_2021_Q3  > 0 ,1,0) as 'CF_2021_Q3'
, iif (c.CN_2021_Q4  > 0 ,1,0) as 'CF_2021_Q4'
, iif (c.CN_2022_Q1  > 0 ,1,0) as 'CF_2022_Q1'
into #FY21Q1_FY22Q1_QF_SG
from  #FY21Q1_FY22Q1_PS_SG c
join [SPSS_Sandbox].[dbo].[LTDV_SCRIPT_25072022_FY21_SPR_PLR_SGR_CAT_DEMO] a on a.DONATION_DONOR_ID = c.Donation_Donor_Id
join [spss_sandbox].[dbo].[LTDV_SCRIPT_0422_DNR_LABEL] b on b.Donation_Donor_Id = a.DONATION_DONOR_ID
where b.FY21_DNR_LABEL = 'SINGLE_GIVER'
order by 1
--(45647 rows affected)
--Completion time: 2022-11-08T01:17:34.9051091-05:00


--getting the pattern and pattern number
SELECT Donation_Donor_Id
, FY21_DNR_CAT
, CASE
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1111' then 'P1111'
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0111' then 'P0111'
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1011' then 'P1011'
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1101' then 'P1101'
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0011' then 'P0011'
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0101' then 'P0101'
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1001' then 'P1001'
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0001' then 'P0001'
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1110' then 'P1110'
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0110' then 'P0110'
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1010' then 'P1010'
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1100' then 'P1100'
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0010' then 'P0010'
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0100' then 'P0100'
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1000' then 'P1000'
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0000' then 'P0000'
else 'UNDEFINED' end as 'Pattern_Q'

--64 COMBINATIONS OF PATTERN AND DONOR CATEGORY AND A OTHER : 65 GROUPS
, CASE
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1111' and FY21_DNR_CAT = 'NEW' then 1
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1111' and FY21_DNR_CAT = 'UPGRADED' then 2
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1111' and FY21_DNR_CAT = 'REACTIVATED' then 3
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1111' and FY21_DNR_CAT = 'OLD' then 4
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0111' and FY21_DNR_CAT = 'NEW' then 5
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0111' and FY21_DNR_CAT = 'UPGRADED' then 6
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0111' and FY21_DNR_CAT = 'REACTIVATED' then 7
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0111' and FY21_DNR_CAT = 'OLD' then 8
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1011' and FY21_DNR_CAT = 'NEW' then 9
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1011' and FY21_DNR_CAT = 'UPGRADED' then 10
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1011' and FY21_DNR_CAT = 'REACTIVATED' then 11
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1011' and FY21_DNR_CAT = 'OLD' then 12
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1101' and FY21_DNR_CAT = 'NEW' then 13
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1101' and FY21_DNR_CAT = 'UPGRADED' then 14
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1101' and FY21_DNR_CAT = 'REACTIVATED' then 15
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1101' and FY21_DNR_CAT = 'OLD' then 16
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0011' and FY21_DNR_CAT = 'NEW' then 17
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0011' and FY21_DNR_CAT = 'UPGRADED' then 18
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0011' and FY21_DNR_CAT = 'REACTIVATED' then 19
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0011' and FY21_DNR_CAT = 'OLD' then 20
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0101' and FY21_DNR_CAT = 'NEW' then 21
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0101' and FY21_DNR_CAT = 'UPGRADED' then 22
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0101' and FY21_DNR_CAT = 'REACTIVATED' then 23
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0101' and FY21_DNR_CAT = 'OLD' then 24
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1001' and FY21_DNR_CAT = 'NEW' then 25
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1001' and FY21_DNR_CAT = 'UPGRADED' then 26
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1001' and FY21_DNR_CAT = 'REACTIVATED' then 27
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1001' and FY21_DNR_CAT = 'OLD' then 28
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0001' and FY21_DNR_CAT = 'NEW' then 29
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0001' and FY21_DNR_CAT = 'UPGRADED' then 30
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0001' and FY21_DNR_CAT = 'REACTIVATED' then 31
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0001' and FY21_DNR_CAT = 'OLD' then 32
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1110' and FY21_DNR_CAT = 'NEW' then 33
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1110' and FY21_DNR_CAT = 'UPGRADED' then 34
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1110' and FY21_DNR_CAT = 'REACTIVATED' then 35
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1110' and FY21_DNR_CAT = 'OLD' then 36
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0110' and FY21_DNR_CAT = 'NEW' then 37
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0110' and FY21_DNR_CAT = 'UPGRADED' then 38
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0110' and FY21_DNR_CAT = 'REACTIVATED' then 39
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0110' and FY21_DNR_CAT = 'OLD' then 40
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1010' and FY21_DNR_CAT = 'NEW' then 41
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1010' and FY21_DNR_CAT = 'UPGRADED' then 42
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1010' and FY21_DNR_CAT = 'REACTIVATED' then 43
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1010' and FY21_DNR_CAT = 'OLD' then 44
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1100' and FY21_DNR_CAT = 'NEW' then 45
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1100' and FY21_DNR_CAT = 'UPGRADED' then 46
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1100' and FY21_DNR_CAT = 'REACTIVATED' then 47
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1100' and FY21_DNR_CAT = 'OLD' then 48
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0010' and FY21_DNR_CAT = 'NEW' then 49
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0010' and FY21_DNR_CAT = 'UPGRADED' then 50
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0010' and FY21_DNR_CAT = 'REACTIVATED' then 51
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0010' and FY21_DNR_CAT = 'OLD' then 52
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0100' and FY21_DNR_CAT = 'NEW' then 53
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0100' and FY21_DNR_CAT = 'UPGRADED' then 54
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0100' and FY21_DNR_CAT = 'REACTIVATED' then 55
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0100' and FY21_DNR_CAT = 'OLD' then 56
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1000' and FY21_DNR_CAT = 'NEW' then 57
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1000' and FY21_DNR_CAT = 'UPGRADED' then 58
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1000' and FY21_DNR_CAT = 'REACTIVATED' then 59
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1000' and FY21_DNR_CAT = 'OLD' then 60
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0000' and FY21_DNR_CAT = 'NEW'  then 61
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0000' and FY21_DNR_CAT = 'UPGRADED' then 62
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0000' and FY21_DNR_CAT = 'REACTIVATED' then 63
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0000' and FY21_DNR_CAT = 'OLD' then 64
else 65 end as 'Pattern_Number'

, iif(CF_2022_Q1 > 0 , 1 , 0) as 'Gave_in_2022_Q1'  --==> needed to relate with patterns to create vector of prior probabilities 
                                                   --scenario is time is set up by the end of 2021 ... Q1FY21 can be used as a bonus
INTO #FY21Q1_FY22Q1_md_SG 
from #FY21Q1_FY22Q1_QF_SG 
ORDER BY 1
--(45647 rows affected)
--Completion time: 2022-11-08T01:18:24.8077408-05:00


select b.Pattern_Number 
, B.Pattern_Q
, B.FY21_DNR_CAT
, count (b.Donation_Donor_Id) as 'N_Single_Givers'  -- ==> Input for P(E) Number of donor in Pattern i / Divided by N_Donors
, sum (b.Gave_in_2022_Q1) as 'N_Single_Givers_Gave_103_108' -- ==> Input for P(H) Number of donors that Gave / Divided by N_Donors
, convert(decimal(5,4) ,sum (b.Gave_in_2022_Q1)* 1./ count (b.Donation_Donor_Id)) as 'Prob_SGR_Gave_SG_NextQ' -- ==> BAYESIAN ESTIMATION OF P(H/E)
--into [SPSS_Sandbox].[dbo].[LTSV_FY21_Prior_Prob_21_22]
into #LTSG_FY21_Prior_Prob_21_22 
FROM #FY21Q1_FY22Q1_md_SG B
group by  b.Pattern_Number , B.Pattern_Q , B.FY21_DNR_CAT
order by 1
--(59 rows affected)
--Completion time: 2022-11-13T22:39:26.0239187-05:00

select * from #LTsg_FY21_Prior_Prob_21_22  B order by 1
--right join [SPSS_Sandbox].[dbo].[LTSV_FY19_20_PR_PROB_4Q] c on c.Pattern_N = b.Pattern_Number order by c.Pattern_N

--=================================================================================================
--Prob vectors for (1) sponsors as pledgers and (2) sponsors and pledgers as single givers
--===================================================================================================
--=========================
--sponsors as pledgers
--==========================
--using the same query that produced #FY21Q1_FY22Q1_PS_PL applied to the group of sponsors

 select c.donation_donor_id 
 , a.FY21_DNR_CAT
--Flag by Quarter
, iif (c.CN_2021_Q1  > 0 ,1,0) as 'CF_2021_Q1'
, iif (c.CN_2021_Q2  > 0 ,1,0) as 'CF_2021_Q2'
, iif (c.CN_2021_Q3  > 0 ,1,0) as 'CF_2021_Q3'
, iif (c.CN_2021_Q4  > 0 ,1,0) as 'CF_2021_Q4'
, iif (c.CN_2022_Q1  > 0 ,1,0) as 'CF_2022_Q1'
into #FY21Q1_FY22Q1_QF_PL_SP_as_PL
from  #FY21Q1_FY22Q1_PS_PL c
join [SPSS_Sandbox].[dbo].[LTDV_SCRIPT_25072022_FY21_SPR_PLR_SGR_CAT_DEMO] a on a.DONATION_DONOR_ID = c.Donation_Donor_Id
join [spss_sandbox].[dbo].[LTDV_SCRIPT_0422_DNR_LABEL] b on b.Donation_Donor_Id = a.DONATION_DONOR_ID
where b.FY21_DNR_LABEL = 'SPONSOR'
order by 1
--(7293 rows affected)
--Completion time: 2022-11-13T19:48:51.2076915-05:00


--getting the pattern and pattern number
SELECT Donation_Donor_Id
, FY21_DNR_CAT
, CASE
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1111' then 'P1111'
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0111' then 'P0111'
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1011' then 'P1011'
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1101' then 'P1101'
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0011' then 'P0011'
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0101' then 'P0101'
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1001' then 'P1001'
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0001' then 'P0001'
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1110' then 'P1110'
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0110' then 'P0110'
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1010' then 'P1010'
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1100' then 'P1100'
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0010' then 'P0010'
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0100' then 'P0100'
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1000' then 'P1000'
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0000' then 'P0000'
else 'UNDEFINED' end as 'Pattern_Q'

--64 COMBINATIONS OF PATTERN AND DONOR CATEGORY AND A OTHER : 65 GROUPS
, CASE
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1111' and FY21_DNR_CAT = 'NEW' then 1
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1111' and FY21_DNR_CAT = 'UPGRADED' then 2
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1111' and FY21_DNR_CAT = 'REACTIVATED' then 3
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1111' and FY21_DNR_CAT = 'OLD' then 4
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0111' and FY21_DNR_CAT = 'NEW' then 5
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0111' and FY21_DNR_CAT = 'UPGRADED' then 6
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0111' and FY21_DNR_CAT = 'REACTIVATED' then 7
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0111' and FY21_DNR_CAT = 'OLD' then 8
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1011' and FY21_DNR_CAT = 'NEW' then 9
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1011' and FY21_DNR_CAT = 'UPGRADED' then 10
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1011' and FY21_DNR_CAT = 'REACTIVATED' then 11
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1011' and FY21_DNR_CAT = 'OLD' then 12
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1101' and FY21_DNR_CAT = 'NEW' then 13
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1101' and FY21_DNR_CAT = 'UPGRADED' then 14
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1101' and FY21_DNR_CAT = 'REACTIVATED' then 15
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1101' and FY21_DNR_CAT = 'OLD' then 16
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0011' and FY21_DNR_CAT = 'NEW' then 17
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0011' and FY21_DNR_CAT = 'UPGRADED' then 18
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0011' and FY21_DNR_CAT = 'REACTIVATED' then 19
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0011' and FY21_DNR_CAT = 'OLD' then 20
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0101' and FY21_DNR_CAT = 'NEW' then 21
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0101' and FY21_DNR_CAT = 'UPGRADED' then 22
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0101' and FY21_DNR_CAT = 'REACTIVATED' then 23
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0101' and FY21_DNR_CAT = 'OLD' then 24
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1001' and FY21_DNR_CAT = 'NEW' then 25
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1001' and FY21_DNR_CAT = 'UPGRADED' then 26
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1001' and FY21_DNR_CAT = 'REACTIVATED' then 27
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1001' and FY21_DNR_CAT = 'OLD' then 28
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0001' and FY21_DNR_CAT = 'NEW' then 29
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0001' and FY21_DNR_CAT = 'UPGRADED' then 30
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0001' and FY21_DNR_CAT = 'REACTIVATED' then 31
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0001' and FY21_DNR_CAT = 'OLD' then 32
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1110' and FY21_DNR_CAT = 'NEW' then 33
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1110' and FY21_DNR_CAT = 'UPGRADED' then 34
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1110' and FY21_DNR_CAT = 'REACTIVATED' then 35
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1110' and FY21_DNR_CAT = 'OLD' then 36
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0110' and FY21_DNR_CAT = 'NEW' then 37
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0110' and FY21_DNR_CAT = 'UPGRADED' then 38
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0110' and FY21_DNR_CAT = 'REACTIVATED' then 39
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0110' and FY21_DNR_CAT = 'OLD' then 40
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1010' and FY21_DNR_CAT = 'NEW' then 41
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1010' and FY21_DNR_CAT = 'UPGRADED' then 42
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1010' and FY21_DNR_CAT = 'REACTIVATED' then 43
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1010' and FY21_DNR_CAT = 'OLD' then 44
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1100' and FY21_DNR_CAT = 'NEW' then 45
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1100' and FY21_DNR_CAT = 'UPGRADED' then 46
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1100' and FY21_DNR_CAT = 'REACTIVATED' then 47
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1100' and FY21_DNR_CAT = 'OLD' then 48
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0010' and FY21_DNR_CAT = 'NEW' then 49
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0010' and FY21_DNR_CAT = 'UPGRADED' then 50
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0010' and FY21_DNR_CAT = 'REACTIVATED' then 51
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0010' and FY21_DNR_CAT = 'OLD' then 52
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0100' and FY21_DNR_CAT = 'NEW' then 53
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0100' and FY21_DNR_CAT = 'UPGRADED' then 54
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0100' and FY21_DNR_CAT = 'REACTIVATED' then 55
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0100' and FY21_DNR_CAT = 'OLD' then 56
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1000' and FY21_DNR_CAT = 'NEW' then 57
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1000' and FY21_DNR_CAT = 'UPGRADED' then 58
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1000' and FY21_DNR_CAT = 'REACTIVATED' then 59
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1000' and FY21_DNR_CAT = 'OLD' then 60
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0000' and FY21_DNR_CAT = 'NEW'  then 61
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0000' and FY21_DNR_CAT = 'UPGRADED' then 62
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0000' and FY21_DNR_CAT = 'REACTIVATED' then 63
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0000' and FY21_DNR_CAT = 'OLD' then 64
else 65 end as 'Pattern_Number'

, iif(CF_2022_Q1 > 0 , 1 , 0) as 'Gave_in_2022_Q1'  --==> needed to relate with patterns to create vector of prior probabilities 
                                                   --scenario is time is set up by the end of 2021 ... Q1FY21 can be used as a bonus
INTO #FY21Q1_FY22Q1_md_pl_SP_AS_PL 
from #FY21Q1_FY22Q1_QF_PL_SP_as_PL
ORDER BY 1
--(7293 rows affected)
--Completion time: 2022-11-13T19:50:01.5389839-05:00


select b.Pattern_Number 
, B.Pattern_Q
, B.FY21_DNR_CAT
, count (b.Donation_Donor_Id) as 'N_SPONSORS'  -- ==> Input for P(E) Number of donor in Pattern i / Divided by N_Donors
, sum (b.Gave_in_2022_Q1) as 'N_SPONSORS_Gave_101' -- ==> Input for P(H) Number of donors that Gave / Divided by N_Donors
, convert(decimal(5,4) ,sum (b.Gave_in_2022_Q1)* 1./ count (b.Donation_Donor_Id)) as 'Prob_SPR_Gave_PL_NextQ_SP_AS_PL' -- ==> BAYESIAN ESTIMATION OF P(H/E)
--into [SPSS_Sandbox].[dbo].[LTSV_FY21_Prior_Prob_21_22]
into #LTSV_FY21_Prior_Prob_21_22_SP_AS_PL
FROM #FY21Q1_FY22Q1_md_pl_SP_AS_PL  B
group by  b.Pattern_Number , B.Pattern_Q , B.FY21_DNR_CAT
order by 1
--(47 rows affected)
--Completion time: 2022-11-13T19:50:47.3792993-05:00


select * from #LTSV_FY21_Prior_Prob_21_22_SP_AS_PL B
right join [SPSS_Sandbox].[dbo].[LTDV_FY21_PRIOR_PROB_4Q_SP_PL_SG_DEMO] c on c.Pattern_Number = b.Pattern_Number order by c.Pattern_Number

--====================================================
--sponsors as single givers
--=====================================================
----using the same query that produced #FY21Q1_FY22Q1_PS_PL applied to the group of sponsors

 select c.donation_donor_id 
 , a.FY21_DNR_CAT
--Flag by Quarter
, iif (c.CN_2021_Q1  > 0 ,1,0) as 'CF_2021_Q1'
, iif (c.CN_2021_Q2  > 0 ,1,0) as 'CF_2021_Q2'
, iif (c.CN_2021_Q3  > 0 ,1,0) as 'CF_2021_Q3'
, iif (c.CN_2021_Q4  > 0 ,1,0) as 'CF_2021_Q4'
, iif (c.CN_2022_Q1  > 0 ,1,0) as 'CF_2022_Q1'
into #FY21Q1_FY22Q1_QF_SG_SP_AS_SG
from  #FY21Q1_FY22Q1_PS_SG c
join [SPSS_Sandbox].[dbo].[LTDV_SCRIPT_25072022_FY21_SPR_PLR_SGR_CAT_DEMO] a on a.DONATION_DONOR_ID = c.Donation_Donor_Id
join [spss_sandbox].[dbo].[LTDV_SCRIPT_0422_DNR_LABEL] b on b.Donation_Donor_Id = a.DONATION_DONOR_ID
where b.FY21_DNR_LABEL = 'SPONSOR'
order by 1
--(75202 rows affected)
--Completion time: 2022-11-13T20:00:41.4826373-05:00


--getting the pattern and pattern number
SELECT Donation_Donor_Id
, FY21_DNR_CAT
, CASE
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1111' then 'P1111'
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0111' then 'P0111'
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1011' then 'P1011'
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1101' then 'P1101'
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0011' then 'P0011'
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0101' then 'P0101'
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1001' then 'P1001'
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0001' then 'P0001'
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1110' then 'P1110'
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0110' then 'P0110'
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1010' then 'P1010'
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1100' then 'P1100'
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0010' then 'P0010'
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0100' then 'P0100'
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1000' then 'P1000'
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0000' then 'P0000'
else 'UNDEFINED' end as 'Pattern_Q'

--64 COMBINATIONS OF PATTERN AND DONOR CATEGORY AND A OTHER : 65 GROUPS
, CASE
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1111' and FY21_DNR_CAT = 'NEW' then 1
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1111' and FY21_DNR_CAT = 'UPGRADED' then 2
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1111' and FY21_DNR_CAT = 'REACTIVATED' then 3
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1111' and FY21_DNR_CAT = 'OLD' then 4
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0111' and FY21_DNR_CAT = 'NEW' then 5
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0111' and FY21_DNR_CAT = 'UPGRADED' then 6
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0111' and FY21_DNR_CAT = 'REACTIVATED' then 7
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0111' and FY21_DNR_CAT = 'OLD' then 8
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1011' and FY21_DNR_CAT = 'NEW' then 9
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1011' and FY21_DNR_CAT = 'UPGRADED' then 10
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1011' and FY21_DNR_CAT = 'REACTIVATED' then 11
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1011' and FY21_DNR_CAT = 'OLD' then 12
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1101' and FY21_DNR_CAT = 'NEW' then 13
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1101' and FY21_DNR_CAT = 'UPGRADED' then 14
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1101' and FY21_DNR_CAT = 'REACTIVATED' then 15
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1101' and FY21_DNR_CAT = 'OLD' then 16
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0011' and FY21_DNR_CAT = 'NEW' then 17
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0011' and FY21_DNR_CAT = 'UPGRADED' then 18
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0011' and FY21_DNR_CAT = 'REACTIVATED' then 19
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0011' and FY21_DNR_CAT = 'OLD' then 20
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0101' and FY21_DNR_CAT = 'NEW' then 21
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0101' and FY21_DNR_CAT = 'UPGRADED' then 22
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0101' and FY21_DNR_CAT = 'REACTIVATED' then 23
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0101' and FY21_DNR_CAT = 'OLD' then 24
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1001' and FY21_DNR_CAT = 'NEW' then 25
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1001' and FY21_DNR_CAT = 'UPGRADED' then 26
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1001' and FY21_DNR_CAT = 'REACTIVATED' then 27
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1001' and FY21_DNR_CAT = 'OLD' then 28
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0001' and FY21_DNR_CAT = 'NEW' then 29
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0001' and FY21_DNR_CAT = 'UPGRADED' then 30
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0001' and FY21_DNR_CAT = 'REACTIVATED' then 31
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0001' and FY21_DNR_CAT = 'OLD' then 32
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1110' and FY21_DNR_CAT = 'NEW' then 33
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1110' and FY21_DNR_CAT = 'UPGRADED' then 34
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1110' and FY21_DNR_CAT = 'REACTIVATED' then 35
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1110' and FY21_DNR_CAT = 'OLD' then 36
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0110' and FY21_DNR_CAT = 'NEW' then 37
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0110' and FY21_DNR_CAT = 'UPGRADED' then 38
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0110' and FY21_DNR_CAT = 'REACTIVATED' then 39
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0110' and FY21_DNR_CAT = 'OLD' then 40
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1010' and FY21_DNR_CAT = 'NEW' then 41
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1010' and FY21_DNR_CAT = 'UPGRADED' then 42
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1010' and FY21_DNR_CAT = 'REACTIVATED' then 43
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1010' and FY21_DNR_CAT = 'OLD' then 44
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1100' and FY21_DNR_CAT = 'NEW' then 45
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1100' and FY21_DNR_CAT = 'UPGRADED' then 46
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1100' and FY21_DNR_CAT = 'REACTIVATED' then 47
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1100' and FY21_DNR_CAT = 'OLD' then 48
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0010' and FY21_DNR_CAT = 'NEW' then 49
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0010' and FY21_DNR_CAT = 'UPGRADED' then 50
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0010' and FY21_DNR_CAT = 'REACTIVATED' then 51
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0010' and FY21_DNR_CAT = 'OLD' then 52
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0100' and FY21_DNR_CAT = 'NEW' then 53
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0100' and FY21_DNR_CAT = 'UPGRADED' then 54
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0100' and FY21_DNR_CAT = 'REACTIVATED' then 55
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0100' and FY21_DNR_CAT = 'OLD' then 56
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1000' and FY21_DNR_CAT = 'NEW' then 57
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1000' and FY21_DNR_CAT = 'UPGRADED' then 58
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1000' and FY21_DNR_CAT = 'REACTIVATED' then 59
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1000' and FY21_DNR_CAT = 'OLD' then 60
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0000' and FY21_DNR_CAT = 'NEW'  then 61
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0000' and FY21_DNR_CAT = 'UPGRADED' then 62
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0000' and FY21_DNR_CAT = 'REACTIVATED' then 63
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0000' and FY21_DNR_CAT = 'OLD' then 64
else 65 end as 'Pattern_Number'

, iif(CF_2022_Q1 > 0 , 1 , 0) as 'Gave_in_2022_Q1'  --==> needed to relate with patterns to create vector of prior probabilities 
                                                   --scenario is time is set up by the end of 2021 ... Q1FY21 can be used as a bonus
INTO #FY21Q1_FY22Q1_md_SG_SP_AS_SG 
from #FY21Q1_FY22Q1_QF_SG_SP_AS_SG 
ORDER BY 1
--(75202 rows affected)
--Completion time: 2022-11-13T20:01:34.2490136-05:00

select b.Pattern_Number 
, B.Pattern_Q
, B.FY21_DNR_CAT
, count (b.Donation_Donor_Id) as 'N_SPONSORS'  -- ==> Input for P(E) Number of donor in Pattern i / Divided by N_Donors
, sum (b.Gave_in_2022_Q1) as 'N_SPONSORS_Gave_103_108' -- ==> Input for P(H) Number of donors that Gave / Divided by N_Donors
, convert(decimal(5,4) ,sum (b.Gave_in_2022_Q1)* 1./ count (b.Donation_Donor_Id)) as 'Prob_SPR_Gave_SG_NextQ' -- ==> BAYESIAN ESTIMATION OF P(H/E)
--into [SPSS_Sandbox].[dbo].[LTSV_FY21_Prior_Prob_21_22]
into #LTSV_FY21_Prior_Prob_21_22_SP_AS_SG
FROM #FY21Q1_FY22Q1_md_SG_SP_AS_SG B
group by  b.Pattern_Number , B.Pattern_Q , B.FY21_DNR_CAT
order by 1

--(60 rows affected)
--Completion time: 2022-11-13T20:02:15.3156614-05:00

--=====================================
--pledgers as single givers
--======================================

 select c.donation_donor_id 
 , a.FY21_DNR_CAT
--Flag by Quarter
, iif (c.CN_2021_Q1  > 0 ,1,0) as 'CF_2021_Q1'
, iif (c.CN_2021_Q2  > 0 ,1,0) as 'CF_2021_Q2'
, iif (c.CN_2021_Q3  > 0 ,1,0) as 'CF_2021_Q3'
, iif (c.CN_2021_Q4  > 0 ,1,0) as 'CF_2021_Q4'
, iif (c.CN_2022_Q1  > 0 ,1,0) as 'CF_2022_Q1'
into #FY21Q1_FY22Q1_QF_SG_PL_AS_SG
from #FY21Q1_FY22Q1_PS_SG c
join [SPSS_Sandbox].[dbo].[LTDV_SCRIPT_25072022_FY21_SPR_PLR_SGR_CAT_DEMO] a on a.DONATION_DONOR_ID = c.Donation_Donor_Id
join [spss_sandbox].[dbo].[LTDV_SCRIPT_0422_DNR_LABEL] b on b.Donation_Donor_Id = a.DONATION_DONOR_ID
where b.FY21_DNR_LABEL = 'PLEDGER'
order by 1
--(2290 rows affected)
--Completion time: 2022-11-13T20:04:20.9796608-05:00


--getting the pattern and pattern number
SELECT Donation_Donor_Id
, FY21_DNR_CAT
, CASE
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1111' then 'P1111'
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0111' then 'P0111'
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1011' then 'P1011'
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1101' then 'P1101'
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0011' then 'P0011'
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0101' then 'P0101'
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1001' then 'P1001'
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0001' then 'P0001'
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1110' then 'P1110'
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0110' then 'P0110'
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1010' then 'P1010'
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1100' then 'P1100'
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0010' then 'P0010'
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0100' then 'P0100'
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1000' then 'P1000'
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0000' then 'P0000'
else 'UNDEFINED' end as 'Pattern_Q'

--64 COMBINATIONS OF PATTERN AND DONOR CATEGORY AND A OTHER : 65 GROUPS
, CASE
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1111' and FY21_DNR_CAT = 'NEW' then 1
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1111' and FY21_DNR_CAT = 'UPGRADED' then 2
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1111' and FY21_DNR_CAT = 'REACTIVATED' then 3
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1111' and FY21_DNR_CAT = 'OLD' then 4
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0111' and FY21_DNR_CAT = 'NEW' then 5
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0111' and FY21_DNR_CAT = 'UPGRADED' then 6
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0111' and FY21_DNR_CAT = 'REACTIVATED' then 7
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0111' and FY21_DNR_CAT = 'OLD' then 8
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1011' and FY21_DNR_CAT = 'NEW' then 9
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1011' and FY21_DNR_CAT = 'UPGRADED' then 10
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1011' and FY21_DNR_CAT = 'REACTIVATED' then 11
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1011' and FY21_DNR_CAT = 'OLD' then 12
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1101' and FY21_DNR_CAT = 'NEW' then 13
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1101' and FY21_DNR_CAT = 'UPGRADED' then 14
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1101' and FY21_DNR_CAT = 'REACTIVATED' then 15
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1101' and FY21_DNR_CAT = 'OLD' then 16
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0011' and FY21_DNR_CAT = 'NEW' then 17
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0011' and FY21_DNR_CAT = 'UPGRADED' then 18
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0011' and FY21_DNR_CAT = 'REACTIVATED' then 19
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0011' and FY21_DNR_CAT = 'OLD' then 20
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0101' and FY21_DNR_CAT = 'NEW' then 21
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0101' and FY21_DNR_CAT = 'UPGRADED' then 22
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0101' and FY21_DNR_CAT = 'REACTIVATED' then 23
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0101' and FY21_DNR_CAT = 'OLD' then 24
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1001' and FY21_DNR_CAT = 'NEW' then 25
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1001' and FY21_DNR_CAT = 'UPGRADED' then 26
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1001' and FY21_DNR_CAT = 'REACTIVATED' then 27
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1001' and FY21_DNR_CAT = 'OLD' then 28
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0001' and FY21_DNR_CAT = 'NEW' then 29
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0001' and FY21_DNR_CAT = 'UPGRADED' then 30
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0001' and FY21_DNR_CAT = 'REACTIVATED' then 31
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0001' and FY21_DNR_CAT = 'OLD' then 32
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1110' and FY21_DNR_CAT = 'NEW' then 33
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1110' and FY21_DNR_CAT = 'UPGRADED' then 34
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1110' and FY21_DNR_CAT = 'REACTIVATED' then 35
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1110' and FY21_DNR_CAT = 'OLD' then 36
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0110' and FY21_DNR_CAT = 'NEW' then 37
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0110' and FY21_DNR_CAT = 'UPGRADED' then 38
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0110' and FY21_DNR_CAT = 'REACTIVATED' then 39
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0110' and FY21_DNR_CAT = 'OLD' then 40
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1010' and FY21_DNR_CAT = 'NEW' then 41
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1010' and FY21_DNR_CAT = 'UPGRADED' then 42
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1010' and FY21_DNR_CAT = 'REACTIVATED' then 43
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1010' and FY21_DNR_CAT = 'OLD' then 44
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1100' and FY21_DNR_CAT = 'NEW' then 45
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1100' and FY21_DNR_CAT = 'UPGRADED' then 46
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1100' and FY21_DNR_CAT = 'REACTIVATED' then 47
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1100' and FY21_DNR_CAT = 'OLD' then 48
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0010' and FY21_DNR_CAT = 'NEW' then 49
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0010' and FY21_DNR_CAT = 'UPGRADED' then 50
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0010' and FY21_DNR_CAT = 'REACTIVATED' then 51
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0010' and FY21_DNR_CAT = 'OLD' then 52
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0100' and FY21_DNR_CAT = 'NEW' then 53
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0100' and FY21_DNR_CAT = 'UPGRADED' then 54
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0100' and FY21_DNR_CAT = 'REACTIVATED' then 55
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0100' and FY21_DNR_CAT = 'OLD' then 56
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1000' and FY21_DNR_CAT = 'NEW' then 57
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1000' and FY21_DNR_CAT = 'UPGRADED' then 58
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1000' and FY21_DNR_CAT = 'REACTIVATED' then 59
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '1000' and FY21_DNR_CAT = 'OLD' then 60
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0000' and FY21_DNR_CAT = 'NEW'  then 61
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0000' and FY21_DNR_CAT = 'UPGRADED' then 62
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0000' and FY21_DNR_CAT = 'REACTIVATED' then 63
WHEN concat( iif(CF_2021_Q1 > 0 , 1 , 0), iif(CF_2021_Q2 > 0 , 1 , 0), iif(CF_2021_Q3 > 0 , 1 , 0), iif(CF_2021_Q4 > 0 , 1 , 0)) = '0000' and FY21_DNR_CAT = 'OLD' then 64
else 65 end as 'Pattern_Number'

, iif(CF_2022_Q1 > 0 , 1 , 0) as 'Gave_in_2022_Q1'  --==> needed to relate with patterns to create vector of prior probabilities 
                                                   --scenario is time is set up by the end of 2021 ... Q1FY21 can be used as a bonus
INTO #FY21Q1_FY22Q1_md_SG_PL_AS_SG 
from #FY21Q1_FY22Q1_QF_SG_PL_AS_SG
ORDER BY 1
--(2290 rows affected)
--Completion time: 2022-11-13T20:09:32.2089542-05:00

select b.Pattern_Number 
, B.Pattern_Q
, B.FY21_DNR_CAT
, count (b.Donation_Donor_Id) as 'N_PLEDGERS'  -- ==> Input for P(E) Number of donor in Pattern i / Divided by N_Donors
, sum (b.Gave_in_2022_Q1) as 'N_PLEDGERS_Gave_103_108' -- ==> Input for P(H) Number of donors that Gave / Divided by N_Donors
, convert(decimal(5,4) ,sum (b.Gave_in_2022_Q1)* 1./ count (b.Donation_Donor_Id)) as 'Prob_PLR_Gave_SG_NextQ_PL_AS_SG' -- ==> BAYESIAN ESTIMATION OF P(H/E)
--into [SPSS_Sandbox].[dbo].[LTSV_FY21_Prior_Prob_21_22]
into #LPTV_FY21_Prior_Prob_21_22_PL_AS_SG
FROM #FY21Q1_FY22Q1_md_SG_PL_AS_SG B
group by  b.Pattern_Number , B.Pattern_Q , B.FY21_DNR_CAT
order by 1
--(51 rows affected)
--Completion time: 2022-11-13T20:10:14.3070992-05:00

--=============================================

--FINAL STEP IN BUILDING THE PRIOR PROBABILITY VECTORS:
--PUTTING ALL six PROBABILITY VECTORS IN ONE SINGLE TABLE
--=============================================

create table #PATTERN_NUMBER_LIST (Pattern_Number integer , Giving_Pattern varchar(4) , FY21_DNR_CAT varchar(12) )
 INSERT INTO #PATTERN_NUMBER_LIST (Pattern_Number, Giving_Pattern, FY21_DNR_CAT)
 Values
( 1 , '1111' , 'NEW') ,
( 2 , '1111' , 'UPGRADED'),
( 3 , '1111' , 'REACTIVATED'),
( 4 , '1111' , 'OLD'),
( 5 , '0111' , 'NEW'),
( 6 , '0111' , 'UPGRADED'),
( 7 , '0111' ,  'REACTIVATED'),
( 8 , '0111' , 'OLD'),
( 9 , '1011' , 'NEW'),
( 10 , '1011' , 'UPGRADED'),
( 11 , '1011' ,  'REACTIVATED'),
( 12 , '1011' , 'OLD'),
( 13 , '1101' , 'NEW'),
( 14 , '1101' , 'UPGRADED'),
( 15 , '1101' ,  'REACTIVATED'),
( 16 , '1101' , 'OLD'),
( 17 , '0011' , 'NEW'),
( 18 , '0011' , 'UPGRADED'),
( 19 , '0011' ,  'REACTIVATED'),
( 20 , '0011' , 'OLD'),
( 21 , '0101' , 'NEW'),
( 22 , '0101' , 'UPGRADED'),
( 23 , '0101' ,  'REACTIVATED'),
( 24 , '0101' , 'OLD'),
( 25 , '1001' , 'NEW'),
( 26 , '1001' , 'UPGRADED'),
( 27 , '1001' ,  'REACTIVATED'),
( 28 , '1001' , 'OLD'),
( 29 , '0001' , 'NEW'),
( 30 , '0001' , 'UPGRADED'),
( 31 , '0001' ,  'REACTIVATED'),
( 32 , '0001' , 'OLD'),
( 33 , '1110' , 'NEW'),
( 34 , '1110' , 'UPGRADED'),
( 35 , '1110' ,  'REACTIVATED'),
( 36 , '1110' , 'OLD'),
( 37 , '0110' , 'NEW'),
( 38 , '0110' , 'UPGRADED'),
( 39 , '0110' ,  'REACTIVATED'),
( 40 , '0110' , 'OLD'),
( 41 , '1010' , 'NEW'),
( 42 , '1010' , 'UPGRADED'),
( 43 , '1010' ,  'REACTIVATED'),
( 44 , '1010' , 'OLD'),
( 45 , '1100' , 'NEW'),
( 46 , '1100' , 'UPGRADED'),
( 47 , '1100' ,  'REACTIVATED'),
( 48 , '1100' , 'OLD'),
( 49 , '0010' , 'NEW'),
( 50 , '0010' , 'UPGRADED'),
( 51 , '0010' ,  'REACTIVATED'),
( 52 , '0010' , 'OLD'),
( 53 , '0100' , 'NEW'),
( 54 , '0100' , 'UPGRADED'),
( 55 , '0100' ,  'REACTIVATED'),
( 56 , '0100' , 'OLD'),
( 57 , '1000' ,  'REACTIVATED'),
( 58 , '1000' , 'UPGRADED'),
( 59 , '1000' ,  'REACTIVATED'),
( 60 , '1000' , 'OLD'),
( 61 , '0000' , 'NEW'),
( 62 , '0000' , 'UPGRADED'),
( 63 , '0000' ,  'REACTIVATED'),
( 64 , '0000' , 'OLD')

--(64 rows affected)
--Completion time: 2022-11-08T02:03:10.9162783-05:00

SELECT ptn.Pattern_Number
, ptn.Giving_Pattern
, ptn.FY21_DNR_CAT
--sponsors
, ISNULL(a.N_Sponsors,0) as 'Nbr_SPRS_WTH_102'
, ISNULL(a.N_Sponsors_Gave_102,0) as 'N_SPRS_Gave_102'
, ISNULL(a.Prob_SPR_Gave_SPP_NextQ,0) as 'Prob_SPR_Gave_SPP_NextQ'
--pledges
, ISNULL(d.N_Sponsors,0) as 'Nbr_SPRS_WTH_101'
, ISNULL(d.N_SPONSORS_Gave_101,0) as 'N_SPRS_Gave_101'
, ISNULL(d.Prob_SPR_Gave_PL_NextQ_SP_AS_PL,0) as 'Prob_SPR_Gave_PL_NextQ'
--single_gifts
, ISNULL(e.N_SPONSORS,0) as 'Nbr_SPRS_WTH_103_108'
, ISNULL(e.N_SPONSORS_Gave_103_108,0) as 'N_SPRS_Gave_103_108'
, ISNULL(e.Prob_SPR_Gave_SG_NextQ,0) as 'Prob_SPR_Gave_SG_NextQ'

--PLEDGERS
--pledges
, ISNULL(B.N_Pledgers,0) as 'Nbr_PLRS_WTH_101'
, ISNULL(B.N_Pledgers_Gave_101,0) as 'N_PLRS_Gave_101'
, ISNULL(B.Prob_PLR_Gave_PL_NextQ,0) as 'Prob_PLR_Gave_PL_NextQ'
--single_gifts
, ISNULL(F.N_PLEDGERS,0) as 'Nbr_PLRS_WTH_103_108'
, ISNULL(F.N_PLEDGERS_Gave_103_108,0) as 'N_PLR_Gave_103_to_108'
, ISNULL(F.Prob_PLR_Gave_SG_NextQ_PL_AS_SG,0) as 'Prob_PLR_Gave_SG_NextQ'

--SINGLE GIFT DONORS
--single_gifts
, ISNULL(C.N_Single_Givers,0) as 'Nbr_SGRS_WTH_103_108'
, ISNULL(C.N_Single_Givers_Gave_103_108,0) as 'Nbr_SGRS_Gave_103_to_108'
, ISNULL(C.Prob_SGR_Gave_SG_NextQ,0) as 'Prob_SGR_Gave_SG_NextQ'

INTO [SPSS_Sandbox].[dbo].[LTDV_FY21_PRIOR_PROB_VECTORS_SP_PL_SG_DEMO]
LEFT JOIN #LTSV_FY21_Prior_Prob_21_22 A on A.Pattern_Number = ptn.Pattern_Number
LEFT JOIN #LTPV_FY21_Prior_Prob_21_22 B ON B.Pattern_Number = ptn.PATTERN_NUMBER 
LEFT JOIN #LTSG_FY21_Prior_Prob_21_22 C ON c.Pattern_Number = ptn.Pattern_Number
LEFT JOIN #LTSV_FY21_Prior_Prob_21_22_SP_AS_PL d ON d.Pattern_Number = ptn.Pattern_Number
LEFT JOIN #LTSV_FY21_Prior_Prob_21_22_SP_AS_SG e ON e.Pattern_Number = ptn.Pattern_Number
LEFT JOIN #LPTV_FY21_Prior_Prob_21_22_PL_AS_SG F ON f.Pattern_Number = ptn.Pattern_Number

ORDER BY PTN.Pattern_Number

--(64 rows affected)
--Completion time: 2022-11-08T02:20:06.6372874-05:00

SELECT * FROM [SPSS_Sandbox].[dbo].[LTDV_FY21_PRIOR_PROB_VECTORS_SP_PL_SG_DEMO]


--ok the Prior Probs can be now used for their respective Bayesian loops ...
--=====================================================================================================

--BLOCK 5: GENERATING THE BAYESIAN LOOPS: SPONSORS (3) , PLEDGERS (2) AND SINGLE GIVERS
-- TO CREATE THE EXPECT NUMBER OF QUARTERS DONORS WILL CONTRIBUTE FINANCIALLY DURING THEIR TENURE


--======================================================================================================
--BAYESIAN LOOPS: SPONSORS (3) , PLEDGERS (2) AND SINGLE GIVERS

--======================================================================================================
--SPONSORS: BAYESIAN LOOPS (3)

--SPONSORS AS SPONSORS
--SPONSORS AS PLEDGERS
--SPONSORS AS SINGLE GIFT GIVERS
--=======================================================================================================

--SPONSORS AS SPONSORS
--sponsor_categories and labels in 
--select top 10 * from [SPSS_Sandbox].[dbo].[LTDV_SCRIPT_FY21_25072022_PAST_INCOME_TNR_DEMO]

--==================================================================================================================================================
--NOT PART OF THE LOOP 
--===================================================================================================================================================
--===================================================================================================================================================
--TABLE 1: #EX_ITER_ORDER -- SEQUENCE OF 4 QUARTERS TO BE PICKED FOR BUILDING THE RELEVANT 4 QUARTERS PATTERN PER SPONSOR 
--THE ROWS CORRESPOND TO THE PREDICTION NUMBER, THE COLUMNS TO THE GENERIC ROLE Q1, Q2,Q3,Q4 
--THIS GENERIC ROLE WILL BE PICKED AS PART OF THE EXTERNAL LOOP ... 
--===================================================================================================================================================

create table EX_ITER_ORDER (EXITER_N integer , EXITER_Desc varchar(7) , Q1 varchar(7) , Q2 varchar(7) , Q3 varchar(7) , Q4 varchar(7) )
 INSERT INTO EX_ITER_ORDER ( EXITER_N , EXITER_Desc, Q1 , Q2 , Q3 , Q4 )
 Values 
(1, 'FY23_Q1' , 'Q1' , 'Q2' , 'Q3' , 'Q4' ) ,
(2, 'FY23_Q2' , 'Q2' , 'Q3' , 'Q4' , 'PRED_1' ) ,
(3, 'FY23_Q3' , 'Q3' , 'Q4' , 'PRED_1' , 'PRED_2' ) ,
(4, 'FY23_Q4' , 'Q4' , 'PRED_1' , 'PRED_2' , 'PRED_3' ) ,
(5, 'FY24_Q1' , 'PRED_1' , 'PRED_2' , 'PRED_3' , 'PRED_4' ) ,
(6, 'FY24_Q2' , 'PRED_2' , 'PRED_3' , 'PRED_4' , 'PRED_5' ) ,
(7, 'FY24_Q3' , 'PRED_3' , 'PRED_4' , 'PRED_5' , 'PRED_6' ) ,
(8, 'FY24_Q4' , 'PRED_4' , 'PRED_5' , 'PRED_6' , 'PRED_7' ) ,
(9, 'FY25_Q1' , 'PRED_5' , 'PRED_6' , 'PRED_7' , 'PRED_8' ) ,
(10, 'FY25_Q2' , 'PRED_6' , 'PRED_7' , 'PRED_8' , 'PRED_9' ) ,
(11, 'FY25_Q3' , 'PRED_7' , 'PRED_8' , 'PRED_9' , 'PRED_10' ) ,
(12, 'FY25_Q4' , 'PRED_8' , 'PRED_9' , 'PRED_10' , 'PRED_11' ) ,
(13, 'FY26_Q1' , 'PRED_9' , 'PRED_10' , 'PRED_11' , 'PRED_12' ) ,
(14, 'FY26_Q2' , 'PRED_10' , 'PRED_11' , 'PRED_12' , 'PRED_13' ) ,
(15, 'FY26_Q3' , 'PRED_11' , 'PRED_12' , 'PRED_13' , 'PRED_14' ) ,
(16, 'FY26_Q4' , 'PRED_12' , 'PRED_13' , 'PRED_14' , 'PRED_15' ) ,
(17, 'FY27_Q1' , 'PRED_13' , 'PRED_14' , 'PRED_15' , 'PRED_16' ) ,
(18, 'FY27_Q2' , 'PRED_14' , 'PRED_15' , 'PRED_16' , 'PRED_17' ) ,
(19, 'FY27_Q3' , 'PRED_15' , 'PRED_16' , 'PRED_17' , 'PRED_18' ) ,
(20, 'FY27_Q4' , 'PRED_16' , 'PRED_17' , 'PRED_18' , 'PRED_19' ) ,
(21, 'FY28_Q1' , 'PRED_17' , 'PRED_18' , 'PRED_19' , 'PRED_20' ) ,
(22, 'FY28_Q2' , 'PRED_18' , 'PRED_19' , 'PRED_20' , 'PRED_21' ) ,
(23, 'FY28_Q3' , 'PRED_19' , 'PRED_20' , 'PRED_21' , 'PRED_22' ) ,
(24, 'FY28_Q4' , 'PRED_20' , 'PRED_21' , 'PRED_22' , 'PRED_23' ) ,
(25, 'FY29_Q1' , 'PRED_21' , 'PRED_22' , 'PRED_23' , 'PRED_24' ) ,
(26, 'FY29_Q2' , 'PRED_22' , 'PRED_23' , 'PRED_24' , 'PRED_25' ) ,
(27, 'FY29_Q3' , 'PRED_23' , 'PRED_24' , 'PRED_25' , 'PRED_26' ) ,
(28, 'FY29_Q4' , 'PRED_24' , 'PRED_25' , 'PRED_26' , 'PRED_27' ) ,
(29, 'FY30_Q1' , 'PRED_25' , 'PRED_26' , 'PRED_27' , 'PRED_28' ) ,
(30, 'FY30_Q2' , 'PRED_26' , 'PRED_27' , 'PRED_28' , 'PRED_29' ) ,
(31, 'FY30_Q3' , 'PRED_27' , 'PRED_28' , 'PRED_29' , 'PRED_30' ) ,
(32, 'FY30_Q4' , 'PRED_28' , 'PRED_29' , 'PRED_30' , 'PRED_31' ) ,
(33, 'FY31_Q1' , 'PRED_29' , 'PRED_30' , 'PRED_31' , 'PRED_32' ) ,
(34, 'FY31_Q2' , 'PRED_30' , 'PRED_31' , 'PRED_32' , 'PRED_33' ) ,
(35, 'FY31_Q3' , 'PRED_31' , 'PRED_32' , 'PRED_33' , 'PRED_34' ) ,
(36, 'FY31_Q4' , 'PRED_32' , 'PRED_33' , 'PRED_34' , 'PRED_35' ) ,
(37, 'FY32_Q1' , 'PRED_33' , 'PRED_34' , 'PRED_35' , 'PRED_36' ) ,
(38, 'FY32_Q2' , 'PRED_34' , 'PRED_35' , 'PRED_36' , 'PRED_37' ) ,
(39, 'FY32_Q3' , 'PRED_35' , 'PRED_36' , 'PRED_37' , 'PRED_38' ) ,
(40, 'FY32_Q4' , 'PRED_36' , 'PRED_37' , 'PRED_38' , 'PRED_39' ) ,
(41, 'FY33_Q1' , 'PRED_37' , 'PRED_38' , 'PRED_39' , 'PRED_40' ) ,
(42, 'FY33_Q2' , 'PRED_38' , 'PRED_39' , 'PRED_40' , 'PRED_41' ) ,
(43, 'FY33_Q3' , 'PRED_39' , 'PRED_40' , 'PRED_41' , 'PRED_42' ) ,
(44, 'FY33_Q4' , 'PRED_40' , 'PRED_41' , 'PRED_42' , 'PRED_43' ) ,
(45, 'FY34_Q1' , 'PRED_41' , 'PRED_42' , 'PRED_43' , 'PRED_44' ) ,
(46, 'FY34_Q2' , 'PRED_42' , 'PRED_43' , 'PRED_44' , 'PRED_45' ) ,
(47, 'FY34_Q3' , 'PRED_43' , 'PRED_44' , 'PRED_45' , 'PRED_46' ) ,
(48, 'FY34_Q4' , 'PRED_44' , 'PRED_45' , 'PRED_46' , 'PRED_47' ) ,
(49, 'FY35_Q1' , 'PRED_45' , 'PRED_46' , 'PRED_47' , 'PRED_48' ) ,
(50, 'FY35_Q2' , 'PRED_46' , 'PRED_47' , 'PRED_48' , 'PRED_49' ) ,
(51, 'FY35_Q3' , 'PRED_47' , 'PRED_48' , 'PRED_49' , 'PRED_50' ) ,
(52, 'FY35_Q4' , 'PRED_48' , 'PRED_49' , 'PRED_50' , 'PRED_51' ) ,
(53, 'FY36_Q1' , 'PRED_49' , 'PRED_50' , 'PRED_51' , 'PRED_52' ) ,
(54, 'FY36_Q2' , 'PRED_50' , 'PRED_51' , 'PRED_52' , 'PRED_53' ) ,
(55, 'FY36_Q3' , 'PRED_51' , 'PRED_52' , 'PRED_53' , 'PRED_54' ) ,
(56, 'FY36_Q4' , 'PRED_52' , 'PRED_53' , 'PRED_54' , 'PRED_55' ) ,
(57, 'FY37_Q1' , 'PRED_53' , 'PRED_54' , 'PRED_55' , 'PRED_56' ) ,
(58, 'FY37_Q2' , 'PRED_54' , 'PRED_55' , 'PRED_56' , 'PRED_57' ) ,
(59, 'FY37_Q3' , 'PRED_55' , 'PRED_56' , 'PRED_57' , 'PRED_58' ) ,
(60, 'FY37_Q4' , 'PRED_56' , 'PRED_57' , 'PRED_58' , 'PRED_59' ) ,
(61, 'FY38_Q1' , 'PRED_57' , 'PRED_58' , 'PRED_59' , 'PRED_60' ) ,
(62, 'FY38_Q2' , 'PRED_58' , 'PRED_59' , 'PRED_60' , 'PRED_61' ) ,
(63, 'FY38_Q3' , 'PRED_59' , 'PRED_60' , 'PRED_61' , 'PRED_62' ) ,
(64, 'FY38_Q4' , 'PRED_60' , 'PRED_61' , 'PRED_62' , 'PRED_63' ) ,
(65, 'FY39_Q1' , 'PRED_61' , 'PRED_62' , 'PRED_63' , 'PRED_64' ) ,
(66, 'FY39_Q2' , 'PRED_62' , 'PRED_63' , 'PRED_64' , 'PRED_65' ) ,
(67, 'FY39_Q3' , 'PRED_63' , 'PRED_64' , 'PRED_65' , 'PRED_66' ) ,
(68, 'FY39_Q4' , 'PRED_64' , 'PRED_65' , 'PRED_66' , 'PRED_67' ) ,
(69, 'FY40_Q1' , 'PRED_65' , 'PRED_66' , 'PRED_67' , 'PRED_68' ) ,
(70, 'FY40_Q2' , 'PRED_66' , 'PRED_67' , 'PRED_68' , 'PRED_69' ) ,
(71, 'FY40_Q3' , 'PRED_67' , 'PRED_68' , 'PRED_69' , 'PRED_70' ) ,
(72, 'FY40_Q4' , 'PRED_68' , 'PRED_69' , 'PRED_70' , 'PRED_71' ) ,
(73, 'FY41_Q1' , 'PRED_69' , 'PRED_70' , 'PRED_71' , 'PRED_72' ) ,
(74, 'FY41_Q2' , 'PRED_70' , 'PRED_71' , 'PRED_72' , 'PRED_73' ) ,
(75, 'FY41_Q3' , 'PRED_71' , 'PRED_72' , 'PRED_73' , 'PRED_74' ) ,
(76, 'FY41_Q4' , 'PRED_72' , 'PRED_73' , 'PRED_74' , 'PRED_75' ) ,
(77, 'FY42_Q1' , 'PRED_73' , 'PRED_74' , 'PRED_75' , 'PRED_76' ) ,
(78, 'FY42_Q2' , 'PRED_74' , 'PRED_75' , 'PRED_76' , 'PRED_77' ) ,
(79, 'FY42_Q3' , 'PRED_75' , 'PRED_76' , 'PRED_77' , 'PRED_78' ) ,
(80, 'FY42_Q4' , 'PRED_76' , 'PRED_77' , 'PRED_78' , 'PRED_79' ) ,
(81, 'FY43_Q1' , 'PRED_77' , 'PRED_78' , 'PRED_79' , 'PRED_80' ) 

--THIS IS INPUT FOR THE LOOP  --THE INITIAL TABLE FROM WHICH EVERYTHING STARTS
--(40 rows affected)
--Completion time: 2022-11-07T12:01:02.5131575-05:00

select * from #EX_ITER_ORDER
order by 1
--select max(EXITER_N) from #EX_ITER_ORDER
--drop table  #EX_ITER_ORDER
--=================================================================================================
--iteration 1
--=================================================================================================
--TABLE 2: HISTORICAL GIVING FLAG PER DONOR FOR last 5 quarters pattern plus prediction AND RELEVANT SPONSOR CATEGORY ... 
--===================================================================================================================================================
--Needs to define last 4 quarters data available ... 

CREATE OR REPLACE TABLE PRED_MODEL_FEATURE.MASTER_PRED_CONSOLIDATED AS
select dn.Donation_Donor_Id as PR_DONOR_ID
, a.DNR_CAT
, ifnull(max(iff(cal.Tri_Fin_Ds_Lg = '2022 Q1' ,1,0)),0) as Q1
, ifnull(max(iff(cal.Tri_Fin_Ds_Lg = '2022 Q2' ,1,0)),0) as Q2
, ifnull(max(iff(cal.Tri_Fin_Ds_Lg = '2022 Q3' ,1,0)),0) as Q3
, ifnull(max(iff(cal.Tri_Fin_Ds_Lg = '2022 Q4' ,1,0)),0) as Q4
from ADOBE.RAW.F_DONATION dn
join adobe.raw.D_Cal cal on cal.Dt = dn.Donation_Deposit_Date
join pred_model_feature.LTDV_SCRIPT_PAST_INCOME_TNR a on a.DONATION_DONOR_ID = dn.Donation_Donor_Id
where dn.Donation_Adjustment_Reason_Sid = 0
and dn.Donation_Income_Type_for_Donor_Metrics_Sid = 102
and dn.Donation_Deposit_Date between '2021-10-01' and '2022-09-30'
group by dn.Donation_Donor_Id, a.DNR_CAT
--(241278 rows affected)
--Completion time: 2022-11-07T11:54:09.3489538-05:00

select * from #MASTER_PRED_CONSOLIDATED
--drop table  #MASTER_PRED_CONSOLIDATED 

--===================================================================================================================================================
--TABLE 3: #MASTER_PATT_CONSOLIDATED -- NUMBER FROM 1 TO 64 THAT SUMMARIZES THE POSITION OF THE SPONSOR IN THE CATEGORY+Q1Q2Q3Q4 PATTERN PER QUARTER 
--THE INITIAL ROW ARE THE DONOR_IDS , THE COLUMNS WILL BE ADDED AS PART OF THE EXTERNAL LOOP  
--THIS GENERIC ROLE WILL BE PICKED AS PART OF THE EXTERNAL LOOP ... 
--===================================================================================================================================================

SELECT A.PR_DONOR_ID 
INTO #MASTER_PATT_CONSOLIDATED
from #MASTER_PRED_CONSOLIDATED A

--(241278 rows affected)
--Completion time: 2022-11-07T12:05:16.8516176-05:00
--DROP TABLE #MASTER_PATT_CONSOLIDATED
--(333616 rows affected)
--SELECT * FROM  #MASTER_PATT_CONSOLIDATED ORDER BY 1

--SELECT PATT_1 , COUNT( PR_DONOR_ID) AS 'CN' FROM  #MASTER_PATT_CONSOLIDATED GROUP BY PATT_1 ORDER BY 1

SELECT * FROM #MASTER_PRED_CONSOLIDATED

--===========================================================================================================================================
-- END OF OUT-OF-THE-LOOP-TABLES
--===========================================================================================================================================
--===========================================================================================================================================
-- START OF EXTERNAL LOOP
--===========================================================================================================================================
--EXTERNAL LOOP 1 - WE NEED TO CREATE THE CODE FOR SELECTING THE RELEVANT SEQUENCE OF 4 QUARTERS TO BE PICKED FOR BUILDING PATTERN PER SPONSOR 
--CAREFUL WITH THE CREATION OF TEMPORAL TABLES AS PART OF THE EXEC()!!!
--tHE Exec command makes temp procedure from THE @sql qUEry and executes it. 
--When that procedure ends, all temp tables created in it will be dropped immediately, so NO access to the created table from outside current dynamic query. 
--use global temp table ##table_name to keep it alive ... MAKE SURE TO DELETE IT AFTER THE PROCESURE IS FINISHED ...
--again  dynamically, cannot use #TEMPTABLE because a local temp table will only exist in the scope of the query that defines it. 
--Using ## creates a global temp table which will be accessible outside the scope of the dynamic query.
--EXTERNAL LOOP 2 - now the table ##PATT_Q1Q2Q3Q4 is processed to create the patterns (65 Pattern_QRT + SP Category) per donor
-- GENERATE PATTERNS FOR EACH SPONSOR BASED ON THE INFORMATION OF QUERY1 Q1,Q2,Q3,Q4 1,0 INDICATORS AND THE DNR_CATEGORY 
--64 COMBINATIONS OF PATTERN AND DONOR CATEGORY AND An OTHER category in case there were some issue with data: 65 GROUPS
--===============================
--EXTERNAL LOOP 3 - now the table #Patt65 is left joined with [SPSS_Sandbox].[dbo].[LTDV_FY21_PRIOR_PROB_VECTORS_SP_PL_SG_DEMO] to create the counts patterns 
--(65 Pattern_QRT + SP Category) per donor and the number of people expected to give per pattern
-- GENERATE THE CONSOLIDATED MATRIX WITH 64 ROWS ONE PATTERN IN EACH ONE AND JOINS WITH [SPSS_Sandbox].[dbo].[LTDV_FY21_PRIOR_PROB_VECTORS_SP_PL_SG_DEMO]
-- THAT HAS THE PRIOR PROBABILITY PER PATTERN TO GENERATE THE NUMBER OF CASES PREDICTED AS GIVING IN THE QUARTER.
-- ==> Input for P(E) Number of donor in Pattern i / Divided by N_Donors
--, sum (b.Gave_in_2020_Q2) as 'N_Donors_Gave' -- ==> Input for P(H) Number of donors that Gave / Divided by N_Donors
--, convert(decimal(5,4) ,sum (b.Gave_in_2020_Q2)* 1./ count (b.Donation_Donor_Id)) as 'Prob_Gave_NextQ' -- ==> BAYESIAN ESTIMATION OF P(H/E)
--into [SPSS_Sandbox].[dbo].[LTSV_FY19_20_Prior_Prob_19_20]

DECLARE @PQRT INTEGER;         -- (1) declares the temporal variable count as an integer 
SET @PQRT = 1 ;     		   -- (2) set the initial value of the temporal value @PQRT as 1
DECLARE @PEND INTEGER;         -- (3) declares the variable @PEND 
SET @PEND = (select max(EXITER_N) from  #EX_ITER_ORDER) ;                --(select max(EXITER_N) from  #EX_ITER_ORDER) ;

--EXTERNAL LOOP: DEFINITION OF THE CONDITION FOR WHILE CONDITION THE ITERATION ==>

WHILE (@PQRT <=@PEND)
BEGIN
PRINT @PQRT

 CREATE TABLE #PATTERN_data (PR_DONOR_ID NUMERIC(8,0) , Pattern_Number INTEGER)

DECLARE @Q1 VARCHAR(7)
DECLARE @Q2 VARCHAR(7)
DECLARE @Q3 VARCHAR(7)
DECLARE @Q4 VARCHAR(7)
SET @Q1 = (SELECT R.Q1 FROM #EX_ITER_ORDER R WHERE R.EXITER_N = @PQRT)
SET @Q2 = (SELECT R.Q2 FROM #EX_ITER_ORDER R WHERE R.EXITER_N = @PQRT)
SET @Q3 = (SELECT R.Q3 FROM #EX_ITER_ORDER R WHERE R.EXITER_N = @PQRT)
SET @Q4 = (SELECT R.Q4 FROM #EX_ITER_ORDER R WHERE R.EXITER_N = @PQRT)

PRINT @Q1
PRINT @Q2
PRINT @Q3
PRINT @Q4

--DECLARE @SQL1 NVARCHAR(MAX)
DECLARE @SQL1 NVARCHAR(MAX)
SET @SQL1 = 'SELECT PR_DONOR_ID, DNR_CAT, CAST(CONCAT(' + CAST(@Q1 AS VARCHAR(10)) +  ',' + CAST(@Q2 AS VARCHAR(10)) +  ',' + CAST(@Q3 AS VARCHAR(10)) +  ',' + CAST(@Q4 AS VARCHAR(10))
 + ') AS VARCHAR(4)) AS ''QRT_PATT'' INTO #PATT_Q1Q2Q3Q4 FROM #MASTER_PRED_CONSOLIDATED

INSERT INTO #PATTERN_data (PR_DONOR_ID , Pattern_Number)
SELECT PR_DONOR_ID
, CASE 
WHEN QRT_PATT = ''1111'' and DNR_CAT = ''NEW'' then 1
WHEN QRT_PATT = ''1111'' and DNR_CAT = ''UPGRADED'' then 2
WHEN QRT_PATT = ''1111'' and DNR_CAT = ''REACTIVATED'' then 3
WHEN QRT_PATT = ''1111'' and DNR_CAT = ''OLD'' then 4
WHEN QRT_PATT = ''0111'' and DNR_CAT = ''NEW'' then 5
WHEN QRT_PATT = ''0111'' and DNR_CAT = ''UPGRADED'' then 6
WHEN QRT_PATT = ''0111'' and DNR_CAT = ''REACTIVATED'' then 7
WHEN QRT_PATT = ''0111'' and DNR_CAT = ''OLD'' then 8
WHEN QRT_PATT = ''1011'' and DNR_CAT = ''NEW'' then 9
WHEN QRT_PATT = ''1011'' and DNR_CAT = ''UPGRADED'' then 10
WHEN QRT_PATT = ''1011'' and DNR_CAT = ''REACTIVATED'' then 11
WHEN QRT_PATT = ''1011'' and DNR_CAT = ''OLD'' then 12
WHEN QRT_PATT = ''1101'' and DNR_CAT = ''NEW'' then 13
WHEN QRT_PATT = ''1101'' and DNR_CAT = ''UPGRADED'' then 14
WHEN QRT_PATT = ''1101'' and DNR_CAT = ''REACTIVATED'' then 15
WHEN QRT_PATT = ''1101'' and DNR_CAT = ''OLD'' then 16
WHEN QRT_PATT = ''0011'' and DNR_CAT = ''NEW'' then 17
WHEN QRT_PATT = ''0011'' and DNR_CAT = ''UPGRADED'' then 18
WHEN QRT_PATT = ''0011'' and DNR_CAT = ''REACTIVATED'' then 19
WHEN QRT_PATT = ''0011'' and DNR_CAT = ''OLD'' then 20
WHEN QRT_PATT = ''0101'' and DNR_CAT = ''NEW'' then 21
WHEN QRT_PATT = ''0101'' and DNR_CAT = ''UPGRADED'' then 22
WHEN QRT_PATT = ''0101'' and DNR_CAT = ''REACTIVATED'' then 23
WHEN QRT_PATT = ''0101'' and DNR_CAT = ''OLD'' then 24
WHEN QRT_PATT = ''1001'' and DNR_CAT = ''NEW'' then 25
WHEN QRT_PATT = ''1001'' and DNR_CAT = ''UPGRADED'' then 26
WHEN QRT_PATT = ''1001'' and DNR_CAT = ''REACTIVATED'' then 27
WHEN QRT_PATT = ''1001'' and DNR_CAT = ''OLD'' then 28
WHEN QRT_PATT = ''0001'' and DNR_CAT = ''NEW'' then 29
WHEN QRT_PATT = ''0001'' and DNR_CAT = ''UPGRADED'' then 30
WHEN QRT_PATT = ''0001'' and DNR_CAT = ''REACTIVATED'' then 31
WHEN QRT_PATT = ''0001'' and DNR_CAT = ''OLD'' then 32
WHEN QRT_PATT = ''1110'' and DNR_CAT = ''NEW'' then 33
WHEN QRT_PATT = ''1110'' and DNR_CAT = ''UPGRADED'' then 34
WHEN QRT_PATT = ''1110'' and DNR_CAT = ''REACTIVATED'' then 35
WHEN QRT_PATT = ''1110'' and DNR_CAT = ''OLD'' then 36
WHEN QRT_PATT = ''0110'' and DNR_CAT = ''NEW'' then 37
WHEN QRT_PATT = ''0110'' and DNR_CAT = ''UPGRADED'' then 38
WHEN QRT_PATT = ''0110'' and DNR_CAT = ''REACTIVATED'' then 39
WHEN QRT_PATT = ''0110'' and DNR_CAT = ''OLD'' then 40
WHEN QRT_PATT = ''1010'' and DNR_CAT = ''NEW'' then 41
WHEN QRT_PATT = ''1010'' and DNR_CAT = ''UPGRADED'' then 42
WHEN QRT_PATT = ''1010'' and DNR_CAT = ''REACTIVATED'' then 43
WHEN QRT_PATT = ''1010'' and DNR_CAT = ''OLD'' then 44
WHEN QRT_PATT = ''1100'' and DNR_CAT = ''NEW'' then 45
WHEN QRT_PATT = ''1100'' and DNR_CAT = ''UPGRADED'' then 46
WHEN QRT_PATT = ''1100'' and DNR_CAT = ''REACTIVATED'' then 47
WHEN QRT_PATT = ''1100'' and DNR_CAT = ''OLD'' then 48
WHEN QRT_PATT = ''0010'' and DNR_CAT = ''NEW'' then 49
WHEN QRT_PATT = ''0010'' and DNR_CAT = ''UPGRADED'' then 50
WHEN QRT_PATT = ''0010'' and DNR_CAT = ''REACTIVATED'' then 51
WHEN QRT_PATT = ''0010'' and DNR_CAT = ''OLD'' then 52
WHEN QRT_PATT = ''0100'' and DNR_CAT = ''NEW'' then 53
WHEN QRT_PATT = ''0100'' and DNR_CAT = ''UPGRADED'' then 54
WHEN QRT_PATT = ''0100'' and DNR_CAT = ''REACTIVATED'' then 55
WHEN QRT_PATT = ''0100'' and DNR_CAT = ''OLD'' then 56
WHEN QRT_PATT = ''1000'' and DNR_CAT = ''NEW'' then 57
WHEN QRT_PATT = ''1000'' and DNR_CAT = ''UPGRADED'' then 58
WHEN QRT_PATT = ''1000'' and DNR_CAT = ''REACTIVATED'' then 59
WHEN QRT_PATT = ''1000'' and DNR_CAT = ''OLD'' then 60
WHEN QRT_PATT = ''0000'' and DNR_CAT = ''NEW''  then 61
WHEN QRT_PATT = ''0000'' and DNR_CAT = ''UPGRADED'' then 62
WHEN QRT_PATT = ''0000'' and DNR_CAT = ''REACTIVATED'' then 63
WHEN QRT_PATT = ''0000'' and DNR_CAT = ''OLD'' then 64
else 65 end as ''Pattern_Number''
FROM #PATT_Q1Q2Q3Q4'

EXECUTE (@SQL1)

--SELECT  * FROM #PATTERN_data
--select * from [SPSS_Sandbox].[dbo].[LTDV_FY21_PRIOR_PROB_VECTORS_SP_PL_SG_DEMO]

select a.Pattern_Number
, count (b.PR_DONOR_ID) as 'N_Donors'
, a.Prob_SPR_Gave_SPP_NextQ 
, round(a.Prob_SPR_Gave_SPP_NextQ*count (b.PR_DONOR_ID),0) as 'Y_Give'
, count (b.PR_DONOR_ID) - round(a.Prob_SPR_Gave_SPP_NextQ*count (b.PR_DONOR_ID),0) as 'N_Give' ----> calculation just uses relevant SP Prob vector 
into #Patt65_CONSOL
FROM [SPSS_Sandbox].[dbo].[LTDV_FY21_PRIOR_PROB_VECTORS_SP_PL_SG_DEMO] a   --==> this table has the information on patterns 
left join  #PATTERN_data b on a.Pattern_Number = b.Pattern_Number
group by a.Pattern_Number , a.Prob_SPR_Gave_SPP_NextQ
order by 1

--SELECT * FROM #Patt65_CONSOL ORDER BY 1

--NOW IT IS THE TIME TO INSERT THE INTERNAL LOOP THAT WILL PROCESS EVERYONE OF THE 64 GROUPS IN ORDER TO CREATE A FLAG give/didNOT give PER DONOR PER QUARTER
--THE INTERNAL LOOP RUNS 64 TIMES PER QUARTER.
---BUT BEFORE THAT

--EXTERNAL LOOP 4: table #QRT_PREDICTION THAT STORES THE PREDICTION FOR ALL DONORS FOR THE QUARTER
--The table #QRT_PREDICTION will store all the quarter results coming from the internal loop 
--Must be created OUT of both the external and internal loop since the table is created just once
--and must BE just populated at the end of every external loop iteration 

CREATE TABLE #QRT_PREDICTION (PR_DONOR_ID NUMERIC(8,0) , PATTERN_NUMBER INTEGER , GAVE_FLG INTEGER )
--SELECT * FROM #QRT_PREDICTION
--DROP TABLE #QRT_PREDICTION
--==========================================================================================================
--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP
--==========================================================================================================
--==========================================================================================================
--==========================================================================================================

--INTERNAL LOOP 1:             
--Basic loop to populate the file of sponsors randomly selected as givers during the period

DECLARE @PATT INTEGER;        -- (1) declares the temporal variable count as an integer
SET @PATT = 1 ;               -- (2) set the initial value of the temporal value @PATT as 1
DECLARE @END INTEGER;         -- (3) SET THE VARIABLE @END THAT DEFINES THE TOTAL NUMBER OF ITERATIONS
SET @END = (SELECT MAX(Y.Pattern_Number) FROM #Patt65_CONSOL Y) 

--INTERNAL LOOP 2: SETTING THE LOOP CONDITION ==> ITERATIONS GO FROM 1 TO 64
WHILE (@PATT <=@END)         -- (3) WHILE defines the loop the code will be executed until @PATT  reaches the value 64 NO SEMICOLON!!!

--INTERNAL LOOP 3: BEGIN ===> END DEFINE THE SPACE FOR THE CODE THAT WILL RUN IN ITERATIONS      
BEGIN	                     -- (4) this defines where the code the loop applies to starts
							-- (5) this is the body of code 
--INTERNAL LOOP 4: 
--FIRST DECLARE AND DEFINE THE VARIABLE @NY THAT IS THE NUMBER OF SPONSORS THAT ARE PREDICTED WILL GIVE IN CATEGORY @PATT
DECLARE @NY INTEGER;
SET @NY = (SELECT Y.Y_GIVE FROM #Patt65_CONSOL Y WHERE Y.Pattern_Number = @PATT)

--INTERNAL LOOP 5: CREATING THE SCORES FOR A PARTICULAR PATTERN @NY AND STORE IT INTO #ONE_PATT
-- #ONE_PATT creates and stores the values for the current loop pattern -- this code stores the randomly selected @NY as GAVE_FLG = 1
select top (@NY) a.PR_DONOR_ID , a.Pattern_Number, 1 AS 'GAVE_FLG' into #ONE_PATT from #PATTERN_data a where a.Pattern_Number = @PATT  ORDER BY NEWID()

-- #ONE_PATT creates and stores the values for the current loop pattern -- this code stores the not randomly selected @NY as GAVE_FLG = 0
insert into #ONE_PATT
select b.PR_DONOR_ID , b.Pattern_Number, 0 AS 'GAVE_FLG' from #PATTERN_data b 
left join #ONE_PATT a on a.PR_DONOR_ID = b.PR_DONOR_ID  
where a.PR_DONOR_ID is null and b.pattern_number = @PATT

--INTERNAL LOOP 6: #ONE_PATT RESULT FROM PATTERN ITERATION STORED IN #QRT_PREDICTION ON A CUMULATIVE BASIS
--now the result of #ONE_PATT which is just for one pattern is stored into the more formal #QRT_Prediction that stores ALL pattern results
insert into #QRT_PREDICTION (PR_DONOR_ID, PATTERN_NUMBER, GAVE_FLG)
select A.PR_DONOR_ID , a.Pattern_Number, a.GAVE_FLG from #ONE_PATT a

--FINALLY THE #ONE_PATT ID DROPPED SO IT CAN BE RE-USED FOR THE NEXT PATTERN IN THE ITERATION 
drop table #ONE_PATT
	

PRINT @PATT;                  -- (5) print instruction ... can be way more complex
SET @PATT = @PATT+1;          -- (6) the @variable @PATT is set up as @PATT +1 in order to allow for the next iteration 
END;						  -- (7) this instruction tells the machine to end the loop 

--by the END all donors must have a GAVE_FLG as 1, 0 as they were randomly selected in their respective Pattern (1 to 64)
--END OF INTERNAL LOOP WHAT COMES NEXT IS PROCESSED AS PART OF THE EXTERNAL LOOP ONLY AS MANY TIMES AS PREDICTION QUSRTERS ARE DEFINED
--==============================================================================================================
--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP
--==============================================================================================================
--DECLARE @PQRT INTEGER;         -- (1) declares the temporal variable count as an integer 
--SET @PQRT = 1 ;     		   -- (2) set the initial value of the temporal value @PQRT as 1
--DECLARE @PEND INTEGER;         -- (3) declares the variable @PEND 
--SET @PEND = 1 ; 
--EXTERNAL LOOP 5: 
--INTRODUCE CODE TO SAVE THE gAVE/dID NOT GIVE PREDICTION FROM #QRT_PREDICTION INTO THE MASTER TABLE #MASTER_PRED_CONSOLIDATED

--THE VARIABLES @SQL2 AND @SQL3 ARE DECLARED NVARCHAR(MAX) MEANING TEXT OF THE CODE WITH EXTENSION AS REQUIRED
--@SQL1 WILL DEAL WITH THE ALTER TABLE CODE (ADD NEW COLUMN TO #INITIAL)
--@SQL2 WILL CORRESPOND TO THE UPDATE OF THE TABLE (ADD CONTENT OF #SEQ_data TO NEW COLUMN IN #INITIAL

    DECLARE @SQL2 NVARCHAR(MAX)   
	DECLARE @SQL3 NVARCHAR(MAX)   

-- First statement add the new row to THE #MASTER_PRED_CONSOLIDATED Table 
-- the name of the column is PRED_1 for the first iteration, PRED_2 for the second and so on ...  it will stop at PRED_i , 

    SET @SQL2 = 'ALTER TABLE #MASTER_PRED_CONSOLIDATED
        ADD ' + concat('PRED_',CAST(@PQRT AS VARCHAR(10))) + ' INTEGER'

-- Second Stement Write the result of the iteration from #SEQ_data into #INITIAL_data into the correct column PRED_i for iteration i

   SET @SQL3 = 'UPDATE #MASTER_PRED_CONSOLIDATED
        SET ' + concat('PRED_',CAST(@PQRT AS VARCHAR(10))) + ' = I.GAVE_FLG
		from 
		#QRT_PREDICTION I,
	    #MASTER_PRED_CONSOLIDATED H
        where
        I.PR_DONOR_ID = H.PR_DONOR_ID'

--THE EXECUTE() CODE WILL ALLOW THE TEXT IN @SQL2 AND @SQL3 TO BE INTERPRETED AS REAL CODE NOT JUST TEXT
EXECUTE (@SQL2)
EXECUTE (@SQL3)

--drop table #QRT_PREDICTION
--EXTERNAL LOOP 6: 
--INTRODUCE CODE TO SAVE THE PATTERN INFORMATION FROM #QRT_PREDICTION INTO THE MASTER_PATT TABLE #MASTER_PATT_CONSOLIDATED
--DECLARE @PQRT INTEGER;         -- (1) declares the temporal variable count as an integer 
--SET @PQRT = 1 ;     		   -- (2) set the initial value of the temporal value @PQRT as 1
--DECLARE @PEND INTEGER;         -- (3) declares the variable @PEND 
--SET @PEND = 1 ; 
--THE VARIABLES @SQL4 AND @SQL5 ARE DECLARED NVARCHAR(MAX) MEANING TEXT OF THE CODE WITH EXTENSION AS REQUIRED
--@SQL1 WILL DEAL WITH THE ALTER TABLE CODE (ADD NEW COLUMN TO #INITIAL)
--@SQL2 WILL CORRESPOND TO THE UPDATE OF THE TABLE (ADD CONTENT OF #SEQ_data TO NEW COLUMN IN #INITIAL

    DECLARE @SQL4 NVARCHAR(MAX)   
	DECLARE @SQL5 NVARCHAR(MAX)   

-- First statement add the new row to THE #MASTER_PRED_CONSOLIDATED Table 
-- the name of the column is PRED_1 for the first iteration, PRED_2 for the second and so on ...  it will stop at PRED_i , 

    SET @SQL4 = 'ALTER TABLE #MASTER_PATT_CONSOLIDATED
        ADD ' + concat('PATT_',CAST(@PQRT AS VARCHAR(10))) + ' INTEGER'

-- Second Stement Write the result of the iteration from #SEQ_data into #INITIAL_data into the correct column PRED_i for iteration i

   SET @SQL5 = 'UPDATE #MASTER_PATT_CONSOLIDATED
        SET ' + concat('PATT_',CAST(@PQRT AS VARCHAR(10))) + ' = I.PATTERN_NUMBER
		from 
		#QRT_PREDICTION I,
	    #MASTER_PATT_CONSOLIDATED H
        where
        I.PR_DONOR_ID = H.PR_DONOR_ID'

--THE EXECUTE() CODE WILL ALLOW THE TEXT IN @SQL4 AND @SQL5 TO BE INTERPRETED AS REAL CODE NOT JUST TEXT
EXECUTE (@SQL4)
EXECUTE (@SQL5)


--FINALLY THE #QRT_PREDICTION TABLE IS DROPPED SO IT CAN BE RE-USED FOR THE NEXT QUARTER (EXTERNAL LOOP ITERATION) PATTERN IN THE ITERATION 
drop table #QRT_PREDICTION
drop table #Patt65_CONSOL
drop table #PATTERN_data
--drop table #Patt65_CONSOL	

--PRINT @PQRT;                  -- (5) print instruction ... can be way more complex
SET @PQRT = @PQRT+1;          -- (6) the @variable @PATT is set up as @PATT +1 in order to allow for the next iteration 
END;						  -- (7) this instruction tells the machine to end the loop 

select * 
INTO [SPSS_Sandbox].[dbo].[LTDV_SP_FY21_SPP_INCOME_FLG_80QRT_DEMO] 
from #MASTER_PRED_CONSOLIDATED
ORDER BY 1

select * 
INTO [SPSS_Sandbox].[dbo].[LTDV_SP_FY21_SPP_PATTERN_N_80Q_RT]
from #MASTER_PATT_CONSOLIDATED
order by 1

--12m:53m
--(241278 rows affected)
--Completion time: 2022-11-08T14:55:19.8319245-05:00



--Completion time: 2022-11-14T22:38:52.4889296-05:00
--==============================================================================================================
--==============================================================================================================
--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP
--==============================================================================================================
drop table #MASTER_PRED_CONSOLIDATED
drop table #MASTER_PATT_CONSOLIDATED

--LOOP TABLES IN [SPSS_SANDBOX]:
select * from [SPSS_Sandbox].[dbo].[LTDV_SP_FY21_SPP_INCOME_FLG_80QRT_DEMO] 
select * from [SPSS_Sandbox].[dbo].[LTDV_SP_FY21_SPP_PATTERN_N_80QRT] order by 1

--ok!! ALgorithm worked beautifully!

--=================================================================================================
--iteration 2
--=================================================================================================
--TABLE 2: HISTORICAL GIVING FLAG PER DONOR FOR last 5 quarters pattern plus prediction AND RELEVANT SPONSOR CATEGORY ... 
--===================================================================================================================================================
--Needs to define last 4 quarters data available ... 

select dn.Donation_Donor_Id as 'PR_DONOR_ID'
, a.DNR_CAT
, isnull(max(iif(cal.Tri_Fin_Ds_Lg = '2022 Q1' ,1,0)),0) as 'Q1'
, isnull(max(iif(cal.Tri_Fin_Ds_Lg = '2022 Q2' ,1,0)),0) as 'Q2'
, isnull(max(iif(cal.Tri_Fin_Ds_Lg = '2022 Q3' ,1,0)),0) as 'Q3'
, isnull(max(iif(cal.Tri_Fin_Ds_Lg = '2022 Q4' ,1,0)),0) as 'Q4'
into #MASTER_PRED_CONSOLIDATED
from ADOBE.RAW.F_DONATION dn
join [BI_DW].[dbo].[D_Cal] cal on cal.Dt = dn.Donation_Deposit_Date
join [SPSS_Sandbox].[dbo].[LTDV_SCRIPT_FY21_25072022_PAST_INCOME_TNR_DEMO] a on a.DONATION_DONOR_ID = dn.Donation_Donor_Id
where dn.Donation_Adjustment_Reason_Sid = 0
and dn.Donation_Income_Type_for_Donor_Metrics_Sid = 102
	   --SCOPE OF THE QUERY must be ONE FISCAL YEAR 
and dn.Donation_Deposit_Date between '2021-10-01' and '2022-09-30'
group by dn.Donation_Donor_Id, a.DNR_CAT
--(241278 rows affected)
--Completion time: 2022-11-07T11:54:09.3489538-05:00

select * from #MASTER_PRED_CONSOLIDATED
--drop table  #MASTER_PRED_CONSOLIDATED 

--===================================================================================================================================================
--TABLE 3: #MASTER_PATT_CONSOLIDATED -- NUMBER FROM 1 TO 64 THAT SUMMARIZES THE POSITION OF THE SPONSOR IN THE CATEGORY+Q1Q2Q3Q4 PATTERN PER QUARTER 
--THE INITIAL ROW ARE THE DONOR_IDS , THE COLUMNS WILL BE ADDED AS PART OF THE EXTERNAL LOOP  
--THIS GENERIC ROLE WILL BE PICKED AS PART OF THE EXTERNAL LOOP ... 
--===================================================================================================================================================

SELECT A.PR_DONOR_ID 
INTO #MASTER_PATT_CONSOLIDATED
from #MASTER_PRED_CONSOLIDATED A

--(241278 rows affected)
--Completion time: 2022-11-07T12:05:16.8516176-05:00
--DROP TABLE #MASTER_PATT_CONSOLIDATED
--(333616 rows affected)
--SELECT * FROM  #MASTER_PATT_CONSOLIDATED ORDER BY 1

--SELECT PATT_1 , COUNT( PR_DONOR_ID) AS 'CN' FROM  #MASTER_PATT_CONSOLIDATED GROUP BY PATT_1 ORDER BY 1

SELECT * FROM #MASTER_PRED_CONSOLIDATED

--===========================================================================================================================================
-- END OF OUT-OF-THE-LOOP-TABLES
--===========================================================================================================================================
--===========================================================================================================================================
-- START OF EXTERNAL LOOP
--===========================================================================================================================================
--EXTERNAL LOOP 1 - WE NEED TO CREATE THE CODE FOR SELECTING THE RELEVANT SEQUENCE OF 4 QUARTERS TO BE PICKED FOR BUILDING PATTERN PER SPONSOR 
--CAREFUL WITH THE CREATION OF TEMPORAL TABLES AS PART OF THE EXEC()!!!
--tHE Exec command makes temp procedure from THE @sql qUEry and executes it. 
--When that procedure ends, all temp tables created in it will be dropped immediately, so NO access to the created table from outside current dynamic query. 
--use global temp table ##table_name to keep it alive ... MAKE SURE TO DELETE IT AFTER THE PROCESURE IS FINISHED ...
--again  dynamically, cannot use #TEMPTABLE because a local temp table will only exist in the scope of the query that defines it. 
--Using ## creates a global temp table which will be accessible outside the scope of the dynamic query.
--EXTERNAL LOOP 2 - now the table ##PATT_Q1Q2Q3Q4 is processed to create the patterns (65 Pattern_QRT + SP Category) per donor
-- GENERATE PATTERNS FOR EACH SPONSOR BASED ON THE INFORMATION OF QUERY1 Q1,Q2,Q3,Q4 1,0 INDICATORS AND THE DNR_CATEGORY 
--64 COMBINATIONS OF PATTERN AND DONOR CATEGORY AND An OTHER category in case there were some issue with data: 65 GROUPS
--===============================
--EXTERNAL LOOP 3 - now the table #Patt65 is left joined with [SPSS_Sandbox].[dbo].[LTDV_FY21_PRIOR_PROB_VECTORS_SP_PL_SG_DEMO] to create the counts patterns 
--(65 Pattern_QRT + SP Category) per donor and the number of people expected to give per pattern
-- GENERATE THE CONSOLIDATED MATRIX WITH 64 ROWS ONE PATTERN IN EACH ONE AND JOINS WITH [SPSS_Sandbox].[dbo].[LTDV_FY21_PRIOR_PROB_VECTORS_SP_PL_SG_DEMO]
-- THAT HAS THE PRIOR PROBABILITY PER PATTERN TO GENERATE THE NUMBER OF CASES PREDICTED AS GIVING IN THE QUARTER.
-- ==> Input for P(E) Number of donor in Pattern i / Divided by N_Donors
--, sum (b.Gave_in_2020_Q2) as 'N_Donors_Gave' -- ==> Input for P(H) Number of donors that Gave / Divided by N_Donors
--, convert(decimal(5,4) ,sum (b.Gave_in_2020_Q2)* 1./ count (b.Donation_Donor_Id)) as 'Prob_Gave_NextQ' -- ==> BAYESIAN ESTIMATION OF P(H/E)
--into [SPSS_Sandbox].[dbo].[LTSV_FY19_20_Prior_Prob_19_20]

DECLARE @PQRT INTEGER;         -- (1) declares the temporal variable count as an integer 
SET @PQRT = 1 ;     		   -- (2) set the initial value of the temporal value @PQRT as 1
DECLARE @PEND INTEGER;         -- (3) declares the variable @PEND 
SET @PEND = (select max(EXITER_N) from  #EX_ITER_ORDER) ;                --(select max(EXITER_N) from  #EX_ITER_ORDER) ;

--EXTERNAL LOOP: DEFINITION OF THE CONDITION FOR WHILE CONDITION THE ITERATION ==>

WHILE (@PQRT <=@PEND)
BEGIN
PRINT @PQRT

 CREATE TABLE #PATTERN_data (PR_DONOR_ID NUMERIC(8,0) , Pattern_Number INTEGER)

DECLARE @Q1 VARCHAR(7)
DECLARE @Q2 VARCHAR(7)
DECLARE @Q3 VARCHAR(7)
DECLARE @Q4 VARCHAR(7)
SET @Q1 = (SELECT R.Q1 FROM #EX_ITER_ORDER R WHERE R.EXITER_N = @PQRT)
SET @Q2 = (SELECT R.Q2 FROM #EX_ITER_ORDER R WHERE R.EXITER_N = @PQRT)
SET @Q3 = (SELECT R.Q3 FROM #EX_ITER_ORDER R WHERE R.EXITER_N = @PQRT)
SET @Q4 = (SELECT R.Q4 FROM #EX_ITER_ORDER R WHERE R.EXITER_N = @PQRT)

PRINT @Q1
PRINT @Q2
PRINT @Q3
PRINT @Q4

--DECLARE @SQL1 NVARCHAR(MAX)
DECLARE @SQL1 NVARCHAR(MAX)
SET @SQL1 = 'SELECT PR_DONOR_ID, DNR_CAT, CAST(CONCAT(' + CAST(@Q1 AS VARCHAR(10)) +  ',' + CAST(@Q2 AS VARCHAR(10)) +  ',' + CAST(@Q3 AS VARCHAR(10)) +  ',' + CAST(@Q4 AS VARCHAR(10))
 + ') AS VARCHAR(4)) AS ''QRT_PATT'' INTO #PATT_Q1Q2Q3Q4 FROM #MASTER_PRED_CONSOLIDATED

INSERT INTO #PATTERN_data (PR_DONOR_ID , Pattern_Number)
SELECT PR_DONOR_ID
, CASE 
WHEN QRT_PATT = ''1111'' and DNR_CAT = ''NEW'' then 1
WHEN QRT_PATT = ''1111'' and DNR_CAT = ''UPGRADED'' then 2
WHEN QRT_PATT = ''1111'' and DNR_CAT = ''REACTIVATED'' then 3
WHEN QRT_PATT = ''1111'' and DNR_CAT = ''OLD'' then 4
WHEN QRT_PATT = ''0111'' and DNR_CAT = ''NEW'' then 5
WHEN QRT_PATT = ''0111'' and DNR_CAT = ''UPGRADED'' then 6
WHEN QRT_PATT = ''0111'' and DNR_CAT = ''REACTIVATED'' then 7
WHEN QRT_PATT = ''0111'' and DNR_CAT = ''OLD'' then 8
WHEN QRT_PATT = ''1011'' and DNR_CAT = ''NEW'' then 9
WHEN QRT_PATT = ''1011'' and DNR_CAT = ''UPGRADED'' then 10
WHEN QRT_PATT = ''1011'' and DNR_CAT = ''REACTIVATED'' then 11
WHEN QRT_PATT = ''1011'' and DNR_CAT = ''OLD'' then 12
WHEN QRT_PATT = ''1101'' and DNR_CAT = ''NEW'' then 13
WHEN QRT_PATT = ''1101'' and DNR_CAT = ''UPGRADED'' then 14
WHEN QRT_PATT = ''1101'' and DNR_CAT = ''REACTIVATED'' then 15
WHEN QRT_PATT = ''1101'' and DNR_CAT = ''OLD'' then 16
WHEN QRT_PATT = ''0011'' and DNR_CAT = ''NEW'' then 17
WHEN QRT_PATT = ''0011'' and DNR_CAT = ''UPGRADED'' then 18
WHEN QRT_PATT = ''0011'' and DNR_CAT = ''REACTIVATED'' then 19
WHEN QRT_PATT = ''0011'' and DNR_CAT = ''OLD'' then 20
WHEN QRT_PATT = ''0101'' and DNR_CAT = ''NEW'' then 21
WHEN QRT_PATT = ''0101'' and DNR_CAT = ''UPGRADED'' then 22
WHEN QRT_PATT = ''0101'' and DNR_CAT = ''REACTIVATED'' then 23
WHEN QRT_PATT = ''0101'' and DNR_CAT = ''OLD'' then 24
WHEN QRT_PATT = ''1001'' and DNR_CAT = ''NEW'' then 25
WHEN QRT_PATT = ''1001'' and DNR_CAT = ''UPGRADED'' then 26
WHEN QRT_PATT = ''1001'' and DNR_CAT = ''REACTIVATED'' then 27
WHEN QRT_PATT = ''1001'' and DNR_CAT = ''OLD'' then 28
WHEN QRT_PATT = ''0001'' and DNR_CAT = ''NEW'' then 29
WHEN QRT_PATT = ''0001'' and DNR_CAT = ''UPGRADED'' then 30
WHEN QRT_PATT = ''0001'' and DNR_CAT = ''REACTIVATED'' then 31
WHEN QRT_PATT = ''0001'' and DNR_CAT = ''OLD'' then 32
WHEN QRT_PATT = ''1110'' and DNR_CAT = ''NEW'' then 33
WHEN QRT_PATT = ''1110'' and DNR_CAT = ''UPGRADED'' then 34
WHEN QRT_PATT = ''1110'' and DNR_CAT = ''REACTIVATED'' then 35
WHEN QRT_PATT = ''1110'' and DNR_CAT = ''OLD'' then 36
WHEN QRT_PATT = ''0110'' and DNR_CAT = ''NEW'' then 37
WHEN QRT_PATT = ''0110'' and DNR_CAT = ''UPGRADED'' then 38
WHEN QRT_PATT = ''0110'' and DNR_CAT = ''REACTIVATED'' then 39
WHEN QRT_PATT = ''0110'' and DNR_CAT = ''OLD'' then 40
WHEN QRT_PATT = ''1010'' and DNR_CAT = ''NEW'' then 41
WHEN QRT_PATT = ''1010'' and DNR_CAT = ''UPGRADED'' then 42
WHEN QRT_PATT = ''1010'' and DNR_CAT = ''REACTIVATED'' then 43
WHEN QRT_PATT = ''1010'' and DNR_CAT = ''OLD'' then 44
WHEN QRT_PATT = ''1100'' and DNR_CAT = ''NEW'' then 45
WHEN QRT_PATT = ''1100'' and DNR_CAT = ''UPGRADED'' then 46
WHEN QRT_PATT = ''1100'' and DNR_CAT = ''REACTIVATED'' then 47
WHEN QRT_PATT = ''1100'' and DNR_CAT = ''OLD'' then 48
WHEN QRT_PATT = ''0010'' and DNR_CAT = ''NEW'' then 49
WHEN QRT_PATT = ''0010'' and DNR_CAT = ''UPGRADED'' then 50
WHEN QRT_PATT = ''0010'' and DNR_CAT = ''REACTIVATED'' then 51
WHEN QRT_PATT = ''0010'' and DNR_CAT = ''OLD'' then 52
WHEN QRT_PATT = ''0100'' and DNR_CAT = ''NEW'' then 53
WHEN QRT_PATT = ''0100'' and DNR_CAT = ''UPGRADED'' then 54
WHEN QRT_PATT = ''0100'' and DNR_CAT = ''REACTIVATED'' then 55
WHEN QRT_PATT = ''0100'' and DNR_CAT = ''OLD'' then 56
WHEN QRT_PATT = ''1000'' and DNR_CAT = ''NEW'' then 57
WHEN QRT_PATT = ''1000'' and DNR_CAT = ''UPGRADED'' then 58
WHEN QRT_PATT = ''1000'' and DNR_CAT = ''REACTIVATED'' then 59
WHEN QRT_PATT = ''1000'' and DNR_CAT = ''OLD'' then 60
WHEN QRT_PATT = ''0000'' and DNR_CAT = ''NEW''  then 61
WHEN QRT_PATT = ''0000'' and DNR_CAT = ''UPGRADED'' then 62
WHEN QRT_PATT = ''0000'' and DNR_CAT = ''REACTIVATED'' then 63
WHEN QRT_PATT = ''0000'' and DNR_CAT = ''OLD'' then 64
else 65 end as ''Pattern_Number''
FROM #PATT_Q1Q2Q3Q4'

EXECUTE (@SQL1)

--SELECT  * FROM #PATTERN_data
--select * from [SPSS_Sandbox].[dbo].[LTDV_FY21_PRIOR_PROB_VECTORS_SP_PL_SG_DEMO]

select a.Pattern_Number
, count (b.PR_DONOR_ID) as 'N_Donors'
, a.Prob_SPR_Gave_SPP_NextQ 
, round(a.Prob_SPR_Gave_SPP_NextQ*count (b.PR_DONOR_ID),0) as 'Y_Give'
, count (b.PR_DONOR_ID) - round(a.Prob_SPR_Gave_SPP_NextQ*count (b.PR_DONOR_ID),0) as 'N_Give' ----> calculation just uses relevant SP Prob vector 
into #Patt65_CONSOL
FROM [SPSS_Sandbox].[dbo].[LTDV_FY21_PRIOR_PROB_VECTORS_SP_PL_SG_DEMO] a   --==> this table has the information on patterns 
left join  #PATTERN_data b on a.Pattern_Number = b.Pattern_Number
group by a.Pattern_Number , a.Prob_SPR_Gave_SPP_NextQ
order by 1

--SELECT * FROM #Patt65_CONSOL ORDER BY 1

--NOW IT IS THE TIME TO INSERT THE INTERNAL LOOP THAT WILL PROCESS EVERYONE OF THE 64 GROUPS IN ORDER TO CREATE A FLAG give/didNOT give PER DONOR PER QUARTER
--THE INTERNAL LOOP RUNS 64 TIMES PER QUARTER.
---BUT BEFORE THAT

--EXTERNAL LOOP 4: table #QRT_PREDICTION THAT STORES THE PREDICTION FOR ALL DONORS FOR THE QUARTER
--The table #QRT_PREDICTION will store all the quarter results coming from the internal loop 
--Must be created OUT of both the external and internal loop since the table is created just once
--and must BE just populated at the end of every external loop iteration 

CREATE TABLE #QRT_PREDICTION (PR_DONOR_ID NUMERIC(8,0) , PATTERN_NUMBER INTEGER , GAVE_FLG INTEGER )
--SELECT * FROM #QRT_PREDICTION
--DROP TABLE #QRT_PREDICTION
--==========================================================================================================
--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP
--==========================================================================================================
--==========================================================================================================
--==========================================================================================================

--INTERNAL LOOP 1:             
--Basic loop to populate the file of sponsors randomly selected as givers during the period

DECLARE @PATT INTEGER;        -- (1) declares the temporal variable count as an integer
SET @PATT = 1 ;               -- (2) set the initial value of the temporal value @PATT as 1
DECLARE @END INTEGER;         -- (3) SET THE VARIABLE @END THAT DEFINES THE TOTAL NUMBER OF ITERATIONS
SET @END = (SELECT MAX(Y.Pattern_Number) FROM #Patt65_CONSOL Y) 

--INTERNAL LOOP 2: SETTING THE LOOP CONDITION ==> ITERATIONS GO FROM 1 TO 64
WHILE (@PATT <=@END)         -- (3) WHILE defines the loop the code will be executed until @PATT  reaches the value 64 NO SEMICOLON!!!

--INTERNAL LOOP 3: BEGIN ===> END DEFINE THE SPACE FOR THE CODE THAT WILL RUN IN ITERATIONS      
BEGIN	                     -- (4) this defines where the code the loop applies to starts
							-- (5) this is the body of code 
--INTERNAL LOOP 4: 
--FIRST DECLARE AND DEFINE THE VARIABLE @NY THAT IS THE NUMBER OF SPONSORS THAT ARE PREDICTED WILL GIVE IN CATEGORY @PATT
DECLARE @NY INTEGER;
SET @NY = (SELECT Y.Y_GIVE FROM #Patt65_CONSOL Y WHERE Y.Pattern_Number = @PATT)

--INTERNAL LOOP 5: CREATING THE SCORES FOR A PARTICULAR PATTERN @NY AND STORE IT INTO #ONE_PATT
-- #ONE_PATT creates and stores the values for the current loop pattern -- this code stores the randomly selected @NY as GAVE_FLG = 1
select top (@NY) a.PR_DONOR_ID , a.Pattern_Number, 1 AS 'GAVE_FLG' into #ONE_PATT from #PATTERN_data a where a.Pattern_Number = @PATT  ORDER BY NEWID()

-- #ONE_PATT creates and stores the values for the current loop pattern -- this code stores the not randomly selected @NY as GAVE_FLG = 0
insert into #ONE_PATT
select b.PR_DONOR_ID , b.Pattern_Number, 0 AS 'GAVE_FLG' from #PATTERN_data b 
left join #ONE_PATT a on a.PR_DONOR_ID = b.PR_DONOR_ID  
where a.PR_DONOR_ID is null and b.pattern_number = @PATT

--INTERNAL LOOP 6: #ONE_PATT RESULT FROM PATTERN ITERATION STORED IN #QRT_PREDICTION ON A CUMULATIVE BASIS
--now the result of #ONE_PATT which is just for one pattern is stored into the more formal #QRT_Prediction that stores ALL pattern results
insert into #QRT_PREDICTION (PR_DONOR_ID, PATTERN_NUMBER, GAVE_FLG)
select A.PR_DONOR_ID , a.Pattern_Number, a.GAVE_FLG from #ONE_PATT a

--FINALLY THE #ONE_PATT ID DROPPED SO IT CAN BE RE-USED FOR THE NEXT PATTERN IN THE ITERATION 
drop table #ONE_PATT
	

PRINT @PATT;                  -- (5) print instruction ... can be way more complex
SET @PATT = @PATT+1;          -- (6) the @variable @PATT is set up as @PATT +1 in order to allow for the next iteration 
END;						  -- (7) this instruction tells the machine to end the loop 

--by the END all donors must have a GAVE_FLG as 1, 0 as they were randomly selected in their respective Pattern (1 to 64)
--END OF INTERNAL LOOP WHAT COMES NEXT IS PROCESSED AS PART OF THE EXTERNAL LOOP ONLY AS MANY TIMES AS PREDICTION QUSRTERS ARE DEFINED
--==============================================================================================================
--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP
--==============================================================================================================
--DECLARE @PQRT INTEGER;         -- (1) declares the temporal variable count as an integer 
--SET @PQRT = 1 ;     		   -- (2) set the initial value of the temporal value @PQRT as 1
--DECLARE @PEND INTEGER;         -- (3) declares the variable @PEND 
--SET @PEND = 1 ; 
--EXTERNAL LOOP 5: 
--INTRODUCE CODE TO SAVE THE gAVE/dID NOT GIVE PREDICTION FROM #QRT_PREDICTION INTO THE MASTER TABLE #MASTER_PRED_CONSOLIDATED

--THE VARIABLES @SQL2 AND @SQL3 ARE DECLARED NVARCHAR(MAX) MEANING TEXT OF THE CODE WITH EXTENSION AS REQUIRED
--@SQL1 WILL DEAL WITH THE ALTER TABLE CODE (ADD NEW COLUMN TO #INITIAL)
--@SQL2 WILL CORRESPOND TO THE UPDATE OF THE TABLE (ADD CONTENT OF #SEQ_data TO NEW COLUMN IN #INITIAL

    DECLARE @SQL2 NVARCHAR(MAX)   
	DECLARE @SQL3 NVARCHAR(MAX)   

-- First statement add the new row to THE #MASTER_PRED_CONSOLIDATED Table 
-- the name of the column is PRED_1 for the first iteration, PRED_2 for the second and so on ...  it will stop at PRED_i , 

    SET @SQL2 = 'ALTER TABLE #MASTER_PRED_CONSOLIDATED
        ADD ' + concat('PRED_',CAST(@PQRT AS VARCHAR(10))) + ' INTEGER'

-- Second Stement Write the result of the iteration from #SEQ_data into #INITIAL_data into the correct column PRED_i for iteration i

   SET @SQL3 = 'UPDATE #MASTER_PRED_CONSOLIDATED
        SET ' + concat('PRED_',CAST(@PQRT AS VARCHAR(10))) + ' = I.GAVE_FLG
		from 
		#QRT_PREDICTION I,
	    #MASTER_PRED_CONSOLIDATED H
        where
        I.PR_DONOR_ID = H.PR_DONOR_ID'

--THE EXECUTE() CODE WILL ALLOW THE TEXT IN @SQL2 AND @SQL3 TO BE INTERPRETED AS REAL CODE NOT JUST TEXT
EXECUTE (@SQL2)
EXECUTE (@SQL3)

--drop table #QRT_PREDICTION
--EXTERNAL LOOP 6: 
--INTRODUCE CODE TO SAVE THE PATTERN INFORMATION FROM #QRT_PREDICTION INTO THE MASTER_PATT TABLE #MASTER_PATT_CONSOLIDATED
--DECLARE @PQRT INTEGER;         -- (1) declares the temporal variable count as an integer 
--SET @PQRT = 1 ;     		   -- (2) set the initial value of the temporal value @PQRT as 1
--DECLARE @PEND INTEGER;         -- (3) declares the variable @PEND 
--SET @PEND = 1 ; 
--THE VARIABLES @SQL4 AND @SQL5 ARE DECLARED NVARCHAR(MAX) MEANING TEXT OF THE CODE WITH EXTENSION AS REQUIRED
--@SQL1 WILL DEAL WITH THE ALTER TABLE CODE (ADD NEW COLUMN TO #INITIAL)
--@SQL2 WILL CORRESPOND TO THE UPDATE OF THE TABLE (ADD CONTENT OF #SEQ_data TO NEW COLUMN IN #INITIAL

    DECLARE @SQL4 NVARCHAR(MAX)   
	DECLARE @SQL5 NVARCHAR(MAX)   

-- First statement add the new row to THE #MASTER_PRED_CONSOLIDATED Table 
-- the name of the column is PRED_1 for the first iteration, PRED_2 for the second and so on ...  it will stop at PRED_i , 

    SET @SQL4 = 'ALTER TABLE #MASTER_PATT_CONSOLIDATED
        ADD ' + concat('PATT_',CAST(@PQRT AS VARCHAR(10))) + ' INTEGER'

-- Second Stement Write the result of the iteration from #SEQ_data into #INITIAL_data into the correct column PRED_i for iteration i

   SET @SQL5 = 'UPDATE #MASTER_PATT_CONSOLIDATED
        SET ' + concat('PATT_',CAST(@PQRT AS VARCHAR(10))) + ' = I.PATTERN_NUMBER
		from 
		#QRT_PREDICTION I,
	    #MASTER_PATT_CONSOLIDATED H
        where
        I.PR_DONOR_ID = H.PR_DONOR_ID'

--THE EXECUTE() CODE WILL ALLOW THE TEXT IN @SQL4 AND @SQL5 TO BE INTERPRETED AS REAL CODE NOT JUST TEXT
EXECUTE (@SQL4)
EXECUTE (@SQL5)


--FINALLY THE #QRT_PREDICTION TABLE IS DROPPED SO IT CAN BE RE-USED FOR THE NEXT QUARTER (EXTERNAL LOOP ITERATION) PATTERN IN THE ITERATION 
drop table #QRT_PREDICTION
drop table #Patt65_CONSOL
drop table #PATTERN_data
--drop table #Patt65_CONSOL	

--PRINT @PQRT;                  -- (5) print instruction ... can be way more complex
SET @PQRT = @PQRT+1;          -- (6) the @variable @PATT is set up as @PATT +1 in order to allow for the next iteration 
END;						  -- (7) this instruction tells the machine to end the loop 


select * 
INTO [SPSS_Sandbox].[dbo].[LTDV_SP_FY21_SPP_INCOME_FLG_80QRT_1_DEMO] 
from #MASTER_PRED_CONSOLIDATED
ORDER BY 1
--(241278 rows affected)

select * 
INTO [SPSS_Sandbox].[dbo].[LTDV_SP_FY21_SPP_PATTERN_N_80Q_1RT]
from #MASTER_PATT_CONSOLIDATED
order by 1

--==============================================================================================================
--==============================================================================================================
--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP
--==============================================================================================================
drop table #MASTER_PRED_CONSOLIDATED
drop table #MASTER_PATT_CONSOLIDATED

--iteration 2
select * from [SPSS_Sandbox].[dbo].[LTDV_SP_FY21_SPP_INCOME_FLG_80QRT_1_DEMO] 
select * from [SPSS_Sandbox].[dbo].[LTDV_SP_FY21_SPP_PATTERN_N_80Q_1RT] order by 1

--==================================================================================
--=================================================================================================
--iteration 3
--=================================================================================================
--TABLE 2: HISTORICAL GIVING FLAG PER DONOR FOR last 5 quarters pattern plus prediction AND RELEVANT SPONSOR CATEGORY ... 
--===================================================================================================================================================
--Needs to define last 4 quarters data available ... 

select dn.Donation_Donor_Id as 'PR_DONOR_ID'
, a.DNR_CAT
, isnull(max(iif(cal.Tri_Fin_Ds_Lg = '2022 Q1' ,1,0)),0) as 'Q1'
, isnull(max(iif(cal.Tri_Fin_Ds_Lg = '2022 Q2' ,1,0)),0) as 'Q2'
, isnull(max(iif(cal.Tri_Fin_Ds_Lg = '2022 Q3' ,1,0)),0) as 'Q3'
, isnull(max(iif(cal.Tri_Fin_Ds_Lg = '2022 Q4' ,1,0)),0) as 'Q4'
into #MASTER_PRED_CONSOLIDATED
from ADOBE.RAW.F_DONATION dn
join [BI_DW].[dbo].[D_Cal] cal on cal.Dt = dn.Donation_Deposit_Date
join [SPSS_Sandbox].[dbo].[LTDV_SCRIPT_FY21_25072022_PAST_INCOME_TNR_DEMO] a on a.DONATION_DONOR_ID = dn.Donation_Donor_Id
where dn.Donation_Adjustment_Reason_Sid = 0
and dn.Donation_Income_Type_for_Donor_Metrics_Sid = 102
	   --SCOPE OF THE QUERY must be ONE FISCAL YEAR 
and dn.Donation_Deposit_Date between '2021-10-01' and '2022-09-30'
group by dn.Donation_Donor_Id, a.DNR_CAT
--(241278 rows affected)
--Completion time: 2022-11-07T11:54:09.3489538-05:00

select * from #MASTER_PRED_CONSOLIDATED
--drop table  #MASTER_PRED_CONSOLIDATED 

--===================================================================================================================================================
--TABLE 3: #MASTER_PATT_CONSOLIDATED -- NUMBER FROM 1 TO 64 THAT SUMMARIZES THE POSITION OF THE SPONSOR IN THE CATEGORY+Q1Q2Q3Q4 PATTERN PER QUARTER 
--THE INITIAL ROW ARE THE DONOR_IDS , THE COLUMNS WILL BE ADDED AS PART OF THE EXTERNAL LOOP  
--THIS GENERIC ROLE WILL BE PICKED AS PART OF THE EXTERNAL LOOP ... 
--===================================================================================================================================================

SELECT A.PR_DONOR_ID 
INTO #MASTER_PATT_CONSOLIDATED
from #MASTER_PRED_CONSOLIDATED A

--(241278 rows affected)
--Completion time: 2022-11-07T12:05:16.8516176-05:00
--DROP TABLE #MASTER_PATT_CONSOLIDATED
--(333616 rows affected)
--SELECT * FROM  #MASTER_PATT_CONSOLIDATED ORDER BY 1

--SELECT PATT_1 , COUNT( PR_DONOR_ID) AS 'CN' FROM  #MASTER_PATT_CONSOLIDATED GROUP BY PATT_1 ORDER BY 1

SELECT * FROM #MASTER_PRED_CONSOLIDATED

--===========================================================================================================================================
-- END OF OUT-OF-THE-LOOP-TABLES
--===========================================================================================================================================
--===========================================================================================================================================
-- START OF EXTERNAL LOOP
--===========================================================================================================================================
--EXTERNAL LOOP 1 - WE NEED TO CREATE THE CODE FOR SELECTING THE RELEVANT SEQUENCE OF 4 QUARTERS TO BE PICKED FOR BUILDING PATTERN PER SPONSOR 
--CAREFUL WITH THE CREATION OF TEMPORAL TABLES AS PART OF THE EXEC()!!!
--tHE Exec command makes temp procedure from THE @sql qUEry and executes it. 
--When that procedure ends, all temp tables created in it will be dropped immediately, so NO access to the created table from outside current dynamic query. 
--use global temp table ##table_name to keep it alive ... MAKE SURE TO DELETE IT AFTER THE PROCESURE IS FINISHED ...
--again  dynamically, cannot use #TEMPTABLE because a local temp table will only exist in the scope of the query that defines it. 
--Using ## creates a global temp table which will be accessible outside the scope of the dynamic query.
--EXTERNAL LOOP 2 - now the table ##PATT_Q1Q2Q3Q4 is processed to create the patterns (65 Pattern_QRT + SP Category) per donor
-- GENERATE PATTERNS FOR EACH SPONSOR BASED ON THE INFORMATION OF QUERY1 Q1,Q2,Q3,Q4 1,0 INDICATORS AND THE DNR_CATEGORY 
--64 COMBINATIONS OF PATTERN AND DONOR CATEGORY AND An OTHER category in case there were some issue with data: 65 GROUPS
--===============================
--EXTERNAL LOOP 3 - now the table #Patt65 is left joined with [SPSS_Sandbox].[dbo].[LTDV_FY21_PRIOR_PROB_VECTORS_SP_PL_SG_DEMO] to create the counts patterns 
--(65 Pattern_QRT + SP Category) per donor and the number of people expected to give per pattern
-- GENERATE THE CONSOLIDATED MATRIX WITH 64 ROWS ONE PATTERN IN EACH ONE AND JOINS WITH [SPSS_Sandbox].[dbo].[LTDV_FY21_PRIOR_PROB_VECTORS_SP_PL_SG_DEMO]
-- THAT HAS THE PRIOR PROBABILITY PER PATTERN TO GENERATE THE NUMBER OF CASES PREDICTED AS GIVING IN THE QUARTER.
-- ==> Input for P(E) Number of donor in Pattern i / Divided by N_Donors
--, sum (b.Gave_in_2020_Q2) as 'N_Donors_Gave' -- ==> Input for P(H) Number of donors that Gave / Divided by N_Donors
--, convert(decimal(5,4) ,sum (b.Gave_in_2020_Q2)* 1./ count (b.Donation_Donor_Id)) as 'Prob_Gave_NextQ' -- ==> BAYESIAN ESTIMATION OF P(H/E)
--into [SPSS_Sandbox].[dbo].[LTSV_FY19_20_Prior_Prob_19_20]

DECLARE @PQRT INTEGER;         -- (1) declares the temporal variable count as an integer 
SET @PQRT = 1 ;     		   -- (2) set the initial value of the temporal value @PQRT as 1
DECLARE @PEND INTEGER;         -- (3) declares the variable @PEND 
SET @PEND = (select max(EXITER_N) from  #EX_ITER_ORDER) ;                --(select max(EXITER_N) from  #EX_ITER_ORDER) ;

--EXTERNAL LOOP: DEFINITION OF THE CONDITION FOR WHILE CONDITION THE ITERATION ==>

WHILE (@PQRT <=@PEND)
BEGIN
PRINT @PQRT

 CREATE TABLE #PATTERN_data (PR_DONOR_ID NUMERIC(8,0) , Pattern_Number INTEGER)

DECLARE @Q1 VARCHAR(7)
DECLARE @Q2 VARCHAR(7)
DECLARE @Q3 VARCHAR(7)
DECLARE @Q4 VARCHAR(7)
SET @Q1 = (SELECT R.Q1 FROM #EX_ITER_ORDER R WHERE R.EXITER_N = @PQRT)
SET @Q2 = (SELECT R.Q2 FROM #EX_ITER_ORDER R WHERE R.EXITER_N = @PQRT)
SET @Q3 = (SELECT R.Q3 FROM #EX_ITER_ORDER R WHERE R.EXITER_N = @PQRT)
SET @Q4 = (SELECT R.Q4 FROM #EX_ITER_ORDER R WHERE R.EXITER_N = @PQRT)

PRINT @Q1
PRINT @Q2
PRINT @Q3
PRINT @Q4

--DECLARE @SQL1 NVARCHAR(MAX)
DECLARE @SQL1 NVARCHAR(MAX)
SET @SQL1 = 'SELECT PR_DONOR_ID, DNR_CAT, CAST(CONCAT(' + CAST(@Q1 AS VARCHAR(10)) +  ',' + CAST(@Q2 AS VARCHAR(10)) +  ',' + CAST(@Q3 AS VARCHAR(10)) +  ',' + CAST(@Q4 AS VARCHAR(10))
 + ') AS VARCHAR(4)) AS ''QRT_PATT'' INTO #PATT_Q1Q2Q3Q4 FROM #MASTER_PRED_CONSOLIDATED

INSERT INTO #PATTERN_data (PR_DONOR_ID , Pattern_Number)
SELECT PR_DONOR_ID
, CASE 
WHEN QRT_PATT = ''1111'' and DNR_CAT = ''NEW'' then 1
WHEN QRT_PATT = ''1111'' and DNR_CAT = ''UPGRADED'' then 2
WHEN QRT_PATT = ''1111'' and DNR_CAT = ''REACTIVATED'' then 3
WHEN QRT_PATT = ''1111'' and DNR_CAT = ''OLD'' then 4
WHEN QRT_PATT = ''0111'' and DNR_CAT = ''NEW'' then 5
WHEN QRT_PATT = ''0111'' and DNR_CAT = ''UPGRADED'' then 6
WHEN QRT_PATT = ''0111'' and DNR_CAT = ''REACTIVATED'' then 7
WHEN QRT_PATT = ''0111'' and DNR_CAT = ''OLD'' then 8
WHEN QRT_PATT = ''1011'' and DNR_CAT = ''NEW'' then 9
WHEN QRT_PATT = ''1011'' and DNR_CAT = ''UPGRADED'' then 10
WHEN QRT_PATT = ''1011'' and DNR_CAT = ''REACTIVATED'' then 11
WHEN QRT_PATT = ''1011'' and DNR_CAT = ''OLD'' then 12
WHEN QRT_PATT = ''1101'' and DNR_CAT = ''NEW'' then 13
WHEN QRT_PATT = ''1101'' and DNR_CAT = ''UPGRADED'' then 14
WHEN QRT_PATT = ''1101'' and DNR_CAT = ''REACTIVATED'' then 15
WHEN QRT_PATT = ''1101'' and DNR_CAT = ''OLD'' then 16
WHEN QRT_PATT = ''0011'' and DNR_CAT = ''NEW'' then 17
WHEN QRT_PATT = ''0011'' and DNR_CAT = ''UPGRADED'' then 18
WHEN QRT_PATT = ''0011'' and DNR_CAT = ''REACTIVATED'' then 19
WHEN QRT_PATT = ''0011'' and DNR_CAT = ''OLD'' then 20
WHEN QRT_PATT = ''0101'' and DNR_CAT = ''NEW'' then 21
WHEN QRT_PATT = ''0101'' and DNR_CAT = ''UPGRADED'' then 22
WHEN QRT_PATT = ''0101'' and DNR_CAT = ''REACTIVATED'' then 23
WHEN QRT_PATT = ''0101'' and DNR_CAT = ''OLD'' then 24
WHEN QRT_PATT = ''1001'' and DNR_CAT = ''NEW'' then 25
WHEN QRT_PATT = ''1001'' and DNR_CAT = ''UPGRADED'' then 26
WHEN QRT_PATT = ''1001'' and DNR_CAT = ''REACTIVATED'' then 27
WHEN QRT_PATT = ''1001'' and DNR_CAT = ''OLD'' then 28
WHEN QRT_PATT = ''0001'' and DNR_CAT = ''NEW'' then 29
WHEN QRT_PATT = ''0001'' and DNR_CAT = ''UPGRADED'' then 30
WHEN QRT_PATT = ''0001'' and DNR_CAT = ''REACTIVATED'' then 31
WHEN QRT_PATT = ''0001'' and DNR_CAT = ''OLD'' then 32
WHEN QRT_PATT = ''1110'' and DNR_CAT = ''NEW'' then 33
WHEN QRT_PATT = ''1110'' and DNR_CAT = ''UPGRADED'' then 34
WHEN QRT_PATT = ''1110'' and DNR_CAT = ''REACTIVATED'' then 35
WHEN QRT_PATT = ''1110'' and DNR_CAT = ''OLD'' then 36
WHEN QRT_PATT = ''0110'' and DNR_CAT = ''NEW'' then 37
WHEN QRT_PATT = ''0110'' and DNR_CAT = ''UPGRADED'' then 38
WHEN QRT_PATT = ''0110'' and DNR_CAT = ''REACTIVATED'' then 39
WHEN QRT_PATT = ''0110'' and DNR_CAT = ''OLD'' then 40
WHEN QRT_PATT = ''1010'' and DNR_CAT = ''NEW'' then 41
WHEN QRT_PATT = ''1010'' and DNR_CAT = ''UPGRADED'' then 42
WHEN QRT_PATT = ''1010'' and DNR_CAT = ''REACTIVATED'' then 43
WHEN QRT_PATT = ''1010'' and DNR_CAT = ''OLD'' then 44
WHEN QRT_PATT = ''1100'' and DNR_CAT = ''NEW'' then 45
WHEN QRT_PATT = ''1100'' and DNR_CAT = ''UPGRADED'' then 46
WHEN QRT_PATT = ''1100'' and DNR_CAT = ''REACTIVATED'' then 47
WHEN QRT_PATT = ''1100'' and DNR_CAT = ''OLD'' then 48
WHEN QRT_PATT = ''0010'' and DNR_CAT = ''NEW'' then 49
WHEN QRT_PATT = ''0010'' and DNR_CAT = ''UPGRADED'' then 50
WHEN QRT_PATT = ''0010'' and DNR_CAT = ''REACTIVATED'' then 51
WHEN QRT_PATT = ''0010'' and DNR_CAT = ''OLD'' then 52
WHEN QRT_PATT = ''0100'' and DNR_CAT = ''NEW'' then 53
WHEN QRT_PATT = ''0100'' and DNR_CAT = ''UPGRADED'' then 54
WHEN QRT_PATT = ''0100'' and DNR_CAT = ''REACTIVATED'' then 55
WHEN QRT_PATT = ''0100'' and DNR_CAT = ''OLD'' then 56
WHEN QRT_PATT = ''1000'' and DNR_CAT = ''NEW'' then 57
WHEN QRT_PATT = ''1000'' and DNR_CAT = ''UPGRADED'' then 58
WHEN QRT_PATT = ''1000'' and DNR_CAT = ''REACTIVATED'' then 59
WHEN QRT_PATT = ''1000'' and DNR_CAT = ''OLD'' then 60
WHEN QRT_PATT = ''0000'' and DNR_CAT = ''NEW''  then 61
WHEN QRT_PATT = ''0000'' and DNR_CAT = ''UPGRADED'' then 62
WHEN QRT_PATT = ''0000'' and DNR_CAT = ''REACTIVATED'' then 63
WHEN QRT_PATT = ''0000'' and DNR_CAT = ''OLD'' then 64
else 65 end as ''Pattern_Number''
FROM #PATT_Q1Q2Q3Q4'

EXECUTE (@SQL1)

--SELECT  * FROM #PATTERN_data
--select * from [SPSS_Sandbox].[dbo].[LTDV_FY21_PRIOR_PROB_VECTORS_SP_PL_SG_DEMO]

select a.Pattern_Number
, count (b.PR_DONOR_ID) as 'N_Donors'
, a.Prob_SPR_Gave_SPP_NextQ 
, round(a.Prob_SPR_Gave_SPP_NextQ*count (b.PR_DONOR_ID),0) as 'Y_Give'
, count (b.PR_DONOR_ID) - round(a.Prob_SPR_Gave_SPP_NextQ*count (b.PR_DONOR_ID),0) as 'N_Give' ----> calculation just uses relevant SP Prob vector 
into #Patt65_CONSOL
FROM [SPSS_Sandbox].[dbo].[LTDV_FY21_PRIOR_PROB_VECTORS_SP_PL_SG_DEMO] a   --==> this table has the information on patterns 
left join  #PATTERN_data b on a.Pattern_Number = b.Pattern_Number
group by a.Pattern_Number , a.Prob_SPR_Gave_SPP_NextQ
order by 1

--SELECT * FROM #Patt65_CONSOL ORDER BY 1

--NOW IT IS THE TIME TO INSERT THE INTERNAL LOOP THAT WILL PROCESS EVERYONE OF THE 64 GROUPS IN ORDER TO CREATE A FLAG give/didNOT give PER DONOR PER QUARTER
--THE INTERNAL LOOP RUNS 64 TIMES PER QUARTER.
---BUT BEFORE THAT

--EXTERNAL LOOP 4: table #QRT_PREDICTION THAT STORES THE PREDICTION FOR ALL DONORS FOR THE QUARTER
--The table #QRT_PREDICTION will store all the quarter results coming from the internal loop 
--Must be created OUT of both the external and internal loop since the table is created just once
--and must BE just populated at the end of every external loop iteration 

CREATE TABLE #QRT_PREDICTION (PR_DONOR_ID NUMERIC(8,0) , PATTERN_NUMBER INTEGER , GAVE_FLG INTEGER )
--SELECT * FROM #QRT_PREDICTION
--DROP TABLE #QRT_PREDICTION
--==========================================================================================================
--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP
--==========================================================================================================
--==========================================================================================================
--==========================================================================================================

--INTERNAL LOOP 1:             
--Basic loop to populate the file of sponsors randomly selected as givers during the period

DECLARE @PATT INTEGER;        -- (1) declares the temporal variable count as an integer
SET @PATT = 1 ;               -- (2) set the initial value of the temporal value @PATT as 1
DECLARE @END INTEGER;         -- (3) SET THE VARIABLE @END THAT DEFINES THE TOTAL NUMBER OF ITERATIONS
SET @END = (SELECT MAX(Y.Pattern_Number) FROM #Patt65_CONSOL Y) 

--INTERNAL LOOP 2: SETTING THE LOOP CONDITION ==> ITERATIONS GO FROM 1 TO 64
WHILE (@PATT <=@END)         -- (3) WHILE defines the loop the code will be executed until @PATT  reaches the value 64 NO SEMICOLON!!!

--INTERNAL LOOP 3: BEGIN ===> END DEFINE THE SPACE FOR THE CODE THAT WILL RUN IN ITERATIONS      
BEGIN	                     -- (4) this defines where the code the loop applies to starts
							-- (5) this is the body of code 
--INTERNAL LOOP 4: 
--FIRST DECLARE AND DEFINE THE VARIABLE @NY THAT IS THE NUMBER OF SPONSORS THAT ARE PREDICTED WILL GIVE IN CATEGORY @PATT
DECLARE @NY INTEGER;
SET @NY = (SELECT Y.Y_GIVE FROM #Patt65_CONSOL Y WHERE Y.Pattern_Number = @PATT)

--INTERNAL LOOP 5: CREATING THE SCORES FOR A PARTICULAR PATTERN @NY AND STORE IT INTO #ONE_PATT
-- #ONE_PATT creates and stores the values for the current loop pattern -- this code stores the randomly selected @NY as GAVE_FLG = 1
select top (@NY) a.PR_DONOR_ID , a.Pattern_Number, 1 AS 'GAVE_FLG' into #ONE_PATT from #PATTERN_data a where a.Pattern_Number = @PATT  ORDER BY NEWID()

-- #ONE_PATT creates and stores the values for the current loop pattern -- this code stores the not randomly selected @NY as GAVE_FLG = 0
insert into #ONE_PATT
select b.PR_DONOR_ID , b.Pattern_Number, 0 AS 'GAVE_FLG' from #PATTERN_data b 
left join #ONE_PATT a on a.PR_DONOR_ID = b.PR_DONOR_ID  
where a.PR_DONOR_ID is null and b.pattern_number = @PATT

--INTERNAL LOOP 6: #ONE_PATT RESULT FROM PATTERN ITERATION STORED IN #QRT_PREDICTION ON A CUMULATIVE BASIS
--now the result of #ONE_PATT which is just for one pattern is stored into the more formal #QRT_Prediction that stores ALL pattern results
insert into #QRT_PREDICTION (PR_DONOR_ID, PATTERN_NUMBER, GAVE_FLG)
select A.PR_DONOR_ID , a.Pattern_Number, a.GAVE_FLG from #ONE_PATT a

--FINALLY THE #ONE_PATT ID DROPPED SO IT CAN BE RE-USED FOR THE NEXT PATTERN IN THE ITERATION 
drop table #ONE_PATT
	

PRINT @PATT;                  -- (5) print instruction ... can be way more complex
SET @PATT = @PATT+1;          -- (6) the @variable @PATT is set up as @PATT +1 in order to allow for the next iteration 
END;						  -- (7) this instruction tells the machine to end the loop 

--by the END all donors must have a GAVE_FLG as 1, 0 as they were randomly selected in their respective Pattern (1 to 64)
--END OF INTERNAL LOOP WHAT COMES NEXT IS PROCESSED AS PART OF THE EXTERNAL LOOP ONLY AS MANY TIMES AS PREDICTION QUSRTERS ARE DEFINED
--==============================================================================================================
--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP
--==============================================================================================================
--DECLARE @PQRT INTEGER;         -- (1) declares the temporal variable count as an integer 
--SET @PQRT = 1 ;     		   -- (2) set the initial value of the temporal value @PQRT as 1
--DECLARE @PEND INTEGER;         -- (3) declares the variable @PEND 
--SET @PEND = 1 ; 
--EXTERNAL LOOP 5: 
--INTRODUCE CODE TO SAVE THE gAVE/dID NOT GIVE PREDICTION FROM #QRT_PREDICTION INTO THE MASTER TABLE #MASTER_PRED_CONSOLIDATED

--THE VARIABLES @SQL2 AND @SQL3 ARE DECLARED NVARCHAR(MAX) MEANING TEXT OF THE CODE WITH EXTENSION AS REQUIRED
--@SQL1 WILL DEAL WITH THE ALTER TABLE CODE (ADD NEW COLUMN TO #INITIAL)
--@SQL2 WILL CORRESPOND TO THE UPDATE OF THE TABLE (ADD CONTENT OF #SEQ_data TO NEW COLUMN IN #INITIAL

    DECLARE @SQL2 NVARCHAR(MAX)   
	DECLARE @SQL3 NVARCHAR(MAX)   

-- First statement add the new row to THE #MASTER_PRED_CONSOLIDATED Table 
-- the name of the column is PRED_1 for the first iteration, PRED_2 for the second and so on ...  it will stop at PRED_i , 

    SET @SQL2 = 'ALTER TABLE #MASTER_PRED_CONSOLIDATED
        ADD ' + concat('PRED_',CAST(@PQRT AS VARCHAR(10))) + ' INTEGER'

-- Second Stement Write the result of the iteration from #SEQ_data into #INITIAL_data into the correct column PRED_i for iteration i

   SET @SQL3 = 'UPDATE #MASTER_PRED_CONSOLIDATED
        SET ' + concat('PRED_',CAST(@PQRT AS VARCHAR(10))) + ' = I.GAVE_FLG
		from 
		#QRT_PREDICTION I,
	    #MASTER_PRED_CONSOLIDATED H
        where
        I.PR_DONOR_ID = H.PR_DONOR_ID'

--THE EXECUTE() CODE WILL ALLOW THE TEXT IN @SQL2 AND @SQL3 TO BE INTERPRETED AS REAL CODE NOT JUST TEXT
EXECUTE (@SQL2)
EXECUTE (@SQL3)

--drop table #QRT_PREDICTION
--EXTERNAL LOOP 6: 
--INTRODUCE CODE TO SAVE THE PATTERN INFORMATION FROM #QRT_PREDICTION INTO THE MASTER_PATT TABLE #MASTER_PATT_CONSOLIDATED
--DECLARE @PQRT INTEGER;         -- (1) declares the temporal variable count as an integer 
--SET @PQRT = 1 ;     		   -- (2) set the initial value of the temporal value @PQRT as 1
--DECLARE @PEND INTEGER;         -- (3) declares the variable @PEND 
--SET @PEND = 1 ; 
--THE VARIABLES @SQL4 AND @SQL5 ARE DECLARED NVARCHAR(MAX) MEANING TEXT OF THE CODE WITH EXTENSION AS REQUIRED
--@SQL1 WILL DEAL WITH THE ALTER TABLE CODE (ADD NEW COLUMN TO #INITIAL)
--@SQL2 WILL CORRESPOND TO THE UPDATE OF THE TABLE (ADD CONTENT OF #SEQ_data TO NEW COLUMN IN #INITIAL

    DECLARE @SQL4 NVARCHAR(MAX)   
	DECLARE @SQL5 NVARCHAR(MAX)   

-- First statement add the new row to THE #MASTER_PRED_CONSOLIDATED Table 
-- the name of the column is PRED_1 for the first iteration, PRED_2 for the second and so on ...  it will stop at PRED_i , 

    SET @SQL4 = 'ALTER TABLE #MASTER_PATT_CONSOLIDATED
        ADD ' + concat('PATT_',CAST(@PQRT AS VARCHAR(10))) + ' INTEGER'

-- Second Stement Write the result of the iteration from #SEQ_data into #INITIAL_data into the correct column PRED_i for iteration i

   SET @SQL5 = 'UPDATE #MASTER_PATT_CONSOLIDATED
        SET ' + concat('PATT_',CAST(@PQRT AS VARCHAR(10))) + ' = I.PATTERN_NUMBER
		from 
		#QRT_PREDICTION I,
	    #MASTER_PATT_CONSOLIDATED H
        where
        I.PR_DONOR_ID = H.PR_DONOR_ID'

--THE EXECUTE() CODE WILL ALLOW THE TEXT IN @SQL4 AND @SQL5 TO BE INTERPRETED AS REAL CODE NOT JUST TEXT
EXECUTE (@SQL4)
EXECUTE (@SQL5)


--FINALLY THE #QRT_PREDICTION TABLE IS DROPPED SO IT CAN BE RE-USED FOR THE NEXT QUARTER (EXTERNAL LOOP ITERATION) PATTERN IN THE ITERATION 
drop table #QRT_PREDICTION
drop table #Patt65_CONSOL
drop table #PATTERN_data
--drop table #Patt65_CONSOL	

--PRINT @PQRT;                  -- (5) print instruction ... can be way more complex
SET @PQRT = @PQRT+1;          -- (6) the @variable @PATT is set up as @PATT +1 in order to allow for the next iteration 
END;						  -- (7) this instruction tells the machine to end the loop 

--iteration 3
select * 
INTO [SPSS_Sandbox].[dbo].[LTDV_SP_FY21_SPP_INCOME_FLG_80QRT_2_DEMO] 
from #MASTER_PRED_CONSOLIDATED
ORDER BY 1
--(241278 rows affected)
select * 
INTO [SPSS_Sandbox].[dbo].[LTDV_SP_FY21_SPP_PATTERN_N_80Q_2RT]
from #MASTER_PATT_CONSOLIDATED
order by 1

--==============================================================================================================
--==============================================================================================================
--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP
--==============================================================================================================
drop table #MASTER_PRED_CONSOLIDATED
drop table #MASTER_PATT_CONSOLIDATED

--iteration 3
select * from [SPSS_Sandbox].[dbo].[LTDV_SP_FY21_SPP_INCOME_FLG_80QRT_2_DEMO] 
select * from [SPSS_Sandbox].[dbo].[LTDV_SP_FY21_SPP_PATTERN_N_80Q_2RT] order by 1

--=====================================================================================================
-- SPONSORS AS PLEDGERS
--==================================================================================================================================================
--NOT PART OF THE LOOP 
--===================================================================================================================================================
--TABLE 2: HISTORICAL GIVING FLAG PER DONOR FOR last 5 quarters pattern plus prediction AND RELEVANT SPONSOR CATEGORY ... 
--===================================================================================================================================================
--Needs to define last 4 quarters data available ... 

select dn.Donation_Donor_Id as 'PR_DONOR_ID'
, a.DNR_CAT
, isnull(max(iif(cal.Tri_Fin_Ds_Lg = '2022 Q1' ,1,0)),0) as 'Q1'
, isnull(max(iif(cal.Tri_Fin_Ds_Lg = '2022 Q2' ,1,0)),0) as 'Q2'
, isnull(max(iif(cal.Tri_Fin_Ds_Lg = '2022 Q3' ,1,0)),0) as 'Q3'
, isnull(max(iif(cal.Tri_Fin_Ds_Lg = '2022 Q4' ,1,0)),0) as 'Q4'
into #MASTER_PRED_CONSOLIDATED
from ADOBE.RAW.F_DONATION dn
join [BI_DW].[dbo].[D_Cal] cal on cal.Dt = dn.Donation_Deposit_Date
join [SPSS_Sandbox].[dbo].[LTDV_SCRIPT_FY21_25072022_PAST_INCOME_TNR_DEMO] a on a.DONATION_DONOR_ID = dn.Donation_Donor_Id
where dn.Donation_Adjustment_Reason_Sid = 0
and dn.Donation_Income_Type_for_Donor_Metrics_Sid = 101
	   --SCOPE OF THE QUERY must be ONE FISCAL YEAR 
and dn.Donation_Deposit_Date between '2021-10-01' and '2022-09-30'
AND a.DNR_LABEL = 'SPONSOR'
group by dn.Donation_Donor_Id, a.DNR_CAT
--(7023 rows affected)
--Completion time: 2022-11-13T23:51:22.4101588-05:00


select * from #MASTER_PRED_CONSOLIDATED
--drop table  #MASTER_PRED_CONSOLIDATED 

--===================================================================================================================================================
--TABLE 1: #EX_ITER_ORDER -- SEQUENCE OF 4 QUARTERS TO BE PICKED FOR BUILDING THE RELEVANT 4 QUARTERS PATTERN PER SPONSOR 
--THE ROWS CORRESPOND TO THE PREDICTION NUMBER, THE COLUMNS TO THE GENERIC ROLE Q1, Q2,Q3,Q4 
--THIS GENERIC ROLE WILL BE PICKED AS PART OF THE EXTERNAL LOOP ... 
--===================================================================================================================================================
-- TABLE ALREADY CREATED : SEE CODE IN LINES 1176 1267

--===================================================================================================================================================
--TABLE 3: #MASTER_PATT_CONSOLIDATED -- NUMBER FROM 1 TO 64 THAT SUMMARIZES THE POSITION OF THE SPONSOR IN THE CATEGORY+Q1Q2Q3Q4 PATTERN PER QUARTER 
--THE INITIAL ROW ARE THE DONOR_IDS , THE COLUMNS WILL BE ADDED AS PART OF THE EXTERNAL LOOP  
--THIS GENERIC ROLE WILL BE PICKED AS PART OF THE EXTERNAL LOOP ... 
--===================================================================================================================================================

SELECT A.PR_DONOR_ID 
INTO #MASTER_PATT_CONSOLIDATED
from #MASTER_PRED_CONSOLIDATED A

--(19846 rows affected)
--Completion time: 2022-11-13T23:23:16.7657979-05:00
--DROP TABLE #MASTER_PATT_CONSOLIDATED
--(333616 rows affected)
--SELECT * FROM  #MASTER_PATT_CONSOLIDATED ORDER BY 1

--SELECT PATT_1 , COUNT( PR_DONOR_ID) AS 'CN' FROM  #MASTER_PATT_CONSOLIDATED GROUP BY PATT_1 ORDER BY 1

SELECT * FROM #MASTER_PRED_CONSOLIDATED

--===========================================================================================================================================
-- END OF OUT-OF-THE-LOOP-TABLES
--===========================================================================================================================================

--===========================================================================================================================================
-- START OF EXTERNAL LOOP
--===========================================================================================================================================
--EXTERNAL LOOP 1 - WE NEED TO CREATE THE CODE FOR SELECTING THE RELEVANT SEQUENCE OF 4 QUARTERS TO BE PICKED FOR BUILDING PATTERN PER SPONSOR 
--CAREFUL WITH THE CREATION OF TEMPORAL TABLES AS PART OF THE EXEC()!!!
--tHE Exec command makes temp procedure from THE @sql qUEry and executes it. 
--When that procedure ends, all temp tables created in it will be dropped immediately, so NO access to the created table from outside current dynamic query. 
--use global temp table ##table_name to keep it alive ... MAKE SURE TO DELETE IT AFTER THE PROCESURE IS FINISHED ...
--again  dynamically, cannot use #TEMPTABLE because a local temp table will only exist in the scope of the query that defines it. 
--Using ## creates a global temp table which will be accessible outside the scope of the dynamic query.
--EXTERNAL LOOP 2 - now the table ##PATT_Q1Q2Q3Q4 is processed to create the patterns (65 Pattern_QRT + SP Category) per donor
-- GENERATE PATTERNS FOR EACH SPONSOR BASED ON THE INFORMATION OF QUERY1 Q1,Q2,Q3,Q4 1,0 INDICATORS AND THE DNR_CATEGORY 
--64 COMBINATIONS OF PATTERN AND DONOR CATEGORY AND An OTHER category in case there were some issue with data: 65 GROUPS
--===============================
--EXTERNAL LOOP 3 - now the table #Patt65 is left joined with [SPSS_Sandbox].[dbo].[LTDV_FY21_PRIOR_PROB_VECTORS_SP_PL_SG_DEMO] to create the counts patterns 
--(65 Pattern_QRT + SP Category) per donor and the number of people expected to give per pattern
-- GENERATE THE CONSOLIDATED MATRIX WITH 64 ROWS ONE PATTERN IN EACH ONE AND JOINS WITH [SPSS_Sandbox].[dbo].[LTDV_FY21_PRIOR_PROB_VECTORS_SP_PL_SG_DEMO]
-- THAT HAS THE PRIOR PROBABILITY PER PATTERN TO GENERATE THE NUMBER OF CASES PREDICTED AS GIVING IN THE QUARTER.
-- ==> Input for P(E) Number of donor in Pattern i / Divided by N_Donors
--, sum (b.Gave_in_2020_Q2) as 'N_Donors_Gave' -- ==> Input for P(H) Number of donors that Gave / Divided by N_Donors
--, convert(decimal(5,4) ,sum (b.Gave_in_2020_Q2)* 1./ count (b.Donation_Donor_Id)) as 'Prob_Gave_NextQ' -- ==> BAYESIAN ESTIMATION OF P(H/E)
--into [SPSS_Sandbox].[dbo].[LTSV_FY19_20_Prior_Prob_19_20]

DECLARE @PQRT INTEGER;         -- (1) declares the temporal variable count as an integer 
SET @PQRT = 1 ;     		   -- (2) set the initial value of the temporal value @PQRT as 1
DECLARE @PEND INTEGER;         -- (3) declares the variable @PEND 
SET @PEND = (select max(EXITER_N) from  #EX_ITER_ORDER) ;                --(select max(EXITER_N) from  #EX_ITER_ORDER) ;

--EXTERNAL LOOP: DEFINITION OF THE CONDITION FOR WHILE CONDITION THE ITERATION ==>

WHILE (@PQRT <=@PEND)
BEGIN
PRINT @PQRT

 CREATE TABLE #PATTERN_data (PR_DONOR_ID NUMERIC(8,0) , Pattern_Number INTEGER)

DECLARE @Q1 VARCHAR(7)
DECLARE @Q2 VARCHAR(7)
DECLARE @Q3 VARCHAR(7)
DECLARE @Q4 VARCHAR(7)
SET @Q1 = (SELECT R.Q1 FROM #EX_ITER_ORDER R WHERE R.EXITER_N = @PQRT)
SET @Q2 = (SELECT R.Q2 FROM #EX_ITER_ORDER R WHERE R.EXITER_N = @PQRT)
SET @Q3 = (SELECT R.Q3 FROM #EX_ITER_ORDER R WHERE R.EXITER_N = @PQRT)
SET @Q4 = (SELECT R.Q4 FROM #EX_ITER_ORDER R WHERE R.EXITER_N = @PQRT)

PRINT @Q1
PRINT @Q2
PRINT @Q3
PRINT @Q4

--DECLARE @SQL1 NVARCHAR(MAX)
DECLARE @SQL1 NVARCHAR(MAX)
SET @SQL1 = 'SELECT PR_DONOR_ID, DNR_CAT, CAST(CONCAT(' + CAST(@Q1 AS VARCHAR(10)) +  ',' + CAST(@Q2 AS VARCHAR(10)) +  ',' + CAST(@Q3 AS VARCHAR(10)) +  ',' + CAST(@Q4 AS VARCHAR(10))
 + ') AS VARCHAR(4)) AS ''QRT_PATT'' INTO #PATT_Q1Q2Q3Q4 FROM #MASTER_PRED_CONSOLIDATED

INSERT INTO #PATTERN_data (PR_DONOR_ID , Pattern_Number)
SELECT PR_DONOR_ID
, CASE 
WHEN QRT_PATT = ''1111'' and DNR_CAT = ''NEW'' then 1
WHEN QRT_PATT = ''1111'' and DNR_CAT = ''UPGRADED'' then 2
WHEN QRT_PATT = ''1111'' and DNR_CAT = ''REACTIVATED'' then 3
WHEN QRT_PATT = ''1111'' and DNR_CAT = ''OLD'' then 4
WHEN QRT_PATT = ''0111'' and DNR_CAT = ''NEW'' then 5
WHEN QRT_PATT = ''0111'' and DNR_CAT = ''UPGRADED'' then 6
WHEN QRT_PATT = ''0111'' and DNR_CAT = ''REACTIVATED'' then 7
WHEN QRT_PATT = ''0111'' and DNR_CAT = ''OLD'' then 8
WHEN QRT_PATT = ''1011'' and DNR_CAT = ''NEW'' then 9
WHEN QRT_PATT = ''1011'' and DNR_CAT = ''UPGRADED'' then 10
WHEN QRT_PATT = ''1011'' and DNR_CAT = ''REACTIVATED'' then 11
WHEN QRT_PATT = ''1011'' and DNR_CAT = ''OLD'' then 12
WHEN QRT_PATT = ''1101'' and DNR_CAT = ''NEW'' then 13
WHEN QRT_PATT = ''1101'' and DNR_CAT = ''UPGRADED'' then 14
WHEN QRT_PATT = ''1101'' and DNR_CAT = ''REACTIVATED'' then 15
WHEN QRT_PATT = ''1101'' and DNR_CAT = ''OLD'' then 16
WHEN QRT_PATT = ''0011'' and DNR_CAT = ''NEW'' then 17
WHEN QRT_PATT = ''0011'' and DNR_CAT = ''UPGRADED'' then 18
WHEN QRT_PATT = ''0011'' and DNR_CAT = ''REACTIVATED'' then 19
WHEN QRT_PATT = ''0011'' and DNR_CAT = ''OLD'' then 20
WHEN QRT_PATT = ''0101'' and DNR_CAT = ''NEW'' then 21
WHEN QRT_PATT = ''0101'' and DNR_CAT = ''UPGRADED'' then 22
WHEN QRT_PATT = ''0101'' and DNR_CAT = ''REACTIVATED'' then 23
WHEN QRT_PATT = ''0101'' and DNR_CAT = ''OLD'' then 24
WHEN QRT_PATT = ''1001'' and DNR_CAT = ''NEW'' then 25
WHEN QRT_PATT = ''1001'' and DNR_CAT = ''UPGRADED'' then 26
WHEN QRT_PATT = ''1001'' and DNR_CAT = ''REACTIVATED'' then 27
WHEN QRT_PATT = ''1001'' and DNR_CAT = ''OLD'' then 28
WHEN QRT_PATT = ''0001'' and DNR_CAT = ''NEW'' then 29
WHEN QRT_PATT = ''0001'' and DNR_CAT = ''UPGRADED'' then 30
WHEN QRT_PATT = ''0001'' and DNR_CAT = ''REACTIVATED'' then 31
WHEN QRT_PATT = ''0001'' and DNR_CAT = ''OLD'' then 32
WHEN QRT_PATT = ''1110'' and DNR_CAT = ''NEW'' then 33
WHEN QRT_PATT = ''1110'' and DNR_CAT = ''UPGRADED'' then 34
WHEN QRT_PATT = ''1110'' and DNR_CAT = ''REACTIVATED'' then 35
WHEN QRT_PATT = ''1110'' and DNR_CAT = ''OLD'' then 36
WHEN QRT_PATT = ''0110'' and DNR_CAT = ''NEW'' then 37
WHEN QRT_PATT = ''0110'' and DNR_CAT = ''UPGRADED'' then 38
WHEN QRT_PATT = ''0110'' and DNR_CAT = ''REACTIVATED'' then 39
WHEN QRT_PATT = ''0110'' and DNR_CAT = ''OLD'' then 40
WHEN QRT_PATT = ''1010'' and DNR_CAT = ''NEW'' then 41
WHEN QRT_PATT = ''1010'' and DNR_CAT = ''UPGRADED'' then 42
WHEN QRT_PATT = ''1010'' and DNR_CAT = ''REACTIVATED'' then 43
WHEN QRT_PATT = ''1010'' and DNR_CAT = ''OLD'' then 44
WHEN QRT_PATT = ''1100'' and DNR_CAT = ''NEW'' then 45
WHEN QRT_PATT = ''1100'' and DNR_CAT = ''UPGRADED'' then 46
WHEN QRT_PATT = ''1100'' and DNR_CAT = ''REACTIVATED'' then 47
WHEN QRT_PATT = ''1100'' and DNR_CAT = ''OLD'' then 48
WHEN QRT_PATT = ''0010'' and DNR_CAT = ''NEW'' then 49
WHEN QRT_PATT = ''0010'' and DNR_CAT = ''UPGRADED'' then 50
WHEN QRT_PATT = ''0010'' and DNR_CAT = ''REACTIVATED'' then 51
WHEN QRT_PATT = ''0010'' and DNR_CAT = ''OLD'' then 52
WHEN QRT_PATT = ''0100'' and DNR_CAT = ''NEW'' then 53
WHEN QRT_PATT = ''0100'' and DNR_CAT = ''UPGRADED'' then 54
WHEN QRT_PATT = ''0100'' and DNR_CAT = ''REACTIVATED'' then 55
WHEN QRT_PATT = ''0100'' and DNR_CAT = ''OLD'' then 56
WHEN QRT_PATT = ''1000'' and DNR_CAT = ''NEW'' then 57
WHEN QRT_PATT = ''1000'' and DNR_CAT = ''UPGRADED'' then 58
WHEN QRT_PATT = ''1000'' and DNR_CAT = ''REACTIVATED'' then 59
WHEN QRT_PATT = ''1000'' and DNR_CAT = ''OLD'' then 60
WHEN QRT_PATT = ''0000'' and DNR_CAT = ''NEW''  then 61
WHEN QRT_PATT = ''0000'' and DNR_CAT = ''UPGRADED'' then 62
WHEN QRT_PATT = ''0000'' and DNR_CAT = ''REACTIVATED'' then 63
WHEN QRT_PATT = ''0000'' and DNR_CAT = ''OLD'' then 64
else 65 end as ''Pattern_Number''
FROM #PATT_Q1Q2Q3Q4'

EXECUTE (@SQL1)

--SELECT  * FROM #PATTERN_data
--select TOP 10 * from [SPSS_Sandbox].[dbo].[LTDV_FY21_PRIOR_PROB_VECTORS_SP_PL_SG_DEMO]

select a.Pattern_Number
, count (b.PR_DONOR_ID) as 'N_Donors'
, a.Prob_SPR_Gave_PL_NextQ 
, round(a.Prob_SPR_Gave_PL_NextQ*count (b.PR_DONOR_ID),0) as 'Y_Give'
, count (b.PR_DONOR_ID) - round(a.Prob_SPR_Gave_PL_NextQ*count (b.PR_DONOR_ID),0) as 'N_Give' ----> calculation just uses relevant SP Prob vector 
into #Patt65_CONSOL
FROM [SPSS_Sandbox].[dbo].[LTDV_FY21_PRIOR_PROB_VECTORS_SP_PL_SG_DEMO] a   --==> this table has the information on patterns 
left join  #PATTERN_data b on a.Pattern_Number = b.Pattern_Number
group by a.Pattern_Number , a.Prob_SPR_Gave_PL_NextQ
order by 1

--SELECT * FROM #Patt65_CONSOL ORDER BY 1

--NOW IT IS THE TIME TO INSERT THE INTERNAL LOOP THAT WILL PROCESS EVERYONE OF THE 64 GROUPS IN ORDER TO CREATE A FLAG give/didNOT give PER DONOR PER QUARTER
--THE INTERNAL LOOP RUNS 64 TIMES PER QUARTER.
---BUT BEFORE THAT

--EXTERNAL LOOP 4: table #QRT_PREDICTION THAT STORES THE PREDICTION FOR ALL DONORS FOR THE QUARTER
--The table #QRT_PREDICTION will store all the quarter results coming from the internal loop 
--Must be created OUT of both the external and internal loop since the table is created just once
--and must BE just populated at the end of every external loop iteration 

CREATE TABLE #QRT_PREDICTION (PR_DONOR_ID NUMERIC(8,0) , PATTERN_NUMBER INTEGER , GAVE_FLG INTEGER )
--SELECT * FROM #QRT_PREDICTION
--DROP TABLE #QRT_PREDICTION
--==========================================================================================================
--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP
--==========================================================================================================
--==========================================================================================================
--==========================================================================================================

--INTERNAL LOOP 1:             
--Basic loop to populate the file of sponsors randomly selected as givers during the period

DECLARE @PATT INTEGER;        -- (1) declares the temporal variable count as an integer
SET @PATT = 1 ;               -- (2) set the initial value of the temporal value @PATT as 1
DECLARE @END INTEGER;         -- (3) SET THE VARIABLE @END THAT DEFINES THE TOTAL NUMBER OF ITERATIONS
SET @END = (SELECT MAX(Y.Pattern_Number) FROM #Patt65_CONSOL Y) 

--INTERNAL LOOP 2: SETTING THE LOOP CONDITION ==> ITERATIONS GO FROM 1 TO 64
WHILE (@PATT <=@END)         -- (3) WHILE defines the loop the code will be executed until @PATT  reaches the value 64 NO SEMICOLON!!!

--INTERNAL LOOP 3: BEGIN ===> END DEFINE THE SPACE FOR THE CODE THAT WILL RUN IN ITERATIONS      
BEGIN	                     -- (4) this defines where the code the loop applies to starts
							-- (5) this is the body of code 
--INTERNAL LOOP 4: 
--FIRST DECLARE AND DEFINE THE VARIABLE @NY THAT IS THE NUMBER OF SPONSORS THAT ARE PREDICTED WILL GIVE IN CATEGORY @PATT
DECLARE @NY INTEGER;
SET @NY = (SELECT Y.Y_GIVE FROM #Patt65_CONSOL Y WHERE Y.Pattern_Number = @PATT)

--INTERNAL LOOP 5: CREATING THE SCORES FOR A PARTICULAR PATTERN @NY AND STORE IT INTO #ONE_PATT
-- #ONE_PATT creates and stores the values for the current loop pattern -- this code stores the randomly selected @NY as GAVE_FLG = 1
select top (@NY) a.PR_DONOR_ID , a.Pattern_Number, 1 AS 'GAVE_FLG' into #ONE_PATT from #PATTERN_data a where a.Pattern_Number = @PATT  ORDER BY NEWID()

-- #ONE_PATT creates and stores the values for the current loop pattern -- this code stores the not randomly selected @NY as GAVE_FLG = 0
insert into #ONE_PATT
select b.PR_DONOR_ID , b.Pattern_Number, 0 AS 'GAVE_FLG' from #PATTERN_data b 
left join #ONE_PATT a on a.PR_DONOR_ID = b.PR_DONOR_ID  
where a.PR_DONOR_ID is null and b.pattern_number = @PATT

--INTERNAL LOOP 6: #ONE_PATT RESULT FROM PATTERN ITERATION STORED IN #QRT_PREDICTION ON A CUMULATIVE BASIS
--now the result of #ONE_PATT which is just for one pattern is stored into the more formal #QRT_Prediction that stores ALL pattern results
insert into #QRT_PREDICTION (PR_DONOR_ID, PATTERN_NUMBER, GAVE_FLG)
select A.PR_DONOR_ID , a.Pattern_Number, a.GAVE_FLG from #ONE_PATT a

--FINALLY THE #ONE_PATT ID DROPPED SO IT CAN BE RE-USED FOR THE NEXT PATTERN IN THE ITERATION 
drop table #ONE_PATT
	

PRINT @PATT;                  -- (5) print instruction ... can be way more complex
SET @PATT = @PATT+1;          -- (6) the @variable @PATT is set up as @PATT +1 in order to allow for the next iteration 
END;						  -- (7) this instruction tells the machine to end the loop 

--by the END all donors must have a GAVE_FLG as 1, 0 as they were randomly selected in their respective Pattern (1 to 64)
--END OF INTERNAL LOOP WHAT COMES NEXT IS PROCESSED AS PART OF THE EXTERNAL LOOP ONLY AS MANY TIMES AS PREDICTION QUSRTERS ARE DEFINED
--==============================================================================================================
--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP
--==============================================================================================================
--DECLARE @PQRT INTEGER;         -- (1) declares the temporal variable count as an integer 
--SET @PQRT = 1 ;     		   -- (2) set the initial value of the temporal value @PQRT as 1
--DECLARE @PEND INTEGER;         -- (3) declares the variable @PEND 
--SET @PEND = 1 ; 
--EXTERNAL LOOP 5: 
--INTRODUCE CODE TO SAVE THE gAVE/dID NOT GIVE PREDICTION FROM #QRT_PREDICTION INTO THE MASTER TABLE #MASTER_PRED_CONSOLIDATED

--THE VARIABLES @SQL2 AND @SQL3 ARE DECLARED NVARCHAR(MAX) MEANING TEXT OF THE CODE WITH EXTENSION AS REQUIRED
--@SQL1 WILL DEAL WITH THE ALTER TABLE CODE (ADD NEW COLUMN TO #INITIAL)
--@SQL2 WILL CORRESPOND TO THE UPDATE OF THE TABLE (ADD CONTENT OF #SEQ_data TO NEW COLUMN IN #INITIAL

    DECLARE @SQL2 NVARCHAR(MAX)   
	DECLARE @SQL3 NVARCHAR(MAX)   

-- First statement add the new row to THE #MASTER_PRED_CONSOLIDATED Table 
-- the name of the column is PRED_1 for the first iteration, PRED_2 for the second and so on ...  it will stop at PRED_i , 

    SET @SQL2 = 'ALTER TABLE #MASTER_PRED_CONSOLIDATED
        ADD ' + concat('PRED_',CAST(@PQRT AS VARCHAR(10))) + ' INTEGER'

-- Second Stement Write the result of the iteration from #SEQ_data into #INITIAL_data into the correct column PRED_i for iteration i

   SET @SQL3 = 'UPDATE #MASTER_PRED_CONSOLIDATED
        SET ' + concat('PRED_',CAST(@PQRT AS VARCHAR(10))) + ' = I.GAVE_FLG
		from 
		#QRT_PREDICTION I,
	    #MASTER_PRED_CONSOLIDATED H
        where
        I.PR_DONOR_ID = H.PR_DONOR_ID'

--THE EXECUTE() CODE WILL ALLOW THE TEXT IN @SQL2 AND @SQL3 TO BE INTERPRETED AS REAL CODE NOT JUST TEXT
EXECUTE (@SQL2)
EXECUTE (@SQL3)

--drop table #QRT_PREDICTION
--EXTERNAL LOOP 6: 
--INTRODUCE CODE TO SAVE THE PATTERN INFORMATION FROM #QRT_PREDICTION INTO THE MASTER_PATT TABLE #MASTER_PATT_CONSOLIDATED
--DECLARE @PQRT INTEGER;         -- (1) declares the temporal variable count as an integer 
--SET @PQRT = 1 ;     		   -- (2) set the initial value of the temporal value @PQRT as 1
--DECLARE @PEND INTEGER;         -- (3) declares the variable @PEND 
--SET @PEND = 1 ; 
--THE VARIABLES @SQL4 AND @SQL5 ARE DECLARED NVARCHAR(MAX) MEANING TEXT OF THE CODE WITH EXTENSION AS REQUIRED
--@SQL1 WILL DEAL WITH THE ALTER TABLE CODE (ADD NEW COLUMN TO #INITIAL)
--@SQL2 WILL CORRESPOND TO THE UPDATE OF THE TABLE (ADD CONTENT OF #SEQ_data TO NEW COLUMN IN #INITIAL

    DECLARE @SQL4 NVARCHAR(MAX)   
	DECLARE @SQL5 NVARCHAR(MAX)   

-- First statement add the new row to THE #MASTER_PRED_CONSOLIDATED Table 
-- the name of the column is PRED_1 for the first iteration, PRED_2 for the second and so on ...  it will stop at PRED_i , 

    SET @SQL4 = 'ALTER TABLE #MASTER_PATT_CONSOLIDATED
        ADD ' + concat('PATT_',CAST(@PQRT AS VARCHAR(10))) + ' INTEGER'

-- Second Stement Write the result of the iteration from #SEQ_data into #INITIAL_data into the correct column PRED_i for iteration i

   SET @SQL5 = 'UPDATE #MASTER_PATT_CONSOLIDATED
        SET ' + concat('PATT_',CAST(@PQRT AS VARCHAR(10))) + ' = I.PATTERN_NUMBER
		from 
		#QRT_PREDICTION I,
	    #MASTER_PATT_CONSOLIDATED H
        where
        I.PR_DONOR_ID = H.PR_DONOR_ID'

--THE EXECUTE() CODE WILL ALLOW THE TEXT IN @SQL4 AND @SQL5 TO BE INTERPRETED AS REAL CODE NOT JUST TEXT
EXECUTE (@SQL4)
EXECUTE (@SQL5)


--FINALLY THE #QRT_PREDICTION TABLE IS DROPPED SO IT CAN BE RE-USED FOR THE NEXT QUARTER (EXTERNAL LOOP ITERATION) PATTERN IN THE ITERATION 
drop table #QRT_PREDICTION
drop table #Patt65_CONSOL
drop table #PATTERN_data
--drop table #Patt65_CONSOL	

--PRINT @PQRT;                  -- (5) print instruction ... can be way more complex
SET @PQRT = @PQRT+1;          -- (6) the @variable @PATT is set up as @PATT +1 in order to allow for the next iteration 
END;						  -- (7) this instruction tells the machine to end the loop 

select * 
INTO [SPSS_Sandbox].[dbo].[LTDV_SP_FY21_PL_INCOME_FLG_80QRT_DEMO]
from #MASTER_PRED_CONSOLIDATED
ORDER BY 1

select * 
INTO [SPSS_Sandbox].[dbo].[LTDV_SP_FY21_PL_PATTERN_N_80QRT]
from #MASTER_PATT_CONSOLIDATED
order by 1

--12m:53m
--(7023 rows affected)
--Completion time: 2022-11-13T23:53:59.4489217-05:00
--==============================================================================================================
--==============================================================================================================
--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP
--==============================================================================================================
drop table #MASTER_PRED_CONSOLIDATED
drop table #MASTER_PATT_CONSOLIDATED

--LOOP TABLES IN [SPSS_SANDBOX]:
select * from [SPSS_Sandbox].[dbo].[LTDV_SP_FY21_PL_INCOME_FLG_80QRT_DEMO]

select * from [SPSS_Sandbox].[dbo].[LTDV_SP_FY21_PL_PATTERN_N_80QRT] order by 1

--ok!! ALgorithm worked beautifully!


--=====================================================================================================
-- SPONSORS AS SINGLE GIVERS
--==================================================================================================================================================
--NOT PART OF THE LOOP 
--===================================================================================================================================================
--TABLE 1: HISTORICAL GIVING FLAG PER DONOR FOR last 5 quarters pattern plus prediction AND RELEVANT SPONSOR CATEGORY ... 
--===================================================================================================================================================
--Needs to define last 4 quarters data available ... 

select dn.Donation_Donor_Id as 'PR_DONOR_ID'
, a.DNR_CAT
, isnull(max(iif(cal.Tri_Fin_Ds_Lg = '2022 Q1' ,1,0)),0) as 'Q1'
, isnull(max(iif(cal.Tri_Fin_Ds_Lg = '2022 Q2' ,1,0)),0) as 'Q2'
, isnull(max(iif(cal.Tri_Fin_Ds_Lg = '2022 Q3' ,1,0)),0) as 'Q3'
, isnull(max(iif(cal.Tri_Fin_Ds_Lg = '2022 Q4' ,1,0)),0) as 'Q4'
into #MASTER_PRED_CONSOLIDATED
from ADOBE.RAW.F_DONATION dn
join [BI_DW].[dbo].[D_Cal] cal on cal.Dt = dn.Donation_Deposit_Date
join [SPSS_Sandbox].[dbo].[LTDV_SCRIPT_FY21_25072022_PAST_INCOME_TNR_DEMO] a on a.DONATION_DONOR_ID = dn.Donation_Donor_Id
where dn.Donation_Adjustment_Reason_Sid = 0
and dn.Donation_Income_Type_for_Donor_Metrics_Sid IN(103,104,105,106,107,108)
	   --SCOPE OF THE QUERY must be ONE FISCAL YEAR 
and dn.Donation_Deposit_Date between '2021-10-01' and '2022-09-30'
AND a.DNR_LABEL = 'SPONSOR'
group by dn.Donation_Donor_Id, a.DNR_CAT
--(57169 rows affected)
--Completion time: 2022-11-13T23:55:30.6741138-05:00


select * from #MASTER_PRED_CONSOLIDATED
--drop table  #MASTER_PRED_CONSOLIDATED 

--===================================================================================================================================================
--TABLE 2: #EX_ITER_ORDER -- SEQUENCE OF 4 QUARTERS TO BE PICKED FOR BUILDING THE RELEVANT 4 QUARTERS PATTERN PER SPONSOR 
--THE ROWS CORRESPOND TO THE PREDICTION NUMBER, THE COLUMNS TO THE GENERIC ROLE Q1, Q2,Q3,Q4 
--THIS GENERIC ROLE WILL BE PICKED AS PART OF THE EXTERNAL LOOP ... 
--===================================================================================================================================================

-- TABLE ALREADY CREATED : SEE CODE IN LINES 1176 1267

--===================================================================================================================================================
--TABLE 3: #MASTER_PATT_CONSOLIDATED -- NUMBER FROM 1 TO 64 THAT SUMMARIZES THE POSITION OF THE SPONSOR IN THE CATEGORY+Q1Q2Q3Q4 PATTERN PER QUARTER 
--THE INITIAL ROW ARE THE DONOR_IDS , THE COLUMNS WILL BE ADDED AS PART OF THE EXTERNAL LOOP  
--THIS GENERIC ROLE WILL BE PICKED AS PART OF THE EXTERNAL LOOP ... 
--===================================================================================================================================================

SELECT A.PR_DONOR_ID 
INTO #MASTER_PATT_CONSOLIDATED
from #MASTER_PRED_CONSOLIDATED A

--(57169 rows affected)
--Completion time: 2022-11-13T23:23:16.7657979-05:00
--DROP TABLE #MASTER_PATT_CONSOLIDATED
--(333616 rows affected)
--SELECT * FROM  #MASTER_PATT_CONSOLIDATED ORDER BY 1

--SELECT PATT_1 , COUNT( PR_DONOR_ID) AS 'CN' FROM  #MASTER_PATT_CONSOLIDATED GROUP BY PATT_1 ORDER BY 1

SELECT * FROM #MASTER_PRED_CONSOLIDATED

--===========================================================================================================================================
-- END OF OUT-OF-THE-LOOP-TABLES
--===========================================================================================================================================

--===========================================================================================================================================
-- START OF EXTERNAL LOOP
--===========================================================================================================================================
--EXTERNAL LOOP 1 - WE NEED TO CREATE THE CODE FOR SELECTING THE RELEVANT SEQUENCE OF 4 QUARTERS TO BE PICKED FOR BUILDING PATTERN PER SPONSOR 
--CAREFUL WITH THE CREATION OF TEMPORAL TABLES AS PART OF THE EXEC()!!!
--tHE Exec command makes temp procedure from THE @sql qUEry and executes it. 
--When that procedure ends, all temp tables created in it will be dropped immediately, so NO access to the created table from outside current dynamic query. 
--use global temp table ##table_name to keep it alive ... MAKE SURE TO DELETE IT AFTER THE PROCESURE IS FINISHED ...
--again  dynamically, cannot use #TEMPTABLE because a local temp table will only exist in the scope of the query that defines it. 
--Using ## creates a global temp table which will be accessible outside the scope of the dynamic query.
--EXTERNAL LOOP 2 - now the table ##PATT_Q1Q2Q3Q4 is processed to create the patterns (65 Pattern_QRT + SP Category) per donor
-- GENERATE PATTERNS FOR EACH SPONSOR BASED ON THE INFORMATION OF QUERY1 Q1,Q2,Q3,Q4 1,0 INDICATORS AND THE DNR_CATEGORY 
--64 COMBINATIONS OF PATTERN AND DONOR CATEGORY AND An OTHER category in case there were some issue with data: 65 GROUPS
--===============================
--EXTERNAL LOOP 3 - now the table #Patt65 is left joined with [SPSS_Sandbox].[dbo].[LTDV_FY21_PRIOR_PROB_VECTORS_SP_PL_SG_DEMO] to create the counts patterns 
--(65 Pattern_QRT + SP Category) per donor and the number of people expected to give per pattern
-- GENERATE THE CONSOLIDATED MATRIX WITH 64 ROWS ONE PATTERN IN EACH ONE AND JOINS WITH [SPSS_Sandbox].[dbo].[LTDV_FY21_PRIOR_PROB_VECTORS_SP_PL_SG_DEMO]
-- THAT HAS THE PRIOR PROBABILITY PER PATTERN TO GENERATE THE NUMBER OF CASES PREDICTED AS GIVING IN THE QUARTER.
-- ==> Input for P(E) Number of donor in Pattern i / Divided by N_Donors
--, sum (b.Gave_in_2020_Q2) as 'N_Donors_Gave' -- ==> Input for P(H) Number of donors that Gave / Divided by N_Donors
--, convert(decimal(5,4) ,sum (b.Gave_in_2020_Q2)* 1./ count (b.Donation_Donor_Id)) as 'Prob_Gave_NextQ' -- ==> BAYESIAN ESTIMATION OF P(H/E)
--into [SPSS_Sandbox].[dbo].[LTSV_FY19_20_Prior_Prob_19_20]

DECLARE @PQRT INTEGER;         -- (1) declares the temporal variable count as an integer 
SET @PQRT = 1 ;     		   -- (2) set the initial value of the temporal value @PQRT as 1
DECLARE @PEND INTEGER;         -- (3) declares the variable @PEND 
SET @PEND = (select max(EXITER_N) from  #EX_ITER_ORDER) ;                --(select max(EXITER_N) from  #EX_ITER_ORDER) ;

--EXTERNAL LOOP: DEFINITION OF THE CONDITION FOR WHILE CONDITION THE ITERATION ==>

WHILE (@PQRT <=@PEND)
BEGIN
PRINT @PQRT

 CREATE TABLE #PATTERN_data (PR_DONOR_ID NUMERIC(8,0) , Pattern_Number INTEGER)

DECLARE @Q1 VARCHAR(7)
DECLARE @Q2 VARCHAR(7)
DECLARE @Q3 VARCHAR(7)
DECLARE @Q4 VARCHAR(7)
SET @Q1 = (SELECT R.Q1 FROM #EX_ITER_ORDER R WHERE R.EXITER_N = @PQRT)
SET @Q2 = (SELECT R.Q2 FROM #EX_ITER_ORDER R WHERE R.EXITER_N = @PQRT)
SET @Q3 = (SELECT R.Q3 FROM #EX_ITER_ORDER R WHERE R.EXITER_N = @PQRT)
SET @Q4 = (SELECT R.Q4 FROM #EX_ITER_ORDER R WHERE R.EXITER_N = @PQRT)

PRINT @Q1
PRINT @Q2
PRINT @Q3
PRINT @Q4

--DECLARE @SQL1 NVARCHAR(MAX)
DECLARE @SQL1 NVARCHAR(MAX)
SET @SQL1 = 'SELECT PR_DONOR_ID, DNR_CAT, CAST(CONCAT(' + CAST(@Q1 AS VARCHAR(10)) +  ',' + CAST(@Q2 AS VARCHAR(10)) +  ',' + CAST(@Q3 AS VARCHAR(10)) +  ',' + CAST(@Q4 AS VARCHAR(10))
 + ') AS VARCHAR(4)) AS ''QRT_PATT'' INTO #PATT_Q1Q2Q3Q4 FROM #MASTER_PRED_CONSOLIDATED

INSERT INTO #PATTERN_data (PR_DONOR_ID , Pattern_Number)
SELECT PR_DONOR_ID
, CASE 
WHEN QRT_PATT = ''1111'' and DNR_CAT = ''NEW'' then 1
WHEN QRT_PATT = ''1111'' and DNR_CAT = ''UPGRADED'' then 2
WHEN QRT_PATT = ''1111'' and DNR_CAT = ''REACTIVATED'' then 3
WHEN QRT_PATT = ''1111'' and DNR_CAT = ''OLD'' then 4
WHEN QRT_PATT = ''0111'' and DNR_CAT = ''NEW'' then 5
WHEN QRT_PATT = ''0111'' and DNR_CAT = ''UPGRADED'' then 6
WHEN QRT_PATT = ''0111'' and DNR_CAT = ''REACTIVATED'' then 7
WHEN QRT_PATT = ''0111'' and DNR_CAT = ''OLD'' then 8
WHEN QRT_PATT = ''1011'' and DNR_CAT = ''NEW'' then 9
WHEN QRT_PATT = ''1011'' and DNR_CAT = ''UPGRADED'' then 10
WHEN QRT_PATT = ''1011'' and DNR_CAT = ''REACTIVATED'' then 11
WHEN QRT_PATT = ''1011'' and DNR_CAT = ''OLD'' then 12
WHEN QRT_PATT = ''1101'' and DNR_CAT = ''NEW'' then 13
WHEN QRT_PATT = ''1101'' and DNR_CAT = ''UPGRADED'' then 14
WHEN QRT_PATT = ''1101'' and DNR_CAT = ''REACTIVATED'' then 15
WHEN QRT_PATT = ''1101'' and DNR_CAT = ''OLD'' then 16
WHEN QRT_PATT = ''0011'' and DNR_CAT = ''NEW'' then 17
WHEN QRT_PATT = ''0011'' and DNR_CAT = ''UPGRADED'' then 18
WHEN QRT_PATT = ''0011'' and DNR_CAT = ''REACTIVATED'' then 19
WHEN QRT_PATT = ''0011'' and DNR_CAT = ''OLD'' then 20
WHEN QRT_PATT = ''0101'' and DNR_CAT = ''NEW'' then 21
WHEN QRT_PATT = ''0101'' and DNR_CAT = ''UPGRADED'' then 22
WHEN QRT_PATT = ''0101'' and DNR_CAT = ''REACTIVATED'' then 23
WHEN QRT_PATT = ''0101'' and DNR_CAT = ''OLD'' then 24
WHEN QRT_PATT = ''1001'' and DNR_CAT = ''NEW'' then 25
WHEN QRT_PATT = ''1001'' and DNR_CAT = ''UPGRADED'' then 26
WHEN QRT_PATT = ''1001'' and DNR_CAT = ''REACTIVATED'' then 27
WHEN QRT_PATT = ''1001'' and DNR_CAT = ''OLD'' then 28
WHEN QRT_PATT = ''0001'' and DNR_CAT = ''NEW'' then 29
WHEN QRT_PATT = ''0001'' and DNR_CAT = ''UPGRADED'' then 30
WHEN QRT_PATT = ''0001'' and DNR_CAT = ''REACTIVATED'' then 31
WHEN QRT_PATT = ''0001'' and DNR_CAT = ''OLD'' then 32
WHEN QRT_PATT = ''1110'' and DNR_CAT = ''NEW'' then 33
WHEN QRT_PATT = ''1110'' and DNR_CAT = ''UPGRADED'' then 34
WHEN QRT_PATT = ''1110'' and DNR_CAT = ''REACTIVATED'' then 35
WHEN QRT_PATT = ''1110'' and DNR_CAT = ''OLD'' then 36
WHEN QRT_PATT = ''0110'' and DNR_CAT = ''NEW'' then 37
WHEN QRT_PATT = ''0110'' and DNR_CAT = ''UPGRADED'' then 38
WHEN QRT_PATT = ''0110'' and DNR_CAT = ''REACTIVATED'' then 39
WHEN QRT_PATT = ''0110'' and DNR_CAT = ''OLD'' then 40
WHEN QRT_PATT = ''1010'' and DNR_CAT = ''NEW'' then 41
WHEN QRT_PATT = ''1010'' and DNR_CAT = ''UPGRADED'' then 42
WHEN QRT_PATT = ''1010'' and DNR_CAT = ''REACTIVATED'' then 43
WHEN QRT_PATT = ''1010'' and DNR_CAT = ''OLD'' then 44
WHEN QRT_PATT = ''1100'' and DNR_CAT = ''NEW'' then 45
WHEN QRT_PATT = ''1100'' and DNR_CAT = ''UPGRADED'' then 46
WHEN QRT_PATT = ''1100'' and DNR_CAT = ''REACTIVATED'' then 47
WHEN QRT_PATT = ''1100'' and DNR_CAT = ''OLD'' then 48
WHEN QRT_PATT = ''0010'' and DNR_CAT = ''NEW'' then 49
WHEN QRT_PATT = ''0010'' and DNR_CAT = ''UPGRADED'' then 50
WHEN QRT_PATT = ''0010'' and DNR_CAT = ''REACTIVATED'' then 51
WHEN QRT_PATT = ''0010'' and DNR_CAT = ''OLD'' then 52
WHEN QRT_PATT = ''0100'' and DNR_CAT = ''NEW'' then 53
WHEN QRT_PATT = ''0100'' and DNR_CAT = ''UPGRADED'' then 54
WHEN QRT_PATT = ''0100'' and DNR_CAT = ''REACTIVATED'' then 55
WHEN QRT_PATT = ''0100'' and DNR_CAT = ''OLD'' then 56
WHEN QRT_PATT = ''1000'' and DNR_CAT = ''NEW'' then 57
WHEN QRT_PATT = ''1000'' and DNR_CAT = ''UPGRADED'' then 58
WHEN QRT_PATT = ''1000'' and DNR_CAT = ''REACTIVATED'' then 59
WHEN QRT_PATT = ''1000'' and DNR_CAT = ''OLD'' then 60
WHEN QRT_PATT = ''0000'' and DNR_CAT = ''NEW''  then 61
WHEN QRT_PATT = ''0000'' and DNR_CAT = ''UPGRADED'' then 62
WHEN QRT_PATT = ''0000'' and DNR_CAT = ''REACTIVATED'' then 63
WHEN QRT_PATT = ''0000'' and DNR_CAT = ''OLD'' then 64
else 65 end as ''Pattern_Number''
FROM #PATT_Q1Q2Q3Q4'

EXECUTE (@SQL1)

--SELECT  * FROM #PATTERN_data
--select TOP 10 * from [SPSS_Sandbox].[dbo].[LTDV_FY21_PRIOR_PROB_VECTORS_SP_PL_SG_DEMO]

select a.Pattern_Number
, count (b.PR_DONOR_ID) as 'N_Donors'
, a.Prob_SPR_Gave_SG_NextQ 
, round(a.Prob_SPR_Gave_SG_NextQ*count (b.PR_DONOR_ID),0) as 'Y_Give'
, count (b.PR_DONOR_ID) - round(a.Prob_SPR_Gave_SG_NextQ*count (b.PR_DONOR_ID),0) as 'N_Give' ----> calculation just uses relevant SP Prob vector 
into #Patt65_CONSOL
FROM [SPSS_Sandbox].[dbo].[LTDV_FY21_PRIOR_PROB_VECTORS_SP_PL_SG_DEMO] a   --==> this table has the information on patterns 
left join  #PATTERN_data b on a.Pattern_Number = b.Pattern_Number
group by a.Pattern_Number , a.Prob_SPR_Gave_SG_NextQ
order by 1

--SELECT * FROM #Patt65_CONSOL ORDER BY 1

--NOW IT IS THE TIME TO INSERT THE INTERNAL LOOP THAT WILL PROCESS EVERYONE OF THE 64 GROUPS IN ORDER TO CREATE A FLAG give/didNOT give PER DONOR PER QUARTER
--THE INTERNAL LOOP RUNS 64 TIMES PER QUARTER.
---BUT BEFORE THAT

--EXTERNAL LOOP 4: table #QRT_PREDICTION THAT STORES THE PREDICTION FOR ALL DONORS FOR THE QUARTER
--The table #QRT_PREDICTION will store all the quarter results coming from the internal loop 
--Must be created OUT of both the external and internal loop since the table is created just once
--and must BE just populated at the end of every external loop iteration 

CREATE TABLE #QRT_PREDICTION (PR_DONOR_ID NUMERIC(8,0) , PATTERN_NUMBER INTEGER , GAVE_FLG INTEGER )
--SELECT * FROM #QRT_PREDICTION
--DROP TABLE #QRT_PREDICTION
--==========================================================================================================
--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP
--==========================================================================================================
--==========================================================================================================
--==========================================================================================================

--INTERNAL LOOP 1:             
--Basic loop to populate the file of sponsors randomly selected as givers during the period

DECLARE @PATT INTEGER;        -- (1) declares the temporal variable count as an integer
SET @PATT = 1 ;               -- (2) set the initial value of the temporal value @PATT as 1
DECLARE @END INTEGER;         -- (3) SET THE VARIABLE @END THAT DEFINES THE TOTAL NUMBER OF ITERATIONS
SET @END = (SELECT MAX(Y.Pattern_Number) FROM #Patt65_CONSOL Y) 

--INTERNAL LOOP 2: SETTING THE LOOP CONDITION ==> ITERATIONS GO FROM 1 TO 64
WHILE (@PATT <=@END)         -- (3) WHILE defines the loop the code will be executed until @PATT  reaches the value 64 NO SEMICOLON!!!

--INTERNAL LOOP 3: BEGIN ===> END DEFINE THE SPACE FOR THE CODE THAT WILL RUN IN ITERATIONS      
BEGIN	                     -- (4) this defines where the code the loop applies to starts
							-- (5) this is the body of code 
--INTERNAL LOOP 4: 
--FIRST DECLARE AND DEFINE THE VARIABLE @NY THAT IS THE NUMBER OF SPONSORS THAT ARE PREDICTED WILL GIVE IN CATEGORY @PATT
DECLARE @NY INTEGER;
SET @NY = (SELECT Y.Y_GIVE FROM #Patt65_CONSOL Y WHERE Y.Pattern_Number = @PATT)

--INTERNAL LOOP 5: CREATING THE SCORES FOR A PARTICULAR PATTERN @NY AND STORE IT INTO #ONE_PATT
-- #ONE_PATT creates and stores the values for the current loop pattern -- this code stores the randomly selected @NY as GAVE_FLG = 1
select top (@NY) a.PR_DONOR_ID , a.Pattern_Number, 1 AS 'GAVE_FLG' into #ONE_PATT from #PATTERN_data a where a.Pattern_Number = @PATT  ORDER BY NEWID()

-- #ONE_PATT creates and stores the values for the current loop pattern -- this code stores the not randomly selected @NY as GAVE_FLG = 0
insert into #ONE_PATT
select b.PR_DONOR_ID , b.Pattern_Number, 0 AS 'GAVE_FLG' from #PATTERN_data b 
left join #ONE_PATT a on a.PR_DONOR_ID = b.PR_DONOR_ID  
where a.PR_DONOR_ID is null and b.pattern_number = @PATT

--INTERNAL LOOP 6: #ONE_PATT RESULT FROM PATTERN ITERATION STORED IN #QRT_PREDICTION ON A CUMULATIVE BASIS
--now the result of #ONE_PATT which is just for one pattern is stored into the more formal #QRT_Prediction that stores ALL pattern results
insert into #QRT_PREDICTION (PR_DONOR_ID, PATTERN_NUMBER, GAVE_FLG)
select A.PR_DONOR_ID , a.Pattern_Number, a.GAVE_FLG from #ONE_PATT a

--FINALLY THE #ONE_PATT ID DROPPED SO IT CAN BE RE-USED FOR THE NEXT PATTERN IN THE ITERATION 
drop table #ONE_PATT
	

PRINT @PATT;                  -- (5) print instruction ... can be way more complex
SET @PATT = @PATT+1;          -- (6) the @variable @PATT is set up as @PATT +1 in order to allow for the next iteration 
END;						  -- (7) this instruction tells the machine to end the loop 

--by the END all donors must have a GAVE_FLG as 1, 0 as they were randomly selected in their respective Pattern (1 to 64)
--END OF INTERNAL LOOP WHAT COMES NEXT IS PROCESSED AS PART OF THE EXTERNAL LOOP ONLY AS MANY TIMES AS PREDICTION QUSRTERS ARE DEFINED
--==============================================================================================================
--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP
--==============================================================================================================
--DECLARE @PQRT INTEGER;         -- (1) declares the temporal variable count as an integer 
--SET @PQRT = 1 ;     		   -- (2) set the initial value of the temporal value @PQRT as 1
--DECLARE @PEND INTEGER;         -- (3) declares the variable @PEND 
--SET @PEND = 1 ; 
--EXTERNAL LOOP 5: 
--INTRODUCE CODE TO SAVE THE gAVE/dID NOT GIVE PREDICTION FROM #QRT_PREDICTION INTO THE MASTER TABLE #MASTER_PRED_CONSOLIDATED

--THE VARIABLES @SQL2 AND @SQL3 ARE DECLARED NVARCHAR(MAX) MEANING TEXT OF THE CODE WITH EXTENSION AS REQUIRED
--@SQL1 WILL DEAL WITH THE ALTER TABLE CODE (ADD NEW COLUMN TO #INITIAL)
--@SQL2 WILL CORRESPOND TO THE UPDATE OF THE TABLE (ADD CONTENT OF #SEQ_data TO NEW COLUMN IN #INITIAL

    DECLARE @SQL2 NVARCHAR(MAX)   
	DECLARE @SQL3 NVARCHAR(MAX)   

-- First statement add the new row to THE #MASTER_PRED_CONSOLIDATED Table 
-- the name of the column is PRED_1 for the first iteration, PRED_2 for the second and so on ...  it will stop at PRED_i , 

    SET @SQL2 = 'ALTER TABLE #MASTER_PRED_CONSOLIDATED
        ADD ' + concat('PRED_',CAST(@PQRT AS VARCHAR(10))) + ' INTEGER'

-- Second Stement Write the result of the iteration from #SEQ_data into #INITIAL_data into the correct column PRED_i for iteration i

   SET @SQL3 = 'UPDATE #MASTER_PRED_CONSOLIDATED
        SET ' + concat('PRED_',CAST(@PQRT AS VARCHAR(10))) + ' = I.GAVE_FLG
		from 
		#QRT_PREDICTION I,
	    #MASTER_PRED_CONSOLIDATED H
        where
        I.PR_DONOR_ID = H.PR_DONOR_ID'

--THE EXECUTE() CODE WILL ALLOW THE TEXT IN @SQL2 AND @SQL3 TO BE INTERPRETED AS REAL CODE NOT JUST TEXT
EXECUTE (@SQL2)
EXECUTE (@SQL3)

--drop table #QRT_PREDICTION
--EXTERNAL LOOP 6: 
--INTRODUCE CODE TO SAVE THE PATTERN INFORMATION FROM #QRT_PREDICTION INTO THE MASTER_PATT TABLE #MASTER_PATT_CONSOLIDATED
--DECLARE @PQRT INTEGER;         -- (1) declares the temporal variable count as an integer 
--SET @PQRT = 1 ;     		   -- (2) set the initial value of the temporal value @PQRT as 1
--DECLARE @PEND INTEGER;         -- (3) declares the variable @PEND 
--SET @PEND = 1 ; 
--THE VARIABLES @SQL4 AND @SQL5 ARE DECLARED NVARCHAR(MAX) MEANING TEXT OF THE CODE WITH EXTENSION AS REQUIRED
--@SQL1 WILL DEAL WITH THE ALTER TABLE CODE (ADD NEW COLUMN TO #INITIAL)
--@SQL2 WILL CORRESPOND TO THE UPDATE OF THE TABLE (ADD CONTENT OF #SEQ_data TO NEW COLUMN IN #INITIAL

    DECLARE @SQL4 NVARCHAR(MAX)   
	DECLARE @SQL5 NVARCHAR(MAX)   

-- First statement add the new row to THE #MASTER_PRED_CONSOLIDATED Table 
-- the name of the column is PRED_1 for the first iteration, PRED_2 for the second and so on ...  it will stop at PRED_i , 

    SET @SQL4 = 'ALTER TABLE #MASTER_PATT_CONSOLIDATED
        ADD ' + concat('PATT_',CAST(@PQRT AS VARCHAR(10))) + ' INTEGER'

-- Second Stement Write the result of the iteration from #SEQ_data into #INITIAL_data into the correct column PRED_i for iteration i

   SET @SQL5 = 'UPDATE #MASTER_PATT_CONSOLIDATED
        SET ' + concat('PATT_',CAST(@PQRT AS VARCHAR(10))) + ' = I.PATTERN_NUMBER
		from 
		#QRT_PREDICTION I,
	    #MASTER_PATT_CONSOLIDATED H
        where
        I.PR_DONOR_ID = H.PR_DONOR_ID'

--THE EXECUTE() CODE WILL ALLOW THE TEXT IN @SQL4 AND @SQL5 TO BE INTERPRETED AS REAL CODE NOT JUST TEXT
EXECUTE (@SQL4)
EXECUTE (@SQL5)


--FINALLY THE #QRT_PREDICTION TABLE IS DROPPED SO IT CAN BE RE-USED FOR THE NEXT QUARTER (EXTERNAL LOOP ITERATION) PATTERN IN THE ITERATION 
drop table #QRT_PREDICTION
drop table #Patt65_CONSOL
drop table #PATTERN_data
--drop table #Patt65_CONSOL	

--PRINT @PQRT;                  -- (5) print instruction ... can be way more complex
SET @PQRT = @PQRT+1;          -- (6) the @variable @PATT is set up as @PATT +1 in order to allow for the next iteration 
END;						  -- (7) this instruction tells the machine to end the loop 

select * 
INTO [SPSS_Sandbox].[dbo].[LTDV_SP_FY21_SG_INCOME_FLG_80QRT_DEMO]
from #MASTER_PRED_CONSOLIDATED
ORDER BY 1

select * 
INTO [SPSS_Sandbox].[dbo].[LTDV_SP_FY21_SG_PATTERN_N_80QRT]
from #MASTER_PATT_CONSOLIDATED
order by 1

--12m:53m
--(57169 rows affected)
--Completion time: 2022-11-14T17:01:56.7572738-05:00
--==============================================================================================================
--==============================================================================================================
--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP
--==============================================================================================================
drop table #MASTER_PRED_CONSOLIDATED
drop table #MASTER_PATT_CONSOLIDATED

--LOOP TABLES IN [SPSS_SANDBOX]:
select * from [SPSS_Sandbox].[dbo].[LTDV_SP_FY21_SG_INCOME_FLG_80QRT_DEMO] -- 57169 RECORDS  SPONSORS AS SINGLE GIVERS

select * from [SPSS_Sandbox].[dbo].[LTDV_SP_FY21_SG_PATTERN_N_80QRT] order by 1

--ok!! ALgorithm worked beautifully!


--======================================================================================================
--======================================================================================================
--======================================================================================================
--PLEDGERS: BAYESIAN LOOPS (2)
--PLEDGERS AS PLEDGERS
--PLEDGERS AS SINGLE GIFT GIVERS
--=======================================================================================================
--PLEDGER_categories and labels in 
--select * from [SPSS_Sandbox].[dbo].[LTDV_SCRIPT_FY21_25072022_PAST_INCOME_TNR_DEMO] where DNR_LABEL = 'PLEDGER'

--==================================================================================================================================================
-- PLEDGERS AS PLEDGERS
--NOT PART OF THE LOOP 
--===================================================================================================================================================
--TABLE 1: HISTORICAL GIVING FLAG PER DONOR FOR last 5 quarters pattern plus prediction AND RELEVANT PLEDGER CATEGORY ... 
--===================================================================================================================================================
--Needs to define last 4 quarters data available ... 

select dn.Donation_Donor_Id as 'PR_DONOR_ID'
, a.DNR_CAT
, isnull(max(iif(cal.Tri_Fin_Ds_Lg = '2022 Q1' ,1,0)),0) as 'Q1'
, isnull(max(iif(cal.Tri_Fin_Ds_Lg = '2022 Q2' ,1,0)),0) as 'Q2'
, isnull(max(iif(cal.Tri_Fin_Ds_Lg = '2022 Q3' ,1,0)),0) as 'Q3'
, isnull(max(iif(cal.Tri_Fin_Ds_Lg = '2022 Q4' ,1,0)),0) as 'Q4'
into #MASTER_PRED_CONSOLIDATED
from ADOBE.RAW.F_DONATION dn
join [BI_DW].[dbo].[D_Cal] cal on cal.Dt = dn.Donation_Deposit_Date
join [SPSS_Sandbox].[dbo].[LTDV_SCRIPT_FY21_25072022_PAST_INCOME_TNR_DEMO] a on a.DONATION_DONOR_ID = dn.Donation_Donor_Id
where dn.Donation_Adjustment_Reason_Sid = 0
and dn.Donation_Income_Type_for_Donor_Metrics_Sid = 101
and a.DNR_LABEL = 'PLEDGER'
	   --SCOPE OF THE QUERY must be ONE FISCAL YEAR 
and dn.Donation_Deposit_Date between '2021-10-01' and '2022-09-30'
group by dn.Donation_Donor_Id, a.DNR_CAT
--(12647 rows affected)
--Completion time: 2022-11-08T17:04:52.2247279-05:00

select * from #MASTER_PRED_CONSOLIDATED
--12647 records
--drop table  #MASTER_PRED_CONSOLIDATED 

--===================================================================================================================================================
--TABLE 2: #EX_ITER_ORDER -- SEQUENCE OF 4 QUARTERS TO BE PICKED FOR BUILDING THE RELEVANT 4 QUARTERS PATTERN PER SPONSOR 
--THE ROWS CORRESPOND TO THE PREDICTION NUMBER, THE COLUMNS TO THE GENERIC ROLE Q1, Q2,Q3,Q4 
--THIS GENERIC ROLE WILL BE PICKED AS PART OF THE EXTERNAL LOOP ... 
--===================================================================================================================================================
-- TABLE ALREADY CREATED : SEE CODE IN LINES 1176 1267


--===================================================================================================================================================
--TABLE 3: #MASTER_PATT_CONSOLIDATED -- NUMBER FROM 1 TO 64 THAT SUMMARIZES THE POSITION OF THE SPONSOR IN THE CATEGORY+Q1Q2Q3Q4 PATTERN PER QUARTER 
--THE INITIAL ROW ARE THE DONOR_IDS , THE COLUMNS WILL BE ADDED AS PART OF THE EXTERNAL LOOP  
--THIS GENERIC ROLE WILL BE PICKED AS PART OF THE EXTERNAL LOOP ... 
--===================================================================================================================================================

SELECT A.PR_DONOR_ID 
INTO #MASTER_PATT_CONSOLIDATED
from #MASTER_PRED_CONSOLIDATED A

--(12647 rows affected)
--Completion time: 2022-11-08T17:14:17.0994110-05:00

--DROP TABLE #MASTER_PATT_CONSOLIDATED

--SELECT * FROM  #MASTER_PATT_CONSOLIDATED ORDER BY 1

--SELECT PATT_1 , COUNT( PR_DONOR_ID) AS 'CN' FROM  #MASTER_PATT_CONSOLIDATED GROUP BY PATT_1 ORDER BY 1

SELECT * FROM #MASTER_PRED_CONSOLIDATED
--===========================================================================================================================================
-- END OF OUT-OF-THE-LOOP-TABLES
--===========================================================================================================================================

--===========================================================================================================================================
-- START OF EXTERNAL LOOP
--===========================================================================================================================================
--EXTERNAL LOOP 1 - WE NEED TO CREATE THE CODE FOR SELECTING THE RELEVANT SEQUENCE OF 4 QUARTERS TO BE PICKED FOR BUILDING PATTERN PER SPONSOR 
--CAREFUL WITH THE CREATION OF TEMPORAL TABLES AS PART OF THE EXEC()!!!
--tHE Exec command makes temp procedure from THE @sql qUEry and executes it. 
--When that procedure ends, all temp tables created in it will be dropped immediately, so NO access to the created table from outside current dynamic query. 
--use global temp table ##table_name to keep it alive ... MAKE SURE TO DELETE IT AFTER THE PROCESURE IS FINISHED ...
--again  dynamically, cannot use #TEMPTABLE because a local temp table will only exist in the scope of the query that defines it. 
--Using ## creates a global temp table which will be accessible outside the scope of the dynamic query.
--EXTERNAL LOOP 2 - now the table ##PATT_Q1Q2Q3Q4 is processed to create the patterns (65 Pattern_QRT + SP Category) per donor
-- GENERATE PATTERNS FOR EACH SPONSOR BASED ON THE INFORMATION OF QUERY1 Q1,Q2,Q3,Q4 1,0 INDICATORS AND THE DNR_CATEGORY 
--64 COMBINATIONS OF PATTERN AND DONOR CATEGORY AND An OTHER category in case there were some issue with data: 65 GROUPS
--===============================
--EXTERNAL LOOP 3 - now the table #Patt65 is left joined with [SPSS_Sandbox].[dbo].[LTDV_FY21_PRIOR_PROB_VECTORS_SP_PL_SG_DEMO] to create the counts patterns 
--(65 Pattern_QRT + SP Category) per donor and the number of people expected to give per pattern
-- GENERATE THE CONSOLIDATED MATRIX WITH 64 ROWS ONE PATTERN IN EACH ONE AND JOINS WITH [SPSS_Sandbox].[dbo].[LTDV_FY21_PRIOR_PROB_VECTORS_SP_PL_SG_DEMO]
-- THAT HAS THE PRIOR PROBABILITY PER PATTERN TO GENERATE THE NUMBER OF CASES PREDICTED AS GIVING IN THE QUARTER.
-- ==> Input for P(E) Number of donor in Pattern i / Divided by N_Donors
--, sum (b.Gave_in_2020_Q2) as 'N_Donors_Gave' -- ==> Input for P(H) Number of donors that Gave / Divided by N_Donors
--, convert(decimal(5,4) ,sum (b.Gave_in_2020_Q2)* 1./ count (b.Donation_Donor_Id)) as 'Prob_Gave_NextQ' -- ==> BAYESIAN ESTIMATION OF P(H/E)
--into [SPSS_Sandbox].[dbo].[LTSV_FY19_20_Prior_Prob_19_20]

DECLARE @PQRT INTEGER;         -- (1) declares the temporal variable count as an integer 
SET @PQRT = 1 ;     		   -- (2) set the initial value of the temporal value @PQRT as 1
DECLARE @PEND INTEGER;         -- (3) declares the variable @PEND 
SET @PEND = (select max(EXITER_N) from  #EX_ITER_ORDER) ;                --(select max(EXITER_N) from  #EX_ITER_ORDER) ;

--EXTERNAL LOOP: DEFINITION OF THE CONDITION FOR WHILE CONDITION THE ITERATION ==>

WHILE (@PQRT <=@PEND)
BEGIN
PRINT @PQRT

 CREATE TABLE #PATTERN_data (PR_DONOR_ID NUMERIC(8,0) , Pattern_Number INTEGER)

DECLARE @Q1 VARCHAR(7)
DECLARE @Q2 VARCHAR(7)
DECLARE @Q3 VARCHAR(7)
DECLARE @Q4 VARCHAR(7)
SET @Q1 = (SELECT R.Q1 FROM #EX_ITER_ORDER R WHERE R.EXITER_N = @PQRT)
SET @Q2 = (SELECT R.Q2 FROM #EX_ITER_ORDER R WHERE R.EXITER_N = @PQRT)
SET @Q3 = (SELECT R.Q3 FROM #EX_ITER_ORDER R WHERE R.EXITER_N = @PQRT)
SET @Q4 = (SELECT R.Q4 FROM #EX_ITER_ORDER R WHERE R.EXITER_N = @PQRT)

PRINT @Q1
PRINT @Q2
PRINT @Q3
PRINT @Q4

--DECLARE @SQL1 NVARCHAR(MAX)
DECLARE @SQL1 NVARCHAR(MAX)
SET @SQL1 = 'SELECT PR_DONOR_ID, DNR_CAT, CAST(CONCAT(' + CAST(@Q1 AS VARCHAR(10)) +  ',' + CAST(@Q2 AS VARCHAR(10)) +  ',' + CAST(@Q3 AS VARCHAR(10)) +  ',' + CAST(@Q4 AS VARCHAR(10))
 + ') AS VARCHAR(4)) AS ''QRT_PATT'' INTO #PATT_Q1Q2Q3Q4 FROM #MASTER_PRED_CONSOLIDATED

INSERT INTO #PATTERN_data (PR_DONOR_ID , Pattern_Number)
SELECT PR_DONOR_ID
, CASE 
WHEN QRT_PATT = ''1111'' and DNR_CAT = ''NEW'' then 1
WHEN QRT_PATT = ''1111'' and DNR_CAT = ''UPGRADED'' then 2
WHEN QRT_PATT = ''1111'' and DNR_CAT = ''REACTIVATED'' then 3
WHEN QRT_PATT = ''1111'' and DNR_CAT = ''OLD'' then 4
WHEN QRT_PATT = ''0111'' and DNR_CAT = ''NEW'' then 5
WHEN QRT_PATT = ''0111'' and DNR_CAT = ''UPGRADED'' then 6
WHEN QRT_PATT = ''0111'' and DNR_CAT = ''REACTIVATED'' then 7
WHEN QRT_PATT = ''0111'' and DNR_CAT = ''OLD'' then 8
WHEN QRT_PATT = ''1011'' and DNR_CAT = ''NEW'' then 9
WHEN QRT_PATT = ''1011'' and DNR_CAT = ''UPGRADED'' then 10
WHEN QRT_PATT = ''1011'' and DNR_CAT = ''REACTIVATED'' then 11
WHEN QRT_PATT = ''1011'' and DNR_CAT = ''OLD'' then 12
WHEN QRT_PATT = ''1101'' and DNR_CAT = ''NEW'' then 13
WHEN QRT_PATT = ''1101'' and DNR_CAT = ''UPGRADED'' then 14
WHEN QRT_PATT = ''1101'' and DNR_CAT = ''REACTIVATED'' then 15
WHEN QRT_PATT = ''1101'' and DNR_CAT = ''OLD'' then 16
WHEN QRT_PATT = ''0011'' and DNR_CAT = ''NEW'' then 17
WHEN QRT_PATT = ''0011'' and DNR_CAT = ''UPGRADED'' then 18
WHEN QRT_PATT = ''0011'' and DNR_CAT = ''REACTIVATED'' then 19
WHEN QRT_PATT = ''0011'' and DNR_CAT = ''OLD'' then 20
WHEN QRT_PATT = ''0101'' and DNR_CAT = ''NEW'' then 21
WHEN QRT_PATT = ''0101'' and DNR_CAT = ''UPGRADED'' then 22
WHEN QRT_PATT = ''0101'' and DNR_CAT = ''REACTIVATED'' then 23
WHEN QRT_PATT = ''0101'' and DNR_CAT = ''OLD'' then 24
WHEN QRT_PATT = ''1001'' and DNR_CAT = ''NEW'' then 25
WHEN QRT_PATT = ''1001'' and DNR_CAT = ''UPGRADED'' then 26
WHEN QRT_PATT = ''1001'' and DNR_CAT = ''REACTIVATED'' then 27
WHEN QRT_PATT = ''1001'' and DNR_CAT = ''OLD'' then 28
WHEN QRT_PATT = ''0001'' and DNR_CAT = ''NEW'' then 29
WHEN QRT_PATT = ''0001'' and DNR_CAT = ''UPGRADED'' then 30
WHEN QRT_PATT = ''0001'' and DNR_CAT = ''REACTIVATED'' then 31
WHEN QRT_PATT = ''0001'' and DNR_CAT = ''OLD'' then 32
WHEN QRT_PATT = ''1110'' and DNR_CAT = ''NEW'' then 33
WHEN QRT_PATT = ''1110'' and DNR_CAT = ''UPGRADED'' then 34
WHEN QRT_PATT = ''1110'' and DNR_CAT = ''REACTIVATED'' then 35
WHEN QRT_PATT = ''1110'' and DNR_CAT = ''OLD'' then 36
WHEN QRT_PATT = ''0110'' and DNR_CAT = ''NEW'' then 37
WHEN QRT_PATT = ''0110'' and DNR_CAT = ''UPGRADED'' then 38
WHEN QRT_PATT = ''0110'' and DNR_CAT = ''REACTIVATED'' then 39
WHEN QRT_PATT = ''0110'' and DNR_CAT = ''OLD'' then 40
WHEN QRT_PATT = ''1010'' and DNR_CAT = ''NEW'' then 41
WHEN QRT_PATT = ''1010'' and DNR_CAT = ''UPGRADED'' then 42
WHEN QRT_PATT = ''1010'' and DNR_CAT = ''REACTIVATED'' then 43
WHEN QRT_PATT = ''1010'' and DNR_CAT = ''OLD'' then 44
WHEN QRT_PATT = ''1100'' and DNR_CAT = ''NEW'' then 45
WHEN QRT_PATT = ''1100'' and DNR_CAT = ''UPGRADED'' then 46
WHEN QRT_PATT = ''1100'' and DNR_CAT = ''REACTIVATED'' then 47
WHEN QRT_PATT = ''1100'' and DNR_CAT = ''OLD'' then 48
WHEN QRT_PATT = ''0010'' and DNR_CAT = ''NEW'' then 49
WHEN QRT_PATT = ''0010'' and DNR_CAT = ''UPGRADED'' then 50
WHEN QRT_PATT = ''0010'' and DNR_CAT = ''REACTIVATED'' then 51
WHEN QRT_PATT = ''0010'' and DNR_CAT = ''OLD'' then 52
WHEN QRT_PATT = ''0100'' and DNR_CAT = ''NEW'' then 53
WHEN QRT_PATT = ''0100'' and DNR_CAT = ''UPGRADED'' then 54
WHEN QRT_PATT = ''0100'' and DNR_CAT = ''REACTIVATED'' then 55
WHEN QRT_PATT = ''0100'' and DNR_CAT = ''OLD'' then 56
WHEN QRT_PATT = ''1000'' and DNR_CAT = ''NEW'' then 57
WHEN QRT_PATT = ''1000'' and DNR_CAT = ''UPGRADED'' then 58
WHEN QRT_PATT = ''1000'' and DNR_CAT = ''REACTIVATED'' then 59
WHEN QRT_PATT = ''1000'' and DNR_CAT = ''OLD'' then 60
WHEN QRT_PATT = ''0000'' and DNR_CAT = ''NEW''  then 61
WHEN QRT_PATT = ''0000'' and DNR_CAT = ''UPGRADED'' then 62
WHEN QRT_PATT = ''0000'' and DNR_CAT = ''REACTIVATED'' then 63
WHEN QRT_PATT = ''0000'' and DNR_CAT = ''OLD'' then 64
else 65 end as ''Pattern_Number''
FROM #PATT_Q1Q2Q3Q4'

EXECUTE (@SQL1)

--SELECT  * FROM #PATTERN_data
--select TOP 10 * from [SPSS_Sandbox].[dbo].[LTDV_FY21_PRIOR_PROB_VECTORS_SP_PL_SG_DEMO]

select a.Pattern_Number
, count (b.PR_DONOR_ID) as 'N_Donors'
, a.Prob_PLR_Gave_PL_NextQ 
, round(a.Prob_PLR_Gave_PL_NextQ*count (b.PR_DONOR_ID),0) as 'Y_Give'
, count (b.PR_DONOR_ID) - round(a.Prob_PLR_Gave_PL_NextQ*count (b.PR_DONOR_ID),0) as 'N_Give' ----> calculation just uses relevant SP Prob vector 
into #Patt65_CONSOL
FROM [SPSS_Sandbox].[dbo].[LTDV_FY21_PRIOR_PROB_VECTORS_SP_PL_SG_DEMO] a   --==> this table has the information on patterns 
left join  #PATTERN_data b on a.Pattern_Number = b.Pattern_Number
group by a.Pattern_Number , a.Prob_PLR_Gave_PL_NextQ
order by 1

--SELECT * FROM #Patt65_CONSOL ORDER BY 1

--NOW IT IS THE TIME TO INSERT THE INTERNAL LOOP THAT WILL PROCESS EVERYONE OF THE 64 GROUPS IN ORDER TO CREATE A FLAG give/didNOT give PER DONOR PER QUARTER
--THE INTERNAL LOOP RUNS 64 TIMES PER QUARTER.
---BUT BEFORE THAT

--EXTERNAL LOOP 4: table #QRT_PREDICTION THAT STORES THE PREDICTION FOR ALL DONORS FOR THE QUARTER
--The table #QRT_PREDICTION will store all the quarter results coming from the internal loop 
--Must be created OUT of both the external and internal loop since the table is created just once
--and must BE just populated at the end of every external loop iteration 

CREATE TABLE #QRT_PREDICTION (PR_DONOR_ID NUMERIC(8,0) , PATTERN_NUMBER INTEGER , GAVE_FLG INTEGER )
--SELECT * FROM #QRT_PREDICTION
--DROP TABLE #QRT_PREDICTION
--==========================================================================================================
--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP
--==========================================================================================================
--==========================================================================================================
--==========================================================================================================

--INTERNAL LOOP 1:             
--Basic loop to populate the file of sponsors randomly selected as givers during the period

DECLARE @PATT INTEGER;        -- (1) declares the temporal variable count as an integer
SET @PATT = 1 ;               -- (2) set the initial value of the temporal value @PATT as 1
DECLARE @END INTEGER;         -- (3) SET THE VARIABLE @END THAT DEFINES THE TOTAL NUMBER OF ITERATIONS
SET @END = (SELECT MAX(Y.Pattern_Number) FROM #Patt65_CONSOL Y) 

--INTERNAL LOOP 2: SETTING THE LOOP CONDITION ==> ITERATIONS GO FROM 1 TO 64
WHILE (@PATT <=@END)         -- (3) WHILE defines the loop the code will be executed until @PATT  reaches the value 64 NO SEMICOLON!!!

--INTERNAL LOOP 3: BEGIN ===> END DEFINE THE SPACE FOR THE CODE THAT WILL RUN IN ITERATIONS      
BEGIN	                     -- (4) this defines where the code the loop applies to starts
							-- (5) this is the body of code 
--INTERNAL LOOP 4: 
--FIRST DECLARE AND DEFINE THE VARIABLE @NY THAT IS THE NUMBER OF SPONSORS THAT ARE PREDICTED WILL GIVE IN CATEGORY @PATT
DECLARE @NY INTEGER;
SET @NY = (SELECT Y.Y_GIVE FROM #Patt65_CONSOL Y WHERE Y.Pattern_Number = @PATT)

--INTERNAL LOOP 5: CREATING THE SCORES FOR A PARTICULAR PATTERN @NY AND STORE IT INTO #ONE_PATT
-- #ONE_PATT creates and stores the values for the current loop pattern -- this code stores the randomly selected @NY as GAVE_FLG = 1
select top (@NY) a.PR_DONOR_ID , a.Pattern_Number, 1 AS 'GAVE_FLG' into #ONE_PATT from #PATTERN_data a where a.Pattern_Number = @PATT  ORDER BY NEWID()

-- #ONE_PATT creates and stores the values for the current loop pattern -- this code stores the not randomly selected @NY as GAVE_FLG = 0
insert into #ONE_PATT
select b.PR_DONOR_ID , b.Pattern_Number, 0 AS 'GAVE_FLG' from #PATTERN_data b 
left join #ONE_PATT a on a.PR_DONOR_ID = b.PR_DONOR_ID  
where a.PR_DONOR_ID is null and b.pattern_number = @PATT

--INTERNAL LOOP 6: #ONE_PATT RESULT FROM PATTERN ITERATION STORED IN #QRT_PREDICTION ON A CUMULATIVE BASIS
--now the result of #ONE_PATT which is just for one pattern is stored into the more formal #QRT_Prediction that stores ALL pattern results
insert into #QRT_PREDICTION (PR_DONOR_ID, PATTERN_NUMBER, GAVE_FLG)
select A.PR_DONOR_ID , a.Pattern_Number, a.GAVE_FLG from #ONE_PATT a

--FINALLY THE #ONE_PATT ID DROPPED SO IT CAN BE RE-USED FOR THE NEXT PATTERN IN THE ITERATION 
drop table #ONE_PATT
	

PRINT @PATT;                  -- (5) print instruction ... can be way more complex
SET @PATT = @PATT+1;          -- (6) the @variable @PATT is set up as @PATT +1 in order to allow for the next iteration 
END;						  -- (7) this instruction tells the machine to end the loop 

--by the END all donors must have a GAVE_FLG as 1, 0 as they were randomly selected in their respective Pattern (1 to 64)
--END OF INTERNAL LOOP WHAT COMES NEXT IS PROCESSED AS PART OF THE EXTERNAL LOOP ONLY AS MANY TIMES AS PREDICTION QUSRTERS ARE DEFINED
--==============================================================================================================
--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP
--==============================================================================================================
--DECLARE @PQRT INTEGER;         -- (1) declares the temporal variable count as an integer 
--SET @PQRT = 1 ;     		   -- (2) set the initial value of the temporal value @PQRT as 1
--DECLARE @PEND INTEGER;         -- (3) declares the variable @PEND 
--SET @PEND = 1 ; 
--EXTERNAL LOOP 5: 
--INTRODUCE CODE TO SAVE THE gAVE/dID NOT GIVE PREDICTION FROM #QRT_PREDICTION INTO THE MASTER TABLE #MASTER_PRED_CONSOLIDATED

--THE VARIABLES @SQL2 AND @SQL3 ARE DECLARED NVARCHAR(MAX) MEANING TEXT OF THE CODE WITH EXTENSION AS REQUIRED
--@SQL1 WILL DEAL WITH THE ALTER TABLE CODE (ADD NEW COLUMN TO #INITIAL)
--@SQL2 WILL CORRESPOND TO THE UPDATE OF THE TABLE (ADD CONTENT OF #SEQ_data TO NEW COLUMN IN #INITIAL

    DECLARE @SQL2 NVARCHAR(MAX)   
	DECLARE @SQL3 NVARCHAR(MAX)   

-- First statement add the new row to THE #MASTER_PRED_CONSOLIDATED Table 
-- the name of the column is PRED_1 for the first iteration, PRED_2 for the second and so on ...  it will stop at PRED_i , 

    SET @SQL2 = 'ALTER TABLE #MASTER_PRED_CONSOLIDATED
        ADD ' + concat('PRED_',CAST(@PQRT AS VARCHAR(10))) + ' INTEGER'

-- Second Stement Write the result of the iteration from #SEQ_data into #INITIAL_data into the correct column PRED_i for iteration i

   SET @SQL3 = 'UPDATE #MASTER_PRED_CONSOLIDATED
        SET ' + concat('PRED_',CAST(@PQRT AS VARCHAR(10))) + ' = I.GAVE_FLG
		from 
		#QRT_PREDICTION I,
	    #MASTER_PRED_CONSOLIDATED H
        where
        I.PR_DONOR_ID = H.PR_DONOR_ID'

--THE EXECUTE() CODE WILL ALLOW THE TEXT IN @SQL2 AND @SQL3 TO BE INTERPRETED AS REAL CODE NOT JUST TEXT
EXECUTE (@SQL2)
EXECUTE (@SQL3)

--drop table #QRT_PREDICTION
--EXTERNAL LOOP 6: 
--INTRODUCE CODE TO SAVE THE PATTERN INFORMATION FROM #QRT_PREDICTION INTO THE MASTER_PATT TABLE #MASTER_PATT_CONSOLIDATED
--DECLARE @PQRT INTEGER;         -- (1) declares the temporal variable count as an integer 
--SET @PQRT = 1 ;     		   -- (2) set the initial value of the temporal value @PQRT as 1
--DECLARE @PEND INTEGER;         -- (3) declares the variable @PEND 
--SET @PEND = 1 ; 
--THE VARIABLES @SQL4 AND @SQL5 ARE DECLARED NVARCHAR(MAX) MEANING TEXT OF THE CODE WITH EXTENSION AS REQUIRED
--@SQL1 WILL DEAL WITH THE ALTER TABLE CODE (ADD NEW COLUMN TO #INITIAL)
--@SQL2 WILL CORRESPOND TO THE UPDATE OF THE TABLE (ADD CONTENT OF #SEQ_data TO NEW COLUMN IN #INITIAL

    DECLARE @SQL4 NVARCHAR(MAX)   
	DECLARE @SQL5 NVARCHAR(MAX)   

-- First statement add the new row to THE #MASTER_PRED_CONSOLIDATED Table 
-- the name of the column is PRED_1 for the first iteration, PRED_2 for the second and so on ...  it will stop at PRED_i , 

    SET @SQL4 = 'ALTER TABLE #MASTER_PATT_CONSOLIDATED
        ADD ' + concat('PATT_',CAST(@PQRT AS VARCHAR(10))) + ' INTEGER'

-- Second Stement Write the result of the iteration from #SEQ_data into #INITIAL_data into the correct column PRED_i for iteration i

   SET @SQL5 = 'UPDATE #MASTER_PATT_CONSOLIDATED
        SET ' + concat('PATT_',CAST(@PQRT AS VARCHAR(10))) + ' = I.PATTERN_NUMBER
		from 
		#QRT_PREDICTION I,
	    #MASTER_PATT_CONSOLIDATED H
        where
        I.PR_DONOR_ID = H.PR_DONOR_ID'

--THE EXECUTE() CODE WILL ALLOW THE TEXT IN @SQL4 AND @SQL5 TO BE INTERPRETED AS REAL CODE NOT JUST TEXT
EXECUTE (@SQL4)
EXECUTE (@SQL5)


--FINALLY THE #QRT_PREDICTION TABLE IS DROPPED SO IT CAN BE RE-USED FOR THE NEXT QUARTER (EXTERNAL LOOP ITERATION) PATTERN IN THE ITERATION 
drop table #QRT_PREDICTION
drop table #Patt65_CONSOL
drop table #PATTERN_data
--drop table #Patt65_CONSOL	

--PRINT @PQRT;                  -- (5) print instruction ... can be way more complex
SET @PQRT = @PQRT+1;          -- (6) the @variable @PATT is set up as @PATT +1 in order to allow for the next iteration 
END;						  -- (7) this instruction tells the machine to end the loop 

select * 
INTO [SPSS_Sandbox].[dbo].[LTDV_PL_FY21_PL_INCOME_FLG_80QRT_DEMO]
from #MASTER_PRED_CONSOLIDATED
ORDER BY 1

select * 
INTO [SPSS_Sandbox].[dbo].[LTDV_PL_FY21_PL_PATTERN_N_80QRT]
from #MASTER_PATT_CONSOLIDATED
order by 1

--(12647 rows affected)
--Completion time: 2022-11-08T18:01:35.2759103-05:00
--==============================================================================================================
--==============================================================================================================
--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP
--==============================================================================================================

drop table  #MASTER_PRED_CONSOLIDATED
drop table  #MASTER_PATT_CONSOLIDATED

select * from [SPSS_Sandbox].[dbo].[LTDV_PL_FY21_PL_INCOME_FLG_80QRT_DEMO] 

select * from [SPSS_Sandbox].[dbo].[LTDV_PL_FY21_PL_PATTERN_N_80QRT] order by 1

--ok!! ALgorithm work beautifully!


--=========================================================================================================
--PLEDGERS AS SINGLE GIFT GIVERS
--=======================================================================================================
--PLEDGER_categories and labels in 
--select * from [SPSS_Sandbox].[dbo].[LTDV_SCRIPT_FY21_25072022_PAST_INCOME_TNR_DEMO] where DNR_LABEL = 'PLEDGER'

--==================================================================================================================================================
--NOT PART OF THE LOOP 
--===================================================================================================================================================
--TABLE 1: HISTORICAL GIVING FLAG PER DONOR FOR last 5 quarters pattern plus prediction AND RELEVANT PLEDGER CATEGORY ... 
--===================================================================================================================================================
--Needs to define last 4 quarters data available ... 

select dn.Donation_Donor_Id as 'PR_DONOR_ID'
, a.DNR_CAT
, isnull(max(iif(cal.Tri_Fin_Ds_Lg = '2022 Q1' ,1,0)),0) as 'Q1'
, isnull(max(iif(cal.Tri_Fin_Ds_Lg = '2022 Q2' ,1,0)),0) as 'Q2'
, isnull(max(iif(cal.Tri_Fin_Ds_Lg = '2022 Q3' ,1,0)),0) as 'Q3'
, isnull(max(iif(cal.Tri_Fin_Ds_Lg = '2022 Q4' ,1,0)),0) as 'Q4'
into #MASTER_PRED_CONSOLIDATED
from ADOBE.RAW.F_DONATION dn
join [BI_DW].[dbo].[D_Cal] cal on cal.Dt = dn.Donation_Deposit_Date
join [SPSS_Sandbox].[dbo].[LTDV_SCRIPT_FY21_25072022_PAST_INCOME_TNR_DEMO] a on a.DONATION_DONOR_ID = dn.Donation_Donor_Id
where dn.Donation_Adjustment_Reason_Sid = 0
and dn.Donation_Income_Type_for_Donor_Metrics_Sid IN (103,104,105,106,107,108)
and a.DNR_LABEL = 'PLEDGER'
	   --SCOPE OF THE QUERY must be ONE FISCAL YEAR 
and dn.Donation_Deposit_Date between '2021-10-01' and '2022-09-30'
group by dn.Donation_Donor_Id, a.DNR_CAT
--(1995 rows affected)
--Completion time: 2022-11-14T00:16:44.1729252-05:00

select * from #MASTER_PRED_CONSOLIDATED
--1995 records
--drop table  #MASTER_PRED_CONSOLIDATED 

--===================================================================================================================================================
--TABLE 2: #EX_ITER_ORDER -- SEQUENCE OF 4 QUARTERS TO BE PICKED FOR BUILDING THE RELEVANT 4 QUARTERS PATTERN PER SPONSOR 
--THE ROWS CORRESPOND TO THE PREDICTION NUMBER, THE COLUMNS TO THE GENERIC ROLE Q1, Q2,Q3,Q4 
--THIS GENERIC ROLE WILL BE PICKED AS PART OF THE EXTERNAL LOOP ... 
--===================================================================================================================================================
-- TABLE ALREADY CREATED : SEE CODE IN LINES 1176 1267

--===================================================================================================================================================
--TABLE 3: #MASTER_PATT_CONSOLIDATED -- NUMBER FROM 1 TO 64 THAT SUMMARIZES THE POSITION OF THE SPONSOR IN THE CATEGORY+Q1Q2Q3Q4 PATTERN PER QUARTER 
--THE INITIAL ROW ARE THE DONOR_IDS , THE COLUMNS WILL BE ADDED AS PART OF THE EXTERNAL LOOP  
--THIS GENERIC ROLE WILL BE PICKED AS PART OF THE EXTERNAL LOOP ... 
--===================================================================================================================================================

SELECT A.PR_DONOR_ID 
INTO #MASTER_PATT_CONSOLIDATED
from #MASTER_PRED_CONSOLIDATED A

--(1995 rows affected)
--Completion time: 2022-11-14T17:08:13.0270329-05:00

--DROP TABLE #MASTER_PATT_CONSOLIDATED

--SELECT * FROM  #MASTER_PATT_CONSOLIDATED ORDER BY 1

--SELECT PATT_1 , COUNT( PR_DONOR_ID) AS 'CN' FROM  #MASTER_PATT_CONSOLIDATED GROUP BY PATT_1 ORDER BY 1

SELECT * FROM #MASTER_PRED_CONSOLIDATED
--===========================================================================================================================================
-- END OF OUT-OF-THE-LOOP-TABLES
--===========================================================================================================================================

--===========================================================================================================================================
-- START OF EXTERNAL LOOP
--===========================================================================================================================================
--EXTERNAL LOOP 1 - WE NEED TO CREATE THE CODE FOR SELECTING THE RELEVANT SEQUENCE OF 4 QUARTERS TO BE PICKED FOR BUILDING PATTERN PER SPONSOR 
--CAREFUL WITH THE CREATION OF TEMPORAL TABLES AS PART OF THE EXEC()!!!
--tHE Exec command makes temp procedure from THE @sql qUEry and executes it. 
--When that procedure ends, all temp tables created in it will be dropped immediately, so NO access to the created table from outside current dynamic query. 
--use global temp table ##table_name to keep it alive ... MAKE SURE TO DELETE IT AFTER THE PROCESURE IS FINISHED ...
--again  dynamically, cannot use #TEMPTABLE because a local temp table will only exist in the scope of the query that defines it. 
--Using ## creates a global temp table which will be accessible outside the scope of the dynamic query.
--EXTERNAL LOOP 2 - now the table ##PATT_Q1Q2Q3Q4 is processed to create the patterns (65 Pattern_QRT + SP Category) per donor
-- GENERATE PATTERNS FOR EACH SPONSOR BASED ON THE INFORMATION OF QUERY1 Q1,Q2,Q3,Q4 1,0 INDICATORS AND THE DNR_CATEGORY 
--64 COMBINATIONS OF PATTERN AND DONOR CATEGORY AND An OTHER category in case there were some issue with data: 65 GROUPS
--===============================
--EXTERNAL LOOP 3 - now the table #Patt65 is left joined with [SPSS_Sandbox].[dbo].[LTDV_FY21_PRIOR_PROB_VECTORS_SP_PL_SG_DEMO] to create the counts patterns 
--(65 Pattern_QRT + SP Category) per donor and the number of people expected to give per pattern
-- GENERATE THE CONSOLIDATED MATRIX WITH 64 ROWS ONE PATTERN IN EACH ONE AND JOINS WITH [SPSS_Sandbox].[dbo].[LTDV_FY21_PRIOR_PROB_VECTORS_SP_PL_SG_DEMO]
-- THAT HAS THE PRIOR PROBABILITY PER PATTERN TO GENERATE THE NUMBER OF CASES PREDICTED AS GIVING IN THE QUARTER.
-- ==> Input for P(E) Number of donor in Pattern i / Divided by N_Donors
--, sum (b.Gave_in_2020_Q2) as 'N_Donors_Gave' -- ==> Input for P(H) Number of donors that Gave / Divided by N_Donors
--, convert(decimal(5,4) ,sum (b.Gave_in_2020_Q2)* 1./ count (b.Donation_Donor_Id)) as 'Prob_Gave_NextQ' -- ==> BAYESIAN ESTIMATION OF P(H/E)
--into [SPSS_Sandbox].[dbo].[LTSV_FY19_20_Prior_Prob_19_20]

DECLARE @PQRT INTEGER;         -- (1) declares the temporal variable count as an integer 
SET @PQRT = 1 ;     		   -- (2) set the initial value of the temporal value @PQRT as 1
DECLARE @PEND INTEGER;         -- (3) declares the variable @PEND 
SET @PEND = (select max(EXITER_N) from  #EX_ITER_ORDER) ;                --(select max(EXITER_N) from  #EX_ITER_ORDER) ;

--EXTERNAL LOOP: DEFINITION OF THE CONDITION FOR WHILE CONDITION THE ITERATION ==>

WHILE (@PQRT <=@PEND)
BEGIN
PRINT @PQRT

 CREATE TABLE #PATTERN_data (PR_DONOR_ID NUMERIC(8,0) , Pattern_Number INTEGER)

DECLARE @Q1 VARCHAR(7)
DECLARE @Q2 VARCHAR(7)
DECLARE @Q3 VARCHAR(7)
DECLARE @Q4 VARCHAR(7)
SET @Q1 = (SELECT R.Q1 FROM #EX_ITER_ORDER R WHERE R.EXITER_N = @PQRT)
SET @Q2 = (SELECT R.Q2 FROM #EX_ITER_ORDER R WHERE R.EXITER_N = @PQRT)
SET @Q3 = (SELECT R.Q3 FROM #EX_ITER_ORDER R WHERE R.EXITER_N = @PQRT)
SET @Q4 = (SELECT R.Q4 FROM #EX_ITER_ORDER R WHERE R.EXITER_N = @PQRT)

PRINT @Q1
PRINT @Q2
PRINT @Q3
PRINT @Q4

--DECLARE @SQL1 NVARCHAR(MAX)
DECLARE @SQL1 NVARCHAR(MAX)
SET @SQL1 = 'SELECT PR_DONOR_ID, DNR_CAT, CAST(CONCAT(' + CAST(@Q1 AS VARCHAR(10)) +  ',' + CAST(@Q2 AS VARCHAR(10)) +  ',' + CAST(@Q3 AS VARCHAR(10)) +  ',' + CAST(@Q4 AS VARCHAR(10))
 + ') AS VARCHAR(4)) AS ''QRT_PATT'' INTO #PATT_Q1Q2Q3Q4 FROM #MASTER_PRED_CONSOLIDATED

INSERT INTO #PATTERN_data (PR_DONOR_ID , Pattern_Number)
SELECT PR_DONOR_ID
, CASE 
WHEN QRT_PATT = ''1111'' and DNR_CAT = ''NEW'' then 1
WHEN QRT_PATT = ''1111'' and DNR_CAT = ''UPGRADED'' then 2
WHEN QRT_PATT = ''1111'' and DNR_CAT = ''REACTIVATED'' then 3
WHEN QRT_PATT = ''1111'' and DNR_CAT = ''OLD'' then 4
WHEN QRT_PATT = ''0111'' and DNR_CAT = ''NEW'' then 5
WHEN QRT_PATT = ''0111'' and DNR_CAT = ''UPGRADED'' then 6
WHEN QRT_PATT = ''0111'' and DNR_CAT = ''REACTIVATED'' then 7
WHEN QRT_PATT = ''0111'' and DNR_CAT = ''OLD'' then 8
WHEN QRT_PATT = ''1011'' and DNR_CAT = ''NEW'' then 9
WHEN QRT_PATT = ''1011'' and DNR_CAT = ''UPGRADED'' then 10
WHEN QRT_PATT = ''1011'' and DNR_CAT = ''REACTIVATED'' then 11
WHEN QRT_PATT = ''1011'' and DNR_CAT = ''OLD'' then 12
WHEN QRT_PATT = ''1101'' and DNR_CAT = ''NEW'' then 13
WHEN QRT_PATT = ''1101'' and DNR_CAT = ''UPGRADED'' then 14
WHEN QRT_PATT = ''1101'' and DNR_CAT = ''REACTIVATED'' then 15
WHEN QRT_PATT = ''1101'' and DNR_CAT = ''OLD'' then 16
WHEN QRT_PATT = ''0011'' and DNR_CAT = ''NEW'' then 17
WHEN QRT_PATT = ''0011'' and DNR_CAT = ''UPGRADED'' then 18
WHEN QRT_PATT = ''0011'' and DNR_CAT = ''REACTIVATED'' then 19
WHEN QRT_PATT = ''0011'' and DNR_CAT = ''OLD'' then 20
WHEN QRT_PATT = ''0101'' and DNR_CAT = ''NEW'' then 21
WHEN QRT_PATT = ''0101'' and DNR_CAT = ''UPGRADED'' then 22
WHEN QRT_PATT = ''0101'' and DNR_CAT = ''REACTIVATED'' then 23
WHEN QRT_PATT = ''0101'' and DNR_CAT = ''OLD'' then 24
WHEN QRT_PATT = ''1001'' and DNR_CAT = ''NEW'' then 25
WHEN QRT_PATT = ''1001'' and DNR_CAT = ''UPGRADED'' then 26
WHEN QRT_PATT = ''1001'' and DNR_CAT = ''REACTIVATED'' then 27
WHEN QRT_PATT = ''1001'' and DNR_CAT = ''OLD'' then 28
WHEN QRT_PATT = ''0001'' and DNR_CAT = ''NEW'' then 29
WHEN QRT_PATT = ''0001'' and DNR_CAT = ''UPGRADED'' then 30
WHEN QRT_PATT = ''0001'' and DNR_CAT = ''REACTIVATED'' then 31
WHEN QRT_PATT = ''0001'' and DNR_CAT = ''OLD'' then 32
WHEN QRT_PATT = ''1110'' and DNR_CAT = ''NEW'' then 33
WHEN QRT_PATT = ''1110'' and DNR_CAT = ''UPGRADED'' then 34
WHEN QRT_PATT = ''1110'' and DNR_CAT = ''REACTIVATED'' then 35
WHEN QRT_PATT = ''1110'' and DNR_CAT = ''OLD'' then 36
WHEN QRT_PATT = ''0110'' and DNR_CAT = ''NEW'' then 37
WHEN QRT_PATT = ''0110'' and DNR_CAT = ''UPGRADED'' then 38
WHEN QRT_PATT = ''0110'' and DNR_CAT = ''REACTIVATED'' then 39
WHEN QRT_PATT = ''0110'' and DNR_CAT = ''OLD'' then 40
WHEN QRT_PATT = ''1010'' and DNR_CAT = ''NEW'' then 41
WHEN QRT_PATT = ''1010'' and DNR_CAT = ''UPGRADED'' then 42
WHEN QRT_PATT = ''1010'' and DNR_CAT = ''REACTIVATED'' then 43
WHEN QRT_PATT = ''1010'' and DNR_CAT = ''OLD'' then 44
WHEN QRT_PATT = ''1100'' and DNR_CAT = ''NEW'' then 45
WHEN QRT_PATT = ''1100'' and DNR_CAT = ''UPGRADED'' then 46
WHEN QRT_PATT = ''1100'' and DNR_CAT = ''REACTIVATED'' then 47
WHEN QRT_PATT = ''1100'' and DNR_CAT = ''OLD'' then 48
WHEN QRT_PATT = ''0010'' and DNR_CAT = ''NEW'' then 49
WHEN QRT_PATT = ''0010'' and DNR_CAT = ''UPGRADED'' then 50
WHEN QRT_PATT = ''0010'' and DNR_CAT = ''REACTIVATED'' then 51
WHEN QRT_PATT = ''0010'' and DNR_CAT = ''OLD'' then 52
WHEN QRT_PATT = ''0100'' and DNR_CAT = ''NEW'' then 53
WHEN QRT_PATT = ''0100'' and DNR_CAT = ''UPGRADED'' then 54
WHEN QRT_PATT = ''0100'' and DNR_CAT = ''REACTIVATED'' then 55
WHEN QRT_PATT = ''0100'' and DNR_CAT = ''OLD'' then 56
WHEN QRT_PATT = ''1000'' and DNR_CAT = ''NEW'' then 57
WHEN QRT_PATT = ''1000'' and DNR_CAT = ''UPGRADED'' then 58
WHEN QRT_PATT = ''1000'' and DNR_CAT = ''REACTIVATED'' then 59
WHEN QRT_PATT = ''1000'' and DNR_CAT = ''OLD'' then 60
WHEN QRT_PATT = ''0000'' and DNR_CAT = ''NEW''  then 61
WHEN QRT_PATT = ''0000'' and DNR_CAT = ''UPGRADED'' then 62
WHEN QRT_PATT = ''0000'' and DNR_CAT = ''REACTIVATED'' then 63
WHEN QRT_PATT = ''0000'' and DNR_CAT = ''OLD'' then 64
else 65 end as ''Pattern_Number''
FROM #PATT_Q1Q2Q3Q4'

EXECUTE (@SQL1)

--SELECT  * FROM #PATTERN_data
--select TOP 10 * from [SPSS_Sandbox].[dbo].[LTDV_FY21_PRIOR_PROB_VECTORS_SP_PL_SG_DEMO]

select a.Pattern_Number
, count (b.PR_DONOR_ID) as 'N_Donors'
, a.Prob_PLR_Gave_SG_NextQ 
, round(a.Prob_PLR_Gave_SG_NextQ*count (b.PR_DONOR_ID),0) as 'Y_Give'
, count (b.PR_DONOR_ID) - round(a.Prob_PLR_Gave_SG_NextQ*count (b.PR_DONOR_ID),0) as 'N_Give' ----> calculation just uses relevant SP Prob vector 
into #Patt65_CONSOL
FROM [SPSS_Sandbox].[dbo].[LTDV_FY21_PRIOR_PROB_VECTORS_SP_PL_SG_DEMO] a   --==> this table has the information on patterns 
left join  #PATTERN_data b on a.Pattern_Number = b.Pattern_Number
group by a.Pattern_Number , a.Prob_PLR_Gave_SG_NextQ
order by 1

--SELECT * FROM #Patt65_CONSOL ORDER BY 1

--NOW IT IS THE TIME TO INSERT THE INTERNAL LOOP THAT WILL PROCESS EVERYONE OF THE 64 GROUPS IN ORDER TO CREATE A FLAG give/didNOT give PER DONOR PER QUARTER
--THE INTERNAL LOOP RUNS 64 TIMES PER QUARTER.
---BUT BEFORE THAT

--EXTERNAL LOOP 4: table #QRT_PREDICTION THAT STORES THE PREDICTION FOR ALL DONORS FOR THE QUARTER
--The table #QRT_PREDICTION will store all the quarter results coming from the internal loop 
--Must be created OUT of both the external and internal loop since the table is created just once
--and must BE just populated at the end of every external loop iteration 

CREATE TABLE #QRT_PREDICTION (PR_DONOR_ID NUMERIC(8,0) , PATTERN_NUMBER INTEGER , GAVE_FLG INTEGER )
--SELECT * FROM #QRT_PREDICTION
--DROP TABLE #QRT_PREDICTION
--==========================================================================================================
--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP
--==========================================================================================================
--==========================================================================================================
--==========================================================================================================

--INTERNAL LOOP 1:             
--Basic loop to populate the file of sponsors randomly selected as givers during the period

DECLARE @PATT INTEGER;        -- (1) declares the temporal variable count as an integer
SET @PATT = 1 ;               -- (2) set the initial value of the temporal value @PATT as 1
DECLARE @END INTEGER;         -- (3) SET THE VARIABLE @END THAT DEFINES THE TOTAL NUMBER OF ITERATIONS
SET @END = (SELECT MAX(Y.Pattern_Number) FROM #Patt65_CONSOL Y) 

--INTERNAL LOOP 2: SETTING THE LOOP CONDITION ==> ITERATIONS GO FROM 1 TO 64
WHILE (@PATT <=@END)         -- (3) WHILE defines the loop the code will be executed until @PATT  reaches the value 64 NO SEMICOLON!!!

--INTERNAL LOOP 3: BEGIN ===> END DEFINE THE SPACE FOR THE CODE THAT WILL RUN IN ITERATIONS      
BEGIN	                     -- (4) this defines where the code the loop applies to starts
							-- (5) this is the body of code 
--INTERNAL LOOP 4: 
--FIRST DECLARE AND DEFINE THE VARIABLE @NY THAT IS THE NUMBER OF SPONSORS THAT ARE PREDICTED WILL GIVE IN CATEGORY @PATT
DECLARE @NY INTEGER;
SET @NY = (SELECT Y.Y_GIVE FROM #Patt65_CONSOL Y WHERE Y.Pattern_Number = @PATT)

--INTERNAL LOOP 5: CREATING THE SCORES FOR A PARTICULAR PATTERN @NY AND STORE IT INTO #ONE_PATT
-- #ONE_PATT creates and stores the values for the current loop pattern -- this code stores the randomly selected @NY as GAVE_FLG = 1
select top (@NY) a.PR_DONOR_ID , a.Pattern_Number, 1 AS 'GAVE_FLG' into #ONE_PATT from #PATTERN_data a where a.Pattern_Number = @PATT  ORDER BY NEWID()

-- #ONE_PATT creates and stores the values for the current loop pattern -- this code stores the not randomly selected @NY as GAVE_FLG = 0
insert into #ONE_PATT
select b.PR_DONOR_ID , b.Pattern_Number, 0 AS 'GAVE_FLG' from #PATTERN_data b 
left join #ONE_PATT a on a.PR_DONOR_ID = b.PR_DONOR_ID  
where a.PR_DONOR_ID is null and b.pattern_number = @PATT

--INTERNAL LOOP 6: #ONE_PATT RESULT FROM PATTERN ITERATION STORED IN #QRT_PREDICTION ON A CUMULATIVE BASIS
--now the result of #ONE_PATT which is just for one pattern is stored into the more formal #QRT_Prediction that stores ALL pattern results
insert into #QRT_PREDICTION (PR_DONOR_ID, PATTERN_NUMBER, GAVE_FLG)
select A.PR_DONOR_ID , a.Pattern_Number, a.GAVE_FLG from #ONE_PATT a

--FINALLY THE #ONE_PATT ID DROPPED SO IT CAN BE RE-USED FOR THE NEXT PATTERN IN THE ITERATION 
drop table #ONE_PATT
	

PRINT @PATT;                  -- (5) print instruction ... can be way more complex
SET @PATT = @PATT+1;          -- (6) the @variable @PATT is set up as @PATT +1 in order to allow for the next iteration 
END;						  -- (7) this instruction tells the machine to end the loop 

--by the END all donors must have a GAVE_FLG as 1, 0 as they were randomly selected in their respective Pattern (1 to 64)
--END OF INTERNAL LOOP WHAT COMES NEXT IS PROCESSED AS PART OF THE EXTERNAL LOOP ONLY AS MANY TIMES AS PREDICTION QUSRTERS ARE DEFINED
--==============================================================================================================
--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP
--==============================================================================================================
--DECLARE @PQRT INTEGER;         -- (1) declares the temporal variable count as an integer 
--SET @PQRT = 1 ;     		   -- (2) set the initial value of the temporal value @PQRT as 1
--DECLARE @PEND INTEGER;         -- (3) declares the variable @PEND 
--SET @PEND = 1 ; 
--EXTERNAL LOOP 5: 
--INTRODUCE CODE TO SAVE THE gAVE/dID NOT GIVE PREDICTION FROM #QRT_PREDICTION INTO THE MASTER TABLE #MASTER_PRED_CONSOLIDATED

--THE VARIABLES @SQL2 AND @SQL3 ARE DECLARED NVARCHAR(MAX) MEANING TEXT OF THE CODE WITH EXTENSION AS REQUIRED
--@SQL1 WILL DEAL WITH THE ALTER TABLE CODE (ADD NEW COLUMN TO #INITIAL)
--@SQL2 WILL CORRESPOND TO THE UPDATE OF THE TABLE (ADD CONTENT OF #SEQ_data TO NEW COLUMN IN #INITIAL

    DECLARE @SQL2 NVARCHAR(MAX)   
	DECLARE @SQL3 NVARCHAR(MAX)   

-- First statement add the new row to THE #MASTER_PRED_CONSOLIDATED Table 
-- the name of the column is PRED_1 for the first iteration, PRED_2 for the second and so on ...  it will stop at PRED_i , 

    SET @SQL2 = 'ALTER TABLE #MASTER_PRED_CONSOLIDATED
        ADD ' + concat('PRED_',CAST(@PQRT AS VARCHAR(10))) + ' INTEGER'

-- Second Stement Write the result of the iteration from #SEQ_data into #INITIAL_data into the correct column PRED_i for iteration i

   SET @SQL3 = 'UPDATE #MASTER_PRED_CONSOLIDATED
        SET ' + concat('PRED_',CAST(@PQRT AS VARCHAR(10))) + ' = I.GAVE_FLG
		from 
		#QRT_PREDICTION I,
	    #MASTER_PRED_CONSOLIDATED H
        where
        I.PR_DONOR_ID = H.PR_DONOR_ID'

--THE EXECUTE() CODE WILL ALLOW THE TEXT IN @SQL2 AND @SQL3 TO BE INTERPRETED AS REAL CODE NOT JUST TEXT
EXECUTE (@SQL2)
EXECUTE (@SQL3)

--drop table #QRT_PREDICTION
--EXTERNAL LOOP 6: 
--INTRODUCE CODE TO SAVE THE PATTERN INFORMATION FROM #QRT_PREDICTION INTO THE MASTER_PATT TABLE #MASTER_PATT_CONSOLIDATED
--DECLARE @PQRT INTEGER;         -- (1) declares the temporal variable count as an integer 
--SET @PQRT = 1 ;     		   -- (2) set the initial value of the temporal value @PQRT as 1
--DECLARE @PEND INTEGER;         -- (3) declares the variable @PEND 
--SET @PEND = 1 ; 
--THE VARIABLES @SQL4 AND @SQL5 ARE DECLARED NVARCHAR(MAX) MEANING TEXT OF THE CODE WITH EXTENSION AS REQUIRED
--@SQL1 WILL DEAL WITH THE ALTER TABLE CODE (ADD NEW COLUMN TO #INITIAL)
--@SQL2 WILL CORRESPOND TO THE UPDATE OF THE TABLE (ADD CONTENT OF #SEQ_data TO NEW COLUMN IN #INITIAL

    DECLARE @SQL4 NVARCHAR(MAX)   
	DECLARE @SQL5 NVARCHAR(MAX)   

-- First statement add the new row to THE #MASTER_PRED_CONSOLIDATED Table 
-- the name of the column is PRED_1 for the first iteration, PRED_2 for the second and so on ...  it will stop at PRED_i , 

    SET @SQL4 = 'ALTER TABLE #MASTER_PATT_CONSOLIDATED
        ADD ' + concat('PATT_',CAST(@PQRT AS VARCHAR(10))) + ' INTEGER'

-- Second Stement Write the result of the iteration from #SEQ_data into #INITIAL_data into the correct column PRED_i for iteration i

   SET @SQL5 = 'UPDATE #MASTER_PATT_CONSOLIDATED
        SET ' + concat('PATT_',CAST(@PQRT AS VARCHAR(10))) + ' = I.PATTERN_NUMBER
		from 
		#QRT_PREDICTION I,
	    #MASTER_PATT_CONSOLIDATED H
        where
        I.PR_DONOR_ID = H.PR_DONOR_ID'

--THE EXECUTE() CODE WILL ALLOW THE TEXT IN @SQL4 AND @SQL5 TO BE INTERPRETED AS REAL CODE NOT JUST TEXT
EXECUTE (@SQL4)
EXECUTE (@SQL5)


--FINALLY THE #QRT_PREDICTION TABLE IS DROPPED SO IT CAN BE RE-USED FOR THE NEXT QUARTER (EXTERNAL LOOP ITERATION) PATTERN IN THE ITERATION 
drop table #QRT_PREDICTION
drop table #Patt65_CONSOL
drop table #PATTERN_data
--drop table #Patt65_CONSOL	

--PRINT @PQRT;                  -- (5) print instruction ... can be way more complex
SET @PQRT = @PQRT+1;          -- (6) the @variable @PATT is set up as @PATT +1 in order to allow for the next iteration 
END;						  -- (7) this instruction tells the machine to end the loop 

select * 
INTO [SPSS_Sandbox].[dbo].[LTDV_PL_FY21_SG_INCOME_FLG_80QRT_DEMO]
from #MASTER_PRED_CONSOLIDATED
ORDER BY 1

select * 
INTO [SPSS_Sandbox].[dbo].[LTDV_PL_FY21_SG_PATTERN_N_80QRT]
from #MASTER_PATT_CONSOLIDATED
order by 1

--(1995 rows affected)
--Completion time: 2022-11-08T18:01:35.2759103-05:00
--==============================================================================================================
--==============================================================================================================
--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP
--==============================================================================================================

drop table  #MASTER_PRED_CONSOLIDATED
drop table  #MASTER_PATT_CONSOLIDATED

select * from [SPSS_Sandbox].[dbo].[LTDV_PL_FY21_SG_INCOME_FLG_80QRT_DEMO] 

select * from [SPSS_Sandbox].[dbo].[LTDV_PL_FY21_SG_PATTERN_N_80QRT] order by 1

--ok!! ALgorithm work beautifully!



--===============================================================================================================

--======================================================================================================
--SINGLE GIFT GIVERS: BAYESIAN LOOP
--=======================================================================================================
--SINGLE GIVERS_categories and labels in 
--select * from [SPSS_Sandbox].[dbo].[LTDV_SCRIPT_FY21_25072022_PAST_INCOME_TNR_DEMO] where DNR_LABEL = 'SINGLE_GIVER'

--==================================================================================================================================================
--NOT PART OF THE LOOP 
--===================================================================================================================================================
--TABLE 1: HISTORICAL GIVING FLAG PER DONOR FOR last 5 quarters pattern plus prediction AND RELEVANT PLEDGER CATEGORY ... 
--===================================================================================================================================================
--Needs to define last 4 quarters data available ... 

select dn.Donation_Donor_Id as 'PR_DONOR_ID'
, a.DNR_CAT
, isnull(max(iif(cal.Tri_Fin_Ds_Lg = '2022 Q1' ,1,0)),0) as 'Q1'
, isnull(max(iif(cal.Tri_Fin_Ds_Lg = '2022 Q2' ,1,0)),0) as 'Q2'
, isnull(max(iif(cal.Tri_Fin_Ds_Lg = '2022 Q3' ,1,0)),0) as 'Q3'
, isnull(max(iif(cal.Tri_Fin_Ds_Lg = '2022 Q4' ,1,0)),0) as 'Q4'
into #MASTER_PRED_CONSOLIDATED
from ADOBE.RAW.F_DONATION dn
join [BI_DW].[dbo].[D_Cal] cal on cal.Dt = dn.Donation_Deposit_Date
join [SPSS_Sandbox].[dbo].[LTDV_SCRIPT_FY21_25072022_PAST_INCOME_TNR_DEMO] a on a.DONATION_DONOR_ID = dn.Donation_Donor_Id
where dn.Donation_Adjustment_Reason_Sid = 0
and dn.Donation_Income_Type_for_Donor_Metrics_Sid IN (103,104,105,106,107,108)
and a.DNR_LABEL = 'SINGLE_GIVER'
	   --SCOPE OF THE QUERY must be ONE FISCAL YEAR 
and dn.Donation_Deposit_Date between '2021-10-01' and '2022-09-30'
group by dn.Donation_Donor_Id, a.DNR_CAT
--(20359 rows affected)
--Completion time: 2022-11-08T17:04:52.2247279-05:00

select * from #MASTER_PRED_CONSOLIDATED
--20359 records
--drop table  #MASTER_PRED_CONSOLIDATED 

--===================================================================================================================================================
--TABLE 2: #EX_ITER_ORDER -- SEQUENCE OF 4 QUARTERS TO BE PICKED FOR BUILDING THE RELEVANT 4 QUARTERS PATTERN PER SPONSOR 
--THE ROWS CORRESPOND TO THE PREDICTION NUMBER, THE COLUMNS TO THE GENERIC ROLE Q1, Q2,Q3,Q4 
--THIS GENERIC ROLE WILL BE PICKED AS PART OF THE EXTERNAL LOOP ... 
--===================================================================================================================================================
-- TABLE ALREADY CREATED : SEE CODE IN LINES 1176 1267

--===================================================================================================================================================
--TABLE 3: #MASTER_PATT_CONSOLIDATED -- NUMBER FROM 1 TO 64 THAT SUMMARIZES THE POSITION OF THE SPONSOR IN THE CATEGORY+Q1Q2Q3Q4 PATTERN PER QUARTER 
--THE INITIAL ROW ARE THE DONOR_IDS , THE COLUMNS WILL BE ADDED AS PART OF THE EXTERNAL LOOP  
--THIS GENERIC ROLE WILL BE PICKED AS PART OF THE EXTERNAL LOOP ... 
--===================================================================================================================================================

SELECT A.PR_DONOR_ID 
INTO #MASTER_PATT_CONSOLIDATED
from #MASTER_PRED_CONSOLIDATED A

--(20359 rows affected)
--Completion time: 2022-11-08T18:22:11.5034560-05:00

--DROP TABLE #MASTER_PATT_CONSOLIDATED

--SELECT * FROM  #MASTER_PATT_CONSOLIDATED ORDER BY 1

--SELECT PATT_1 , COUNT( PR_DONOR_ID) AS 'CN' FROM  #MASTER_PATT_CONSOLIDATED GROUP BY PATT_1 ORDER BY 1

SELECT * FROM #MASTER_PRED_CONSOLIDATED
--===========================================================================================================================================
-- END OF OUT-OF-THE-LOOP-TABLES
--===========================================================================================================================================

--===========================================================================================================================================
-- START OF EXTERNAL LOOP
--===========================================================================================================================================
--EXTERNAL LOOP 1 - WE NEED TO CREATE THE CODE FOR SELECTING THE RELEVANT SEQUENCE OF 4 QUARTERS TO BE PICKED FOR BUILDING PATTERN PER SPONSOR 
--CAREFUL WITH THE CREATION OF TEMPORAL TABLES AS PART OF THE EXEC()!!!
--tHE Exec command makes temp procedure from THE @sql qUEry and executes it. 
--When that procedure ends, all temp tables created in it will be dropped immediately, so NO access to the created table from outside current dynamic query. 
--use global temp table ##table_name to keep it alive ... MAKE SURE TO DELETE IT AFTER THE PROCESURE IS FINISHED ...
--again  dynamically, cannot use #TEMPTABLE because a local temp table will only exist in the scope of the query that defines it. 
--Using ## creates a global temp table which will be accessible outside the scope of the dynamic query.
--EXTERNAL LOOP 2 - now the table ##PATT_Q1Q2Q3Q4 is processed to create the patterns (65 Pattern_QRT + SP Category) per donor
-- GENERATE PATTERNS FOR EACH SPONSOR BASED ON THE INFORMATION OF QUERY1 Q1,Q2,Q3,Q4 1,0 INDICATORS AND THE DNR_CATEGORY 
--64 COMBINATIONS OF PATTERN AND DONOR CATEGORY AND An OTHER category in case there were some issue with data: 65 GROUPS
--===============================
--EXTERNAL LOOP 3 - now the table #Patt65 is left joined with [SPSS_Sandbox].[dbo].[LTDV_FY21_PRIOR_PROB_VECTORS_SP_PL_SG_DEMO] to create the counts patterns 
--(65 Pattern_QRT + SP Category) per donor and the number of people expected to give per pattern
-- GENERATE THE CONSOLIDATED MATRIX WITH 64 ROWS ONE PATTERN IN EACH ONE AND JOINS WITH [SPSS_Sandbox].[dbo].[LTDV_FY21_PRIOR_PROB_VECTORS_SP_PL_SG_DEMO]
-- THAT HAS THE PRIOR PROBABILITY PER PATTERN TO GENERATE THE NUMBER OF CASES PREDICTED AS GIVING IN THE QUARTER.
-- ==> Input for P(E) Number of donor in Pattern i / Divided by N_Donors
--, sum (b.Gave_in_2020_Q2) as 'N_Donors_Gave' -- ==> Input for P(H) Number of donors that Gave / Divided by N_Donors
--, convert(decimal(5,4) ,sum (b.Gave_in_2020_Q2)* 1./ count (b.Donation_Donor_Id)) as 'Prob_Gave_NextQ' -- ==> BAYESIAN ESTIMATION OF P(H/E)
--into [SPSS_Sandbox].[dbo].[LTSV_FY19_20_Prior_Prob_19_20]

DECLARE @PQRT INTEGER;         -- (1) declares the temporal variable count as an integer 
SET @PQRT = 1 ;     		   -- (2) set the initial value of the temporal value @PQRT as 1
DECLARE @PEND INTEGER;         -- (3) declares the variable @PEND 
SET @PEND = (select max(EXITER_N) from  #EX_ITER_ORDER) ;                --(select max(EXITER_N) from  #EX_ITER_ORDER) ;

--EXTERNAL LOOP: DEFINITION OF THE CONDITION FOR WHILE CONDITION THE ITERATION ==>

WHILE (@PQRT <=@PEND)
BEGIN
PRINT @PQRT

 CREATE TABLE #PATTERN_data (PR_DONOR_ID NUMERIC(8,0) , Pattern_Number INTEGER)

DECLARE @Q1 VARCHAR(7)
DECLARE @Q2 VARCHAR(7)
DECLARE @Q3 VARCHAR(7)
DECLARE @Q4 VARCHAR(7)
SET @Q1 = (SELECT R.Q1 FROM #EX_ITER_ORDER R WHERE R.EXITER_N = @PQRT)
SET @Q2 = (SELECT R.Q2 FROM #EX_ITER_ORDER R WHERE R.EXITER_N = @PQRT)
SET @Q3 = (SELECT R.Q3 FROM #EX_ITER_ORDER R WHERE R.EXITER_N = @PQRT)
SET @Q4 = (SELECT R.Q4 FROM #EX_ITER_ORDER R WHERE R.EXITER_N = @PQRT)

PRINT @Q1
PRINT @Q2
PRINT @Q3
PRINT @Q4

--DECLARE @SQL1 NVARCHAR(MAX)
DECLARE @SQL1 NVARCHAR(MAX)
SET @SQL1 = 'SELECT PR_DONOR_ID, DNR_CAT, CAST(CONCAT(' + CAST(@Q1 AS VARCHAR(10)) +  ',' + CAST(@Q2 AS VARCHAR(10)) +  ',' + CAST(@Q3 AS VARCHAR(10)) +  ',' + CAST(@Q4 AS VARCHAR(10))
 + ') AS VARCHAR(4)) AS ''QRT_PATT'' INTO #PATT_Q1Q2Q3Q4 FROM #MASTER_PRED_CONSOLIDATED

INSERT INTO #PATTERN_data (PR_DONOR_ID , Pattern_Number)
SELECT PR_DONOR_ID
, CASE 
WHEN QRT_PATT = ''1111'' and DNR_CAT = ''NEW'' then 1
WHEN QRT_PATT = ''1111'' and DNR_CAT = ''UPGRADED'' then 2
WHEN QRT_PATT = ''1111'' and DNR_CAT = ''REACTIVATED'' then 3
WHEN QRT_PATT = ''1111'' and DNR_CAT = ''OLD'' then 4
WHEN QRT_PATT = ''0111'' and DNR_CAT = ''NEW'' then 5
WHEN QRT_PATT = ''0111'' and DNR_CAT = ''UPGRADED'' then 6
WHEN QRT_PATT = ''0111'' and DNR_CAT = ''REACTIVATED'' then 7
WHEN QRT_PATT = ''0111'' and DNR_CAT = ''OLD'' then 8
WHEN QRT_PATT = ''1011'' and DNR_CAT = ''NEW'' then 9
WHEN QRT_PATT = ''1011'' and DNR_CAT = ''UPGRADED'' then 10
WHEN QRT_PATT = ''1011'' and DNR_CAT = ''REACTIVATED'' then 11
WHEN QRT_PATT = ''1011'' and DNR_CAT = ''OLD'' then 12
WHEN QRT_PATT = ''1101'' and DNR_CAT = ''NEW'' then 13
WHEN QRT_PATT = ''1101'' and DNR_CAT = ''UPGRADED'' then 14
WHEN QRT_PATT = ''1101'' and DNR_CAT = ''REACTIVATED'' then 15
WHEN QRT_PATT = ''1101'' and DNR_CAT = ''OLD'' then 16
WHEN QRT_PATT = ''0011'' and DNR_CAT = ''NEW'' then 17
WHEN QRT_PATT = ''0011'' and DNR_CAT = ''UPGRADED'' then 18
WHEN QRT_PATT = ''0011'' and DNR_CAT = ''REACTIVATED'' then 19
WHEN QRT_PATT = ''0011'' and DNR_CAT = ''OLD'' then 20
WHEN QRT_PATT = ''0101'' and DNR_CAT = ''NEW'' then 21
WHEN QRT_PATT = ''0101'' and DNR_CAT = ''UPGRADED'' then 22
WHEN QRT_PATT = ''0101'' and DNR_CAT = ''REACTIVATED'' then 23
WHEN QRT_PATT = ''0101'' and DNR_CAT = ''OLD'' then 24
WHEN QRT_PATT = ''1001'' and DNR_CAT = ''NEW'' then 25
WHEN QRT_PATT = ''1001'' and DNR_CAT = ''UPGRADED'' then 26
WHEN QRT_PATT = ''1001'' and DNR_CAT = ''REACTIVATED'' then 27
WHEN QRT_PATT = ''1001'' and DNR_CAT = ''OLD'' then 28
WHEN QRT_PATT = ''0001'' and DNR_CAT = ''NEW'' then 29
WHEN QRT_PATT = ''0001'' and DNR_CAT = ''UPGRADED'' then 30
WHEN QRT_PATT = ''0001'' and DNR_CAT = ''REACTIVATED'' then 31
WHEN QRT_PATT = ''0001'' and DNR_CAT = ''OLD'' then 32
WHEN QRT_PATT = ''1110'' and DNR_CAT = ''NEW'' then 33
WHEN QRT_PATT = ''1110'' and DNR_CAT = ''UPGRADED'' then 34
WHEN QRT_PATT = ''1110'' and DNR_CAT = ''REACTIVATED'' then 35
WHEN QRT_PATT = ''1110'' and DNR_CAT = ''OLD'' then 36
WHEN QRT_PATT = ''0110'' and DNR_CAT = ''NEW'' then 37
WHEN QRT_PATT = ''0110'' and DNR_CAT = ''UPGRADED'' then 38
WHEN QRT_PATT = ''0110'' and DNR_CAT = ''REACTIVATED'' then 39
WHEN QRT_PATT = ''0110'' and DNR_CAT = ''OLD'' then 40
WHEN QRT_PATT = ''1010'' and DNR_CAT = ''NEW'' then 41
WHEN QRT_PATT = ''1010'' and DNR_CAT = ''UPGRADED'' then 42
WHEN QRT_PATT = ''1010'' and DNR_CAT = ''REACTIVATED'' then 43
WHEN QRT_PATT = ''1010'' and DNR_CAT = ''OLD'' then 44
WHEN QRT_PATT = ''1100'' and DNR_CAT = ''NEW'' then 45
WHEN QRT_PATT = ''1100'' and DNR_CAT = ''UPGRADED'' then 46
WHEN QRT_PATT = ''1100'' and DNR_CAT = ''REACTIVATED'' then 47
WHEN QRT_PATT = ''1100'' and DNR_CAT = ''OLD'' then 48
WHEN QRT_PATT = ''0010'' and DNR_CAT = ''NEW'' then 49
WHEN QRT_PATT = ''0010'' and DNR_CAT = ''UPGRADED'' then 50
WHEN QRT_PATT = ''0010'' and DNR_CAT = ''REACTIVATED'' then 51
WHEN QRT_PATT = ''0010'' and DNR_CAT = ''OLD'' then 52
WHEN QRT_PATT = ''0100'' and DNR_CAT = ''NEW'' then 53
WHEN QRT_PATT = ''0100'' and DNR_CAT = ''UPGRADED'' then 54
WHEN QRT_PATT = ''0100'' and DNR_CAT = ''REACTIVATED'' then 55
WHEN QRT_PATT = ''0100'' and DNR_CAT = ''OLD'' then 56
WHEN QRT_PATT = ''1000'' and DNR_CAT = ''NEW'' then 57
WHEN QRT_PATT = ''1000'' and DNR_CAT = ''UPGRADED'' then 58
WHEN QRT_PATT = ''1000'' and DNR_CAT = ''REACTIVATED'' then 59
WHEN QRT_PATT = ''1000'' and DNR_CAT = ''OLD'' then 60
WHEN QRT_PATT = ''0000'' and DNR_CAT = ''NEW''  then 61
WHEN QRT_PATT = ''0000'' and DNR_CAT = ''UPGRADED'' then 62
WHEN QRT_PATT = ''0000'' and DNR_CAT = ''REACTIVATED'' then 63
WHEN QRT_PATT = ''0000'' and DNR_CAT = ''OLD'' then 64
else 65 end as ''Pattern_Number''
FROM #PATT_Q1Q2Q3Q4'

EXECUTE (@SQL1)

--SELECT  * FROM #PATTERN_data
--select * from [SPSS_Sandbox].[dbo].[LTDV_FY21_PRIOR_PROB_VECTORS_SP_PL_SG_DEMO]

--select a.Prob_SGR_Gave_SG_NextQ from [SPSS_Sandbox].[dbo].[LTDV_FY21_PRIOR_PROB_VECTORS_SP_PL_SG_DEMO] a

select a.Pattern_Number
, count (b.PR_DONOR_ID) as 'N_Donors'
, a.Prob_SGR_Gave_SG_NextQ 
, round(a.Prob_SGR_Gave_SG_NextQ*count (b.PR_DONOR_ID),0) as 'Y_Give'
, count (b.PR_DONOR_ID) - round(a.Prob_SGR_Gave_SG_NextQ*count (b.PR_DONOR_ID),0) as 'N_Give' ----> calculation just uses relevant SP Prob vector 
into #Patt65_CONSOL
FROM [SPSS_Sandbox].[dbo].[LTDV_FY21_PRIOR_PROB_VECTORS_SP_PL_SG_DEMO] a   --==> this table has the information on patterns 
left join  #PATTERN_data b on a.Pattern_Number = b.Pattern_Number
group by a.Pattern_Number , a.Prob_SGR_Gave_SG_NextQ
order by 1

--SELECT * FROM #Patt65_CONSOL ORDER BY 1

--NOW IT IS THE TIME TO INSERT THE INTERNAL LOOP THAT WILL PROCESS EVERYONE OF THE 64 GROUPS IN ORDER TO CREATE A FLAG give/didNOT give PER DONOR PER QUARTER
--THE INTERNAL LOOP RUNS 64 TIMES PER QUARTER.
---BUT BEFORE THAT

--EXTERNAL LOOP 4: table #QRT_PREDICTION THAT STORES THE PREDICTION FOR ALL DONORS FOR THE QUARTER
--The table #QRT_PREDICTION will store all the quarter results coming from the internal loop 
--Must be created OUT of both the external and internal loop since the table is created just once
--and must BE just populated at the end of every external loop iteration 

CREATE TABLE #QRT_PREDICTION (PR_DONOR_ID NUMERIC(8,0) , PATTERN_NUMBER INTEGER , GAVE_FLG INTEGER )
--SELECT * FROM #QRT_PREDICTION
--DROP TABLE #QRT_PREDICTION
--==========================================================================================================
--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP
--==========================================================================================================
--==========================================================================================================
--==========================================================================================================

--INTERNAL LOOP 1:             
--Basic loop to populate the file of sponsors randomly selected as givers during the period

DECLARE @PATT INTEGER;        -- (1) declares the temporal variable count as an integer
SET @PATT = 1 ;               -- (2) set the initial value of the temporal value @PATT as 1
DECLARE @END INTEGER;         -- (3) SET THE VARIABLE @END THAT DEFINES THE TOTAL NUMBER OF ITERATIONS
SET @END = (SELECT MAX(Y.Pattern_Number) FROM #Patt65_CONSOL Y) 

--INTERNAL LOOP 2: SETTING THE LOOP CONDITION ==> ITERATIONS GO FROM 1 TO 64
WHILE (@PATT <=@END)         -- (3) WHILE defines the loop the code will be executed until @PATT  reaches the value 64 NO SEMICOLON!!!

--INTERNAL LOOP 3: BEGIN ===> END DEFINE THE SPACE FOR THE CODE THAT WILL RUN IN ITERATIONS      
BEGIN	                     -- (4) this defines where the code the loop applies to starts
							-- (5) this is the body of code 
--INTERNAL LOOP 4: 
--FIRST DECLARE AND DEFINE THE VARIABLE @NY THAT IS THE NUMBER OF SPONSORS THAT ARE PREDICTED WILL GIVE IN CATEGORY @PATT
DECLARE @NY INTEGER;
SET @NY = (SELECT Y.Y_GIVE FROM #Patt65_CONSOL Y WHERE Y.Pattern_Number = @PATT)

--INTERNAL LOOP 5: CREATING THE SCORES FOR A PARTICULAR PATTERN @NY AND STORE IT INTO #ONE_PATT
-- #ONE_PATT creates and stores the values for the current loop pattern -- this code stores the randomly selected @NY as GAVE_FLG = 1
select top (@NY) a.PR_DONOR_ID , a.Pattern_Number, 1 AS 'GAVE_FLG' into #ONE_PATT from #PATTERN_data a where a.Pattern_Number = @PATT  ORDER BY NEWID()

-- #ONE_PATT creates and stores the values for the current loop pattern -- this code stores the not randomly selected @NY as GAVE_FLG = 0
insert into #ONE_PATT
select b.PR_DONOR_ID , b.Pattern_Number, 0 AS 'GAVE_FLG' from #PATTERN_data b 
left join #ONE_PATT a on a.PR_DONOR_ID = b.PR_DONOR_ID  
where a.PR_DONOR_ID is null and b.pattern_number = @PATT

--INTERNAL LOOP 6: #ONE_PATT RESULT FROM PATTERN ITERATION STORED IN #QRT_PREDICTION ON A CUMULATIVE BASIS
--now the result of #ONE_PATT which is just for one pattern is stored into the more formal #QRT_Prediction that stores ALL pattern results
insert into #QRT_PREDICTION (PR_DONOR_ID, PATTERN_NUMBER, GAVE_FLG)
select A.PR_DONOR_ID , a.Pattern_Number, a.GAVE_FLG from #ONE_PATT a

--FINALLY THE #ONE_PATT ID DROPPED SO IT CAN BE RE-USED FOR THE NEXT PATTERN IN THE ITERATION 
drop table #ONE_PATT
	

PRINT @PATT;                  -- (5) print instruction ... can be way more complex
SET @PATT = @PATT+1;          -- (6) the @variable @PATT is set up as @PATT +1 in order to allow for the next iteration 
END;						  -- (7) this instruction tells the machine to end the loop 

--by the END all donors must have a GAVE_FLG as 1, 0 as they were randomly selected in their respective Pattern (1 to 64)
--END OF INTERNAL LOOP WHAT COMES NEXT IS PROCESSED AS PART OF THE EXTERNAL LOOP ONLY AS MANY TIMES AS PREDICTION QUSRTERS ARE DEFINED
--==============================================================================================================
--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP
--==============================================================================================================
--DECLARE @PQRT INTEGER;         -- (1) declares the temporal variable count as an integer 
--SET @PQRT = 1 ;     		   -- (2) set the initial value of the temporal value @PQRT as 1
--DECLARE @PEND INTEGER;         -- (3) declares the variable @PEND 
--SET @PEND = 1 ; 
--EXTERNAL LOOP 5: 
--INTRODUCE CODE TO SAVE THE gAVE/dID NOT GIVE PREDICTION FROM #QRT_PREDICTION INTO THE MASTER TABLE #MASTER_PRED_CONSOLIDATED

--THE VARIABLES @SQL2 AND @SQL3 ARE DECLARED NVARCHAR(MAX) MEANING TEXT OF THE CODE WITH EXTENSION AS REQUIRED
--@SQL1 WILL DEAL WITH THE ALTER TABLE CODE (ADD NEW COLUMN TO #INITIAL)
--@SQL2 WILL CORRESPOND TO THE UPDATE OF THE TABLE (ADD CONTENT OF #SEQ_data TO NEW COLUMN IN #INITIAL

    DECLARE @SQL2 NVARCHAR(MAX)   
	DECLARE @SQL3 NVARCHAR(MAX)   

-- First statement add the new row to THE #MASTER_PRED_CONSOLIDATED Table 
-- the name of the column is PRED_1 for the first iteration, PRED_2 for the second and so on ...  it will stop at PRED_i , 

    SET @SQL2 = 'ALTER TABLE #MASTER_PRED_CONSOLIDATED
        ADD ' + concat('PRED_',CAST(@PQRT AS VARCHAR(10))) + ' INTEGER'

-- Second Stement Write the result of the iteration from #SEQ_data into #INITIAL_data into the correct column PRED_i for iteration i

   SET @SQL3 = 'UPDATE #MASTER_PRED_CONSOLIDATED
        SET ' + concat('PRED_',CAST(@PQRT AS VARCHAR(10))) + ' = I.GAVE_FLG
		from 
		#QRT_PREDICTION I,
	    #MASTER_PRED_CONSOLIDATED H
        where
        I.PR_DONOR_ID = H.PR_DONOR_ID'

--THE EXECUTE() CODE WILL ALLOW THE TEXT IN @SQL2 AND @SQL3 TO BE INTERPRETED AS REAL CODE NOT JUST TEXT
EXECUTE (@SQL2)
EXECUTE (@SQL3)

--drop table #QRT_PREDICTION
--EXTERNAL LOOP 6: 
--INTRODUCE CODE TO SAVE THE PATTERN INFORMATION FROM #QRT_PREDICTION INTO THE MASTER_PATT TABLE #MASTER_PATT_CONSOLIDATED
--DECLARE @PQRT INTEGER;         -- (1) declares the temporal variable count as an integer 
--SET @PQRT = 1 ;     		   -- (2) set the initial value of the temporal value @PQRT as 1
--DECLARE @PEND INTEGER;         -- (3) declares the variable @PEND 
--SET @PEND = 1 ; 
--THE VARIABLES @SQL4 AND @SQL5 ARE DECLARED NVARCHAR(MAX) MEANING TEXT OF THE CODE WITH EXTENSION AS REQUIRED
--@SQL1 WILL DEAL WITH THE ALTER TABLE CODE (ADD NEW COLUMN TO #INITIAL)
--@SQL2 WILL CORRESPOND TO THE UPDATE OF THE TABLE (ADD CONTENT OF #SEQ_data TO NEW COLUMN IN #INITIAL

    DECLARE @SQL4 NVARCHAR(MAX)   
	DECLARE @SQL5 NVARCHAR(MAX)   

-- First statement add the new row to THE #MASTER_PRED_CONSOLIDATED Table 
-- the name of the column is PRED_1 for the first iteration, PRED_2 for the second and so on ...  it will stop at PRED_i , 

    SET @SQL4 = 'ALTER TABLE #MASTER_PATT_CONSOLIDATED
        ADD ' + concat('PATT_',CAST(@PQRT AS VARCHAR(10))) + ' INTEGER'

-- Second Stement Write the result of the iteration from #SEQ_data into #INITIAL_data into the correct column PRED_i for iteration i

   SET @SQL5 = 'UPDATE #MASTER_PATT_CONSOLIDATED
        SET ' + concat('PATT_',CAST(@PQRT AS VARCHAR(10))) + ' = I.PATTERN_NUMBER
		from 
		#QRT_PREDICTION I,
	    #MASTER_PATT_CONSOLIDATED H
        where
        I.PR_DONOR_ID = H.PR_DONOR_ID'

--THE EXECUTE() CODE WILL ALLOW THE TEXT IN @SQL4 AND @SQL5 TO BE INTERPRETED AS REAL CODE NOT JUST TEXT
EXECUTE (@SQL4)
EXECUTE (@SQL5)


--FINALLY THE #QRT_PREDICTION TABLE IS DROPPED SO IT CAN BE RE-USED FOR THE NEXT QUARTER (EXTERNAL LOOP ITERATION) PATTERN IN THE ITERATION 
drop table #QRT_PREDICTION
drop table #Patt65_CONSOL
drop table #PATTERN_data
--drop table #Patt65_CONSOL	

--PRINT @PQRT;                  -- (5) print instruction ... can be way more complex
SET @PQRT = @PQRT+1;          -- (6) the @variable @PATT is set up as @PATT +1 in order to allow for the next iteration 
END;						  -- (7) this instruction tells the machine to end the loop 

select * 
INTO [SPSS_Sandbox].[dbo].[LTDV_SGR_FY21_SG_INCOME_FLG_80QRT_DEMO]
from #MASTER_PRED_CONSOLIDATED
ORDER BY 1

select * 
INTO [SPSS_Sandbox].[dbo].[LTDV_SGR_FY21_SG_PATTERN_N_80QRT]
from #MASTER_PATT_CONSOLIDATED
order by 1

--(20359 rows affected)
--Completion time: 2022-11-08T18:27:21.7399861-05:00
--==============================================================================================================
--==============================================================================================================
--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP--LOOP
--==============================================================================================================
drop table  #MASTER_PRED_CONSOLIDATED
drop table  #MASTER_PATT_CONSOLIDATED

select * FROM [SPSS_Sandbox].[dbo].[LTDV_SGR_FY21_SG_INCOME_FLG_80QRT_DEMO]
ORDER BY 1

select * FROM [SPSS_Sandbox].[dbo].[LTDV_SGR_FY21_SG_PATTERN_N_80QRT]
order by 1

--ok!! ALgorithm work beautifully!

--Commands completed successfully.
--Completion time: 2022-11-08T18:29:19.8828128-05:00
--===============================================================================================================

--SUMMARY OF TABLES GENERATED BY THE LOOPS
--=========================================
--======================================================================




--SPONSORS:
--SPONSORS AS SPONSORS
select * FROM [SPSS_Sandbox].[dbo].[LTDV_SP_FY21_SPP_INCOME_FLG_80QRT_DEMO]
--SPONSORS AS PLEDGERS
select * FROM [SPSS_Sandbox].[dbo].[LTDV_SP_FY21_PL_INCOME_FLG_80QRT_DEMO]
--SPONSORS AS SINGLE GIFT DONORS
select * FROM [SPSS_Sandbox].[dbo].[LTDV_SP_FY21_SG_INCOME_FLG_80QRT_DEMO]
--PLEDGERS:
--PLEDGERS AS PLEDGERS
select * FROM [SPSS_Sandbox].[dbo].[LTDV_PL_FY21_PL_INCOME_FLG_80QRT_DEMO]
--PLEDGERS AS SINGLE GIFT DONORS
select * FROM [SPSS_Sandbox].[dbo].[LTDV_PL_FY21_SG_INCOME_FLG_80QRT_DEMO]
--SINGLE GIFT DONORS:
--SINGLE GIFT DONORS AS SINGLE GIVERS
select * FROM [SPSS_Sandbox].[dbo].[LTDV_SGR_FY21_SG_INCOME_FLG_80QRT_DEMO]


--==================================================================================================================

-- BLOCK 6: ESTIMATION OF AVERAGE GIFTS PER QUARTER PER DONOR - SPONSORSHIP, PLEDGE AND SINGLE GIFT -- SPP, PL, SGD

--==================================================================================================================
--sponsors average gift comes from a composite of sponsorship payments plus pledge payments plus single gift payments

--sponsors: sponsorship component

--first get the raw data as required for any calculation

--3 years data by quarter with number of donations, value of donations average value of donations and flag

--3 years data for the FY21 study goes from FY22Q1 backwards to FY19Q1

CREATE OR REPLACE TABLE PRED_MODEL_FEATURE.LTDV_SPR_SPP_HISTORICAL_RAW_FOR_AVG_ESTIMATION AS
select dn.Donation_Donor_Id as DONOR_ID
, a.DNR_CAT
, A.DNR_LABEL

--SPP GIVING FLAGS
--FY3B
, ifnull(max(iff(cal.Tri_Fin_Ds_Lg = '2019 Q1' ,1,0)),0) as FY3B_Q1_SPP_F
, ifnull(max(iff(cal.Tri_Fin_Ds_Lg = '2019 Q2' ,1,0)),0) as FY3B_Q2_SPP_F
, ifnull(max(iff(cal.Tri_Fin_Ds_Lg = '2019 Q3' ,1,0)),0) as FY3B_Q3_SPP_F
, ifnull(max(iff(cal.Tri_Fin_Ds_Lg = '2019 Q4' ,1,0)),0) as FY3B_Q4_SPP_F
--FY2B
, ifnull(max(iff(cal.Tri_Fin_Ds_Lg = '2020 Q1' ,1,0)),0) as FY2B_Q1_SPP_F
, ifnull(max(iff(cal.Tri_Fin_Ds_Lg = '2020 Q2' ,1,0)),0) as FY2B_Q2_SPP_F
, ifnull(max(iff(cal.Tri_Fin_Ds_Lg = '2020 Q3' ,1,0)),0) as FY2B_Q3_SPP_F
, ifnull(max(iff(cal.Tri_Fin_Ds_Lg = '2020 Q4' ,1,0)),0) as FY2B_Q4_SPP_F
--FYC
, ifnull(max(iff(cal.Tri_Fin_Ds_Lg = '2021 Q1' ,1,0)),0) as FYC_Q1_SPP_F
, ifnull(max(iff(cal.Tri_Fin_Ds_Lg = '2021 Q2' ,1,0)),0) as FYC_Q2_SPP_F
, ifnull(max(iff(cal.Tri_Fin_Ds_Lg = '2021 Q3' ,1,0)),0) as FYC_Q3_SPP_F
, ifnull(max(iff(cal.Tri_Fin_Ds_Lg = '2021 Q4' ,1,0)),0) as FYC_Q4_SPP_F
--FY1N
, ifnull(max(iff(cal.Tri_Fin_Ds_Lg = '2022 Q1' ,1,0)),0) as FY1N_Q1_SPP_F

--SPP - NUMBER OF DONATIONS
--FY3B
, ifnull(sum(iff(cal.Tri_Fin_Ds_Lg = '2019 Q1' ,1,0)),0) as FY3B_Q1_SPP_Q
, ifnull(sum(iff(cal.Tri_Fin_Ds_Lg = '2019 Q2' ,1,0)),0) as FY3B_Q2_SPP_Q
, ifnull(sum(iff(cal.Tri_Fin_Ds_Lg = '2019 Q3' ,1,0)),0) as FY3B_Q3_SPP_Q
, ifnull(sum(iff(cal.Tri_Fin_Ds_Lg = '2019 Q4' ,1,0)),0) as FY3B_Q4_SPP_Q
--FY2B
, ifnull(sum(iff(cal.Tri_Fin_Ds_Lg = '2020 Q1' ,1,0)),0) as FY2B_Q1_SPP_Q
, ifnull(sum(iff(cal.Tri_Fin_Ds_Lg = '2020 Q2' ,1,0)),0) as FY2B_Q2_SPP_Q
, ifnull(sum(iff(cal.Tri_Fin_Ds_Lg = '2020 Q3' ,1,0)),0) as FY2B_Q3_SPP_Q
, ifnull(sum(iff(cal.Tri_Fin_Ds_Lg = '2020 Q4' ,1,0)),0) as FY2B_Q4_SPP_Q
--FYC
, ifnull(sum(iff(cal.Tri_Fin_Ds_Lg = '2021 Q1' ,1,0)),0) as FYC_Q1_SPP_Q
, ifnull(sum(iff(cal.Tri_Fin_Ds_Lg = '2021 Q2' ,1,0)),0) as FYC_Q2_SPP_Q
, ifnull(sum(iff(cal.Tri_Fin_Ds_Lg = '2021 Q3' ,1,0)),0) as FYC_Q3_SPP_Q
, ifnull(sum(iff(cal.Tri_Fin_Ds_Lg = '2021 Q4' ,1,0)),0) as FYC_Q4_SPP_Q
--FY1N
, ifnull(sum(iff(cal.Tri_Fin_Ds_Lg = '2022 Q1' ,1,0)),0) as FY1N_Q1_SPP_Q

--SPP GIVING - AMOUNT (VALUE) OF DONATIONS
--FY3B
, ifnull(SUM(iff(cal.Tri_Fin_Ds_Lg = '2019 Q1' , dn.Donation_Amount,0)),0) as FY3B_Q1_SPP_V
, ifnull(sum(iff(cal.Tri_Fin_Ds_Lg = '2019 Q2' , dn.Donation_Amount,0)),0) as FY3B_Q2_SPP_V
, ifnull(sum(iff(cal.Tri_Fin_Ds_Lg = '2019 Q3' , dn.Donation_Amount,0)),0) as FY3B_Q3_SPP_V
, ifnull(sum(iff(cal.Tri_Fin_Ds_Lg = '2019 Q4' , dn.Donation_Amount,0)),0) as FY3B_Q4_SPP_V
--FY2B
, ifnull(sum(iff(cal.Tri_Fin_Ds_Lg = '2020 Q1' , dn.Donation_Amount,0)),0) as FY2B_Q1_SPP_V
, ifnull(sum(iff(cal.Tri_Fin_Ds_Lg = '2020 Q2' , dn.Donation_Amount,0)),0) as FY2B_Q2_SPP_V
, ifnull(sum(iff(cal.Tri_Fin_Ds_Lg = '2020 Q3' , dn.Donation_Amount,0)),0) as FY2B_Q3_SPP_V
, ifnull(sum(iff(cal.Tri_Fin_Ds_Lg = '2020 Q4' , dn.Donation_Amount,0)),0) as FY2B_Q4_SPP_V
--FYC
, ifnull(sum(iff(cal.Tri_Fin_Ds_Lg = '2021 Q1' , dn.Donation_Amount,0)),0) as FYC_Q1_SPP_V
, ifnull(sum(iff(cal.Tri_Fin_Ds_Lg = '2021 Q2' , dn.Donation_Amount,0)),0) as FYC_Q2_SPP_V
, ifnull(sum(iff(cal.Tri_Fin_Ds_Lg = '2021 Q3' , dn.Donation_Amount,0)),0) as FYC_Q3_SPP_V
, ifnull(sum(iff(cal.Tri_Fin_Ds_Lg = '2021 Q4' , dn.Donation_Amount,0)),0) as FYC_Q4_SPP_V
--FY1N
, ifnull(sum(iff(cal.Tri_Fin_Ds_Lg = '2022 Q1' , dn.Donation_Amount,0)),0) as FY1N_Q1_SPP_V

----SPP GIVING - AVERAGE VALUE OF DONATIONS
, sum(dn.donation_amount) / ifnull(count(dn.donation_id),0) as FYC_SPP_AVG_V 
, MAX(DN.DONATION_AMOUNT) AS FYC_SPP_MAX_V

from ADOBE.RAW.F_DONATION dn
join adobe.raw.D_Cal cal on cal.Dt = dn.Donation_Deposit_Date
join pred_model_feature.LTDV_SCRIPT_PAST_INCOME_TNR a on a.DONATION_DONOR_ID = dn.Donation_Donor_Id
where dn.Donation_Adjustment_Reason_Sid = 0
and dn.Donation_Income_Type_for_Donor_Metrics_Sid IN (102)
and a.DNR_LABEL = 'SPONSOR'
AND DN.DONATION_DEPOSIT_DATE between '2019-10-01' and '2022-12-31'
GROUP BY DN.Donation_Donor_Id , a.DNR_CAT, A.DNR_LABEL  
--(273144 rows affected)
--Completion time: 2022-11-09T01:35:27.2661269-05:00
--AND A.DONATION_DONOR_ID IN (27635424,23540693,23720329,39344163,27363456,42032649,40778334,42835512,27283886,38642898,45858867,46112793,28754349,46944245,37713401,47742226,17528266,47652466,47620869,47674536)

--PLEDGES

CREATE OR REPLACE TABLE pred_model_feature.LTDV_SPR_PLR_PL_HISTORICAL_RAW_FOR_AVG_ESTIMATION AS
select dn.Donation_Donor_Id as DONOR_ID
, a.DNR_CAT
, A.DNR_LABEL

--PLD GIVING FLAGS
--FY3B
, ifnull(max(iff(cal.Tri_Fin_Ds_Lg = '2019 Q1' ,1,0)),0) as FY3B_Q1_PLD_F
, ifnull(max(iff(cal.Tri_Fin_Ds_Lg = '2019 Q2' ,1,0)),0) as FY3B_Q2_PLD_F
, ifnull(max(iff(cal.Tri_Fin_Ds_Lg = '2019 Q3' ,1,0)),0) as FY3B_Q3_PLD_F
, ifnull(max(iff(cal.Tri_Fin_Ds_Lg = '2019 Q4' ,1,0)),0) as FY3B_Q4_PLD_F
--FY2B
, ifnull(max(iff(cal.Tri_Fin_Ds_Lg = '2020 Q1' ,1,0)),0) as FY2B_Q1_PLD_F
, ifnull(max(iff(cal.Tri_Fin_Ds_Lg = '2020 Q2' ,1,0)),0) as FY2B_Q2_PLD_F
, ifnull(max(iff(cal.Tri_Fin_Ds_Lg = '2020 Q3' ,1,0)),0) as FY2B_Q3_PLD_F
, ifnull(max(iff(cal.Tri_Fin_Ds_Lg = '2020 Q4' ,1,0)),0) as FY2B_Q4_PLD_F
--FYC
, ifnull(max(iff(cal.Tri_Fin_Ds_Lg = '2021 Q1' ,1,0)),0) as FYC_Q1_PLD_F
, ifnull(max(iff(cal.Tri_Fin_Ds_Lg = '2021 Q2' ,1,0)),0) as FYC_Q2_PLD_F
, ifnull(max(iff(cal.Tri_Fin_Ds_Lg = '2021 Q3' ,1,0)),0) as FYC_Q3_PLD_F
, ifnull(max(iff(cal.Tri_Fin_Ds_Lg = '2021 Q4' ,1,0)),0) as FYC_Q4_PLD_F
--FY1N
, ifnull(max(iff(cal.Tri_Fin_Ds_Lg = '2022 Q1' ,1,0)),0) as FY1N_Q1_PLD_F

--PLD - NUMBER OF DONATIONS
--FY3B
, ifnull(sum(iff(cal.Tri_Fin_Ds_Lg = '2019 Q1' ,1,0)),0) as FY3B_Q1_PLD_Q
, ifnull(sum(iff(cal.Tri_Fin_Ds_Lg = '2019 Q2' ,1,0)),0) as FY3B_Q2_PLD_Q
, ifnull(sum(iff(cal.Tri_Fin_Ds_Lg = '2019 Q3' ,1,0)),0) as FY3B_Q3_PLD_Q
, ifnull(sum(iff(cal.Tri_Fin_Ds_Lg = '2019 Q4' ,1,0)),0) as FY3B_Q4_PLD_Q
--FY2B
, ifnull(sum(iff(cal.Tri_Fin_Ds_Lg = '2020 Q1' ,1,0)),0) as FY2B_Q1_PLD_Q
, ifnull(sum(iff(cal.Tri_Fin_Ds_Lg = '2020 Q2' ,1,0)),0) as FY2B_Q2_PLD_Q
, ifnull(sum(iff(cal.Tri_Fin_Ds_Lg = '2020 Q3' ,1,0)),0) as FY2B_Q3_PLD_Q
, ifnull(sum(iff(cal.Tri_Fin_Ds_Lg = '2020 Q4' ,1,0)),0) as FY2B_Q4_PLD_Q
--FYC
, ifnull(sum(iff(cal.Tri_Fin_Ds_Lg = '2021 Q1' ,1,0)),0) as FYC_Q1_PLD_Q
, ifnull(sum(iff(cal.Tri_Fin_Ds_Lg = '2021 Q2' ,1,0)),0) as FYC_Q2_PLD_Q
, ifnull(sum(iff(cal.Tri_Fin_Ds_Lg = '2021 Q3' ,1,0)),0) as FYC_Q3_PLD_Q
, ifnull(sum(iff(cal.Tri_Fin_Ds_Lg = '2021 Q4' ,1,0)),0) as FYC_Q4_PLD_Q
--FY1N
, ifnull(sum(iff(cal.Tri_Fin_Ds_Lg = '2022 Q1' ,1,0)),0) as FY1N_Q1_PLD_Q

--PLD GIVING - AMOUNT (VALUE) OF DONA'TIONS'
--FY3B
, ifnull(SUM(iff(cal.Tri_Fin_Ds_Lg = '2019 Q1' , dn.Donation_Amount,0)),0) as FY3B_Q1_PLD_V
, ifnull(sum(iff(cal.Tri_Fin_Ds_Lg = '2019 Q2' , dn.Donation_Amount,0)),0) as FY3B_Q2_PLD_V
, ifnull(sum(iff(cal.Tri_Fin_Ds_Lg = '2019 Q3' , dn.Donation_Amount,0)),0) as FY3B_Q3_PLD_V
, ifnull(sum(iff(cal.Tri_Fin_Ds_Lg = '2019 Q4' , dn.Donation_Amount,0)),0) as FY3B_Q4_PLD_V
--FY2B
, ifnull(sum(iff(cal.Tri_Fin_Ds_Lg = '2020 Q1' , dn.Donation_Amount,0)),0) as FY2B_Q1_PLD_V
, ifnull(sum(iff(cal.Tri_Fin_Ds_Lg = '2020 Q2' , dn.Donation_Amount,0)),0) as FY2B_Q2_PLD_V
, ifnull(sum(iff(cal.Tri_Fin_Ds_Lg = '2020 Q3' , dn.Donation_Amount,0)),0) as FY2B_Q3_PLD_V
, ifnull(sum(iff(cal.Tri_Fin_Ds_Lg = '2020 Q4' , dn.Donation_Amount,0)),0) as FY2B_Q4_PLD_V
--FYC
, ifnull(sum(iff(cal.Tri_Fin_Ds_Lg = '2021 Q1' , dn.Donation_Amount,0)),0) as FYC_Q1_PLD_V
, ifnull(sum(iff(cal.Tri_Fin_Ds_Lg = '2021 Q2' , dn.Donation_Amount,0)),0) as FYC_Q2_PLD_V
, ifnull(sum(iff(cal.Tri_Fin_Ds_Lg = '2021 Q3' , dn.Donation_Amount,0)),0) as FYC_Q3_PLD_V
, ifnull(sum(iff(cal.Tri_Fin_Ds_Lg = '2021 Q4' , dn.Donation_Amount,0)),0) as FYC_Q4_PLD_V
--FY1N
, ifnull(sum(iff(cal.Tri_Fin_Ds_Lg = '2022 Q1' , dn.Donation_Amount,0)),0) as FY1N_Q1_PLD_V

----PLD GIVING - AVERAGE VALUE OF DONATIONS
, sum(dn.donation_amount) / ifnull(count(dn.donation_id),0) as FYC_PLD_AVG_V 
, MAX(DN.DONATION_AMOUNT) AS FYC_PLD_MAX_V

from ADOBE.RAW.F_DONATION dn 
join adobe.raw.D_Cal cal on cal.Dt = dn.Donation_Deposit_Date
join pred_model_feature.LTDV_SCRIPT_PAST_INCOME_TNR a on a.DONATION_DONOR_ID = dn.Donation_Donor_Id
where dn.Donation_Adjustment_Reason_Sid = 0
and dn.Donation_Income_Type_for_Donor_Metrics_Sid IN (101)
and a.DNR_LABEL IN ('SPONSOR', 'PLEDGER')
AND DN.DONATION_DEPOSIT_DATE between '2019-10-01' and '2022-12-31'
GROUP BY DN.Donation_Donor_Id , a.DNR_CAT, A.DNR_LABEL
--22443 ROWS
--Completion time: 2022-11-09T01:38:04.0059881-05:00

--SINGLE GIFTS - 
CREATE OR REPLACE TABLE  pred_model_feature.LTDV_SPR_PLR_SGR_SG_HISTORICAL_RAW_FOR_AVG_ESTIMATION AS
select dn.Donation_Donor_Id as DONOR_ID
, a.DNR_CAT
, A.DNR_LABEL

--SGD GIVING FLAGS
--FY3B
, ifnull(max(iff(cal.Tri_Fin_Ds_Lg = '2019 Q1' ,1,0)),0) as FY3B_Q1_SGD_F
, ifnull(max(iff(cal.Tri_Fin_Ds_Lg = '2019 Q2' ,1,0)),0) as FY3B_Q2_SGD_F
, ifnull(max(iff(cal.Tri_Fin_Ds_Lg = '2019 Q3' ,1,0)),0) as FY3B_Q3_SGD_F
, ifnull(max(iff(cal.Tri_Fin_Ds_Lg = '2019 Q4' ,1,0)),0) as FY3B_Q4_SGD_F
--FY2B
, ifnull(max(iff(cal.Tri_Fin_Ds_Lg = '2020 Q1' ,1,0)),0) as FY2B_Q1_SGD_F
, ifnull(max(iff(cal.Tri_Fin_Ds_Lg = '2020 Q2' ,1,0)),0) as FY2B_Q2_SGD_F
, ifnull(max(iff(cal.Tri_Fin_Ds_Lg = '2020 Q3' ,1,0)),0) as FY2B_Q3_SGD_F
, ifnull(max(iff(cal.Tri_Fin_Ds_Lg = '2020 Q4' ,1,0)),0) as FY2B_Q4_SGD_F
--FYC
, ifnull(max(iff(cal.Tri_Fin_Ds_Lg = '2021 Q1' ,1,0)),0) as FYC_Q1_SGD_F
, ifnull(max(iff(cal.Tri_Fin_Ds_Lg = '2021 Q2' ,1,0)),0) as FYC_Q2_SGD_F
, ifnull(max(iff(cal.Tri_Fin_Ds_Lg = '2021 Q3' ,1,0)),0) as FYC_Q3_SGD_F
, ifnull(max(iff(cal.Tri_Fin_Ds_Lg = '2021 Q4' ,1,0)),0) as FYC_Q4_SGD_F
--FY1N
, ifnull(max(iff(cal.Tri_Fin_Ds_Lg = '2022 Q1' ,1,0)),0) as FY1N_Q1_SGD_F

--SGD - NUMBER OF DONATIONS
--FY3B
, ifnull(sum(iff(cal.Tri_Fin_Ds_Lg = '2019 Q1' ,1,0)),0) as FY3B_Q1_SGD_Q
, ifnull(sum(iff(cal.Tri_Fin_Ds_Lg = '2019 Q2' ,1,0)),0) as FY3B_Q2_SGD_Q
, ifnull(sum(iff(cal.Tri_Fin_Ds_Lg = '2019 Q3' ,1,0)),0) as FY3B_Q3_SGD_Q
, ifnull(sum(iff(cal.Tri_Fin_Ds_Lg = '2019 Q4' ,1,0)),0) as FY3B_Q4_SGD_Q
--FY2B
, ifnull(sum(iff(cal.Tri_Fin_Ds_Lg = '2020 Q1' ,1,0)),0) as FY2B_Q1_SGD_Q
, ifnull(sum(iff(cal.Tri_Fin_Ds_Lg = '2020 Q2' ,1,0)),0) as FY2B_Q2_SGD_Q
, ifnull(sum(iff(cal.Tri_Fin_Ds_Lg = '2020 Q3' ,1,0)),0) as FY2B_Q3_SGD_Q
, ifnull(sum(iff(cal.Tri_Fin_Ds_Lg = '2020 Q4' ,1,0)),0) as FY2B_Q4_SGD_Q
--FYC
, ifnull(sum(iff(cal.Tri_Fin_Ds_Lg = '2021 Q1' ,1,0)),0) as FYC_Q1_SGD_Q
, ifnull(sum(iff(cal.Tri_Fin_Ds_Lg = '2021 Q2' ,1,0)),0) as FYC_Q2_SGD_Q
, ifnull(sum(iff(cal.Tri_Fin_Ds_Lg = '2021 Q3' ,1,0)),0) as FYC_Q3_SGD_Q
, ifnull(sum(iff(cal.Tri_Fin_Ds_Lg = '2021 Q4' ,1,0)),0) as FYC_Q4_SGD_Q
--FY1N
, ifnull(sum(iff(cal.Tri_Fin_Ds_Lg = '2022 Q1' ,1,0)),0) as FY1N_Q1_SGD_Q

--SGD GIVING - AMOUNT (VALUE) OF DONATIONS
--FY3B
, ifnull(SUM(iff(cal.Tri_Fin_Ds_Lg = '2019 Q1' , dn.Donation_Amount,0)),0) as FY3B_Q1_SGD_V
, ifnull(sum(iff(cal.Tri_Fin_Ds_Lg = '2019 Q2' , dn.Donation_Amount,0)),0) as FY3B_Q2_SGD_V
, ifnull(sum(iff(cal.Tri_Fin_Ds_Lg = '2019 Q3' , dn.Donation_Amount,0)),0) as FY3B_Q3_SGD_V
, ifnull(sum(iff(cal.Tri_Fin_Ds_Lg = '2019 Q4' , dn.Donation_Amount,0)),0) as FY3B_Q4_SGD_V
--FY2B
, ifnull(sum(iff(cal.Tri_Fin_Ds_Lg = '2020 Q1' , dn.Donation_Amount,0)),0) as FY2B_Q1_SGD_V
, ifnull(sum(iff(cal.Tri_Fin_Ds_Lg = '2020 Q2' , dn.Donation_Amount,0)),0) as FY2B_Q2_SGD_V
, ifnull(sum(iff(cal.Tri_Fin_Ds_Lg = '2020 Q3' , dn.Donation_Amount,0)),0) as FY2B_Q3_SGD_V
, ifnull(sum(iff(cal.Tri_Fin_Ds_Lg = '2020 Q4' , dn.Donation_Amount,0)),0) as FY2B_Q4_SGD_V
--FYC
, ifnull(sum(iff(cal.Tri_Fin_Ds_Lg = '2021 Q1' , dn.Donation_Amount,0)),0) as FYC_Q1_SGD_V
, ifnull(sum(iff(cal.Tri_Fin_Ds_Lg = '2021 Q2' , dn.Donation_Amount,0)),0) as FYC_Q2_SGD_V
, ifnull(sum(iff(cal.Tri_Fin_Ds_Lg = '2021 Q3' , dn.Donation_Amount,0)),0) as FYC_Q3_SGD_V
, ifnull(sum(iff(cal.Tri_Fin_Ds_Lg = '2021 Q4' , dn.Donation_Amount,0)),0) as FYC_Q4_SGD_V
--FY1N
, ifnull(sum(iff(cal.Tri_Fin_Ds_Lg = '2022 Q1' , dn.Donation_Amount,0)),0) as FY1N_Q1_SGD_V

----SGD GIVING - AVERAGE VALUE OF DONATIONS
, sum(dn.donation_amount) / ifnull(count(dn.donation_id),0) as FYC_SGD_AVG_V 
, MAX(DN.DONATION_AMOUNT) AS FYC_SGD_MAX_V

from ADOBE.RAW.F_DONATION dn
join adobe.raw.D_Cal cal on cal.Dt = dn.Donation_Deposit_Date
join pred_model_feature.LTDV_SCRIPT_PAST_INCOME_TNR a on a.DONATION_DONOR_ID = dn.Donation_Donor_Id
where dn.Donation_Adjustment_Reason_Sid = 0
and dn.Donation_Income_Type_for_Donor_Metrics_Sid IN (103,104,105,106,107,108)
AND DN.DONATION_DEPOSIT_DATE between '2019-10-01' and '2022-12-31'
GROUP BY DN.Donation_Donor_Id , a.DNR_CAT, A.DNR_LABEL

--(147376 rows affected)
--Completion time: 2022-11-09T01:40:50.5598771-05:00

--EXTRACT FROM DONOR_METRICS TABLE TO VERIFY IF DONORS ARE ACTIVE IN THE PRODUCT TO BE ELIGIBLE FOR INCOME ESTIMATION

--THE EXTRACT USE THE LATEST SNAPSHOT DATE (CLOSER TO THE PERIOD WOULD BE ALSO AN OPTION BUT IF THE DONOR HAS CANCELLED THE RECURRING AFTER
--THERE WOULD BE POTWNTIAL MISTAKES OF ATTRIBUTION.

  CREATE OR REPLACE TABLE PRED_MODEL_FEATURE.LTDV_SPR_PLR_SPP_PL_CURRENT_STS_RAW_FOR_AVG_ESTIMATION AS
  SELECT A.DONATION_DONOR_ID
, IFNULL(DM.DM_Qty_Fulfilled_and_Active_Sponsorship_Pledges,0) AS N_SPPS
, IFNULL(DM.DM_Qty_Fulfilled_and_Active_Non_Sponsorship_Ongoing_Pledges,0) AS N_PLDGS
, IFF(IFNULL(DM.DM_Qty_Fulfilled_and_Active_Sponsorship_Pledges,0) = 0, 0,1) AS SPP_ACTIVE
, IFF(IFNULL(DM.DM_Qty_Fulfilled_and_Active_Non_Sponsorship_Ongoing_Pledges,0) = 0, 0,1) AS PLD_ACTIVE

FROM PRED_MODEL_FEATURE.LTDV_SCRIPT_PAST_INCOME_TNR a
LEFT join ADOBE.RAW.Donor_Metrics DM on a.DONATION_DONOR_ID = dM.DM_Donor_Id
WHERE a.DNR_LABEL IN ('SPONSOR', 'PLEDGER')
--(287494 rows affected)
--Completion time: 2022-11-09T02:07:04.6205467-05:00
--
--AND A.DONATION_DONOR_ID IN (27635424,23540693,23720329,39344163,27363456,42032649,40778334,42835512,27283886,38642898,45858867,46112793,28754349,46944245,37713401,47742226,17528266,47652466,47620869,47674536)
--SO RAW INFORMATION ON GIVING METRICS IN 

SELECT * FROM  [SPSS_Sandbox].[dbo].[LTDV_FY21_SPR_SPP_HISTORICAL_RAW_FOR_AVG_ESTIMATION]
SELECT * FROM  [SPSS_Sandbox].[dbo].[LTDV_FY21_SPR_PLR_PL_HISTORICAL_RAW_FOR_AVG_ESTIMATION]
SELECT * FROM  [SPSS_Sandbox].[dbo].[LTDV_FY21_SPR_PLR_SGR_SG_HISTORICAL_RAW_FOR_AVG_ESTIMATION]
SELECT * FROM  [SPSS_Sandbox].[dbo].[LTDV_FY21_SPR_PLR_SPP_PL_CURRENT_STS_RAW_FOR_AVG_ESTIMATION]

--RULES CAN BE DEFINED STRAIGHT FORWARD FOR SPONSORSHIP AND PLEDGES BASED ON 
-- (1) STATUS OF DONOR AS ACTIVE IN THE PRODUCT
-- (2) STATUS OF DONOR AS NEW, REACTIVATED, UPGRADED, OLD

-- (1) IF STATUS OF DONOR IS ACTIVE IN NOT ACTIVE IN THE PRODUCT THEN AVG_GIFT = 0 ELSE
--NEW , REACTIVATED, UPGRADED: PAST GIVING IS INEXISTENT (NEW) , MUST BE NOT CONSIDERED (REACTIVATED) OR HAS BEEN DISTORTED (UPGRADE)
-- THEN RELEVANT PERIOD IS JUST THE LAST FISCAL YEAR. 
-- VALUE ADJUST STRAIGHT TO 4  * THE LAST PAYMENT * THE NUMBER OF SPPS / PLEDGES THE DONOR CURRENTLY HAS.
--FOR THE OLD THE SITUATION IS ESENTIALLY SIMILAR THE VALUE OF THE PAST IS IRRELEVANT WHEN IT IS KNOWN THE NUMBER OF PRODUCTS THE DONOR HAS AND THE RATE AT WHICH HE/SHE CONTRIBUTES
--PER MONTH
 --THE ONLY PROBLEM ARISES FROM THE MINOTIRY OF DONORS THAT PRODUCES A PAYMENT TAHT EXCEEDS THE VALUE OF ONE SPONSORSHIP / PLEDGE
 --USUALLY THESE PAYMENTS WILL COVER A FULL YEAR OR QUARTER ... 
--SO APPLYING THE SIMPLIFIED RULE

--EVERY DONOR HAS A REFERENCE RATE THAT CAN BE USED TO IDENTIFY THE INDIVIDUAL OR GLOBAL NATURE OF THE PAYMENT ... 
  CREATE OR REPLACE TABLE ltdv_MAX_SPP_RATE_PER_SPR AS
 SELECT A.DONATION_DONOR_ID
 , MAX(P.PLEDGE_AMOUNT) AS RATE
  FROM PRED_MODEL_FEATURE.LTDV_SCRIPT_SP_PL_OVERALL_ACQUISITION_INFORMATION_FNL a
  join ADOBE.RAW.D_PLEDGE p on p.pledge_id = a.pledge_id
  where p.Current_Ind = 1 and a.Donation_Income_Type_for_Donor_Metrics_Sid = 102
  GROUP BY A.Donation_Donor_Id
  ORDER BY 1
  --(280179 rows affected)
  --Completion time: 2022-11-09T22:49:43.7060392-05:00

--verify/inspect:  SELECT * FROM #ltdv_FY21_MAX_SPP_RATE_PER_SPR

--CREATING THE TABLE  #LTDV_FY21_METRICS_FOR_SPP_AVG_CALC

CREATE OR REPLACE TABLE PRED_MODEL_FEATURE.LTDV_METRICS_FOR_SPP_AVG_CALC AS
select b.DONATION_DONOR_ID
, A.DNR_LABEL
, A.DNR_CAT
, B.N_SPPS
, B.SPP_ACTIVE
, c.RATE
, CASE
  WHEN C.RATE*12*N_SPPS >= (a.FYC_Q1_SPP_V + a.FYC_Q2_SPP_V + a.FYC_Q3_SPP_V + a.FYC_Q4_SPP_V ) THEN (a.FYC_Q1_SPP_V + a.FYC_Q2_SPP_V + a.FYC_Q3_SPP_V + a.FYC_Q4_SPP_V )
  ELSE C.RATE*12*N_SPPS END AS MIN_12RATE_NSPP_VS_FYC_AMT_SPP_DNTN
, C.RATE*12*N_SPPS AS EXPECTED_PMT_PER_YR
, C.RATE*3*N_SPPS AS EXPECTED_PMT_PER_QRT
--
, a.FYC_Q1_SPP_V + a.FYC_Q2_SPP_V + a.FYC_Q3_SPP_V + a.FYC_Q4_SPP_V AS FYC_AMT_SPP_DNTN
, a.FYC_Q1_SPP_V 
, a.FYC_Q2_SPP_V 
, a.FYC_Q3_SPP_V 
, a.FYC_Q4_SPP_V
--
, a.FYC_Q1_SPP_Q + a.FYC_Q2_SPP_Q + a.FYC_Q3_SPP_Q + a.FYC_Q4_SPP_Q AS FYC_QTY_SPP_DNTN
, a.FYC_Q1_SPP_Q 
, a.FYC_Q2_SPP_Q 
, a.FYC_Q3_SPP_Q 
, a.FYC_Q4_SPP_Q
--
, a.FYC_Q1_SPP_F + a.FYC_Q2_SPP_F + a.FYC_Q3_SPP_F + a.FYC_Q4_SPP_F AS FYC_NBR_QRTS_WTH_SPP_DNTN
, a.FYC_Q1_SPP_F 
, a.FYC_Q2_SPP_F 
, a.FYC_Q3_SPP_F 
, a.FYC_Q4_SPP_F

from PRED_MODEL_FEATURE.LTDV_SPR_SPP_HISTORICAL_RAW_FOR_AVG_ESTIMATION A
JOIN PRED_MODEL_FEATURE.LTDV_SPR_PLR_SPP_PL_CURRENT_STS_RAW_FOR_AVG_ESTIMATION B ON B.DONATION_DONOR_ID = A.DONOR_ID
JOIN PRED_MODEL_FEATURE.ltdv_MAX_SPP_RATE_PER_SPR C ON C.Donation_Donor_Id = A.DONOR_ID
--273141 RECORDS



--income prediction: COVID AND RATE INCREASE CHANGED EVERYTHING!
--THE PAST DOES NOT TELL THE STORY NOW THAT THE TOTAL VALUE IN LAST YEAR IS AFFECTED BY THE NEW RATES.

--THE PROCEDURE HERE WILL BE SIMPLY ONE OF EXTRAPOLATION IN WHICH THE STRUCTURE OF GIVING OF THE FISCAL YEAR IS EXTENDED FOR THE WHOLE HORIZON OF TENURE
--ADJUSTED BY THE PREDICTION OF SHORT-TERM CANCELLATION PRODUCED BY THE BAYESIAN PROCEDURE ... 

--ONE THING TO TAKE INTO CONSIDERATION THOUGH IS THAT THE NEW, AND REACTIVATED WILL POSSIBLY OBSERVE ZERO GIVING IN THE INITIAL PERIODS 
--(THEY WERE NOT IN FILE ... BUT THEY WILL BE IN THE FUTURE ... THE ZEROS MUST BE REPLACED WITH THE MINIMUM EFFECTIVE QUARTER VALUE IN THE TABLE

--ONE STRAIGHT FORWARD APPROACH WILL BE JUST TO AVERAGE OUT THE RESULTING NUMBER OF PAYMENTS AND VALUE THAT COMES OUT FROM THE LOGIC HERE AND USE IT TO CALUCLATE THE AGGREGATE
--FUTURE INCOME ... THE AGGREGATE WILL NOT MAKE A DIFFERENCE SINCE THE TOTAL VALUE PER YEAR WILL BE PRESERVED UNAFFECTED BY THE AGGREGATION


--RULE: COMPARE QUARTERLY POSSIBLE METRICS:  
-- (1) OFFICIAL EXPECTATION:  RATE*3*N_SPPS
-- (2) HISTORICAL AVERAGE : FY21_AMT_SPP_DNTN / 4 
-- (3) LATEST QUARTER: FY21_Q4_SPP_V

-- (1) MUST MATCH (3) , BUT IN CASE OF (1) > (3) THEN PICK (3) (VALUE REFLECTS CURRENT RATE BUT ALSO ACTUAL SPONSOR PERFORMANCE)
-- (2) RELEVANT ONLY WHEN FY21_Q4_SPP_V = 0 

-- SO IT LOOKS LIKE THE GENERAL RULE FOR THE DONORS THAT SHOW ACTIVITY WOULD BE EXTREMELY BASIC
--WHICH ONE TO PICK?


--IN BASIC CODE

--THIS DATA SET COMPILES FY21 DETAILED INFORMATION ON SPP PAYMENTS IN ORDER TO ESTIMATE A VALID AVG_SPP_INC_PER_QUARTER_PER_DONOR

SELECT * FROM #LTDV_FY21_METRICS_FOR_SPP_AVG_CALC

--IN BASIC CODE

--GROUP 
-- 1 CANCELLED (N_SPPS = 0, SPP_ACTIVE = 0)  ==> SPP_AVG_P_QRT = 0    SELECT * FROM #LTDV_FY21_METRICS_FOR_SPP_AVG_CALC WHERE SPP_ACTIVE = 0  --50123 RECORDS MEET THE CONDITION
-- 2 NOT CANCELLED (N_SPPS > 0, , SPP_ACTIVE = 1) ==> SPP_AVG_P_QRT = 

--2.1  NBR_PMTS > 1 AND VALUE/NBR_PMT   PMTOLD

CREATE OR REPLACE TABLE PRED_MODEL_FEATURE.LTDV_SPP_AVG_INCOME_CALC AS
SELECT *
, RATE*3*N_SPPS AS SPP_OFFICIAL_EXP
, ROUND(FYC_AMT_SPP_DNTN / 4,0) AS HIST_SPP_AVG
, FYC_Q4_SPP_V AS LATEST_QUARTER

, CASE 
--(1) SPONSORS THAT LEFT THE FILE
WHEN SPP_ACTIVE = 0 THEN 0

--(2) SPONSORS IN FILE WITH MORE THAN ONE DONATION PER YEAR AND RECORD FOR LAST QUARTER OF THE PROJECT'S FISCAL YEAR
--(2.1) DONORS IN (2) IN ('NEW','REACTIVATED','UPGRADED') -- ALL MUST PICK THE VALUE OF SPP_OFFICIAL --LAST QUARTER MUST BE UNDERSTIMATED IF THE ACQUISITION/UPGRADE HAPPENED THEN.
WHEN SPP_ACTIVE = 1 AND FYC_QTY_SPP_DNTN > 1 AND FYC_Q4_SPP_V > 0 AND DNR_CAT IN ('NEW', 'REACTIVATED', 'UPGRADED') THEN RATE*3*N_SPPS
--(2.2) DONORS IN (2) IN ('OLD') -- MUST PICK THE VALUE OF THE LAST QUARTER OF OF THE PROJECT'S FISCAL YEAR ONLY IF LOWER THAN THE SPP_OFFICIAL
WHEN SPP_ACTIVE = 1 AND FYC_QTY_SPP_DNTN > 1 AND FYC_Q4_SPP_V > 0 AND DNR_CAT IN ('OLD') 
AND RATE*3*N_SPPS <= FYC_Q4_SPP_V THEN RATE*3*N_SPPS
WHEN SPP_ACTIVE = 1 AND FYC_QTY_SPP_DNTN > 1 AND FYC_Q4_SPP_V > 0 AND DNR_CAT IN ('OLD') 
AND RATE*3*N_SPPS > FYC_Q4_SPP_V THEN FYC_Q4_SPP_V

--(3) SPONSORS IN FILE WITH MORE THAN ONE DONATION PER YEAR BUT NO RECORD FOR LAST QUARTER OF THE PROJECT'S FISCAL YEAR
--(2.1) DONORS IN (2) IN ('NEW','REACTIVATED','UPGRADED') -- ALL MUST PICK THE VALUE OF SPP_OFFICIAL --LAST QUARTER MUST BE UNDERSTIMATED IF THE ACQUISITION/UPGRADE HAPPENED THEN.
WHEN SPP_ACTIVE = 1 AND FYC_QTY_SPP_DNTN > 1 AND FYC_Q4_SPP_V > 0 AND DNR_CAT IN ('NEW', 'REACTIVATED', 'UPGRADED') THEN RATE*3*N_SPPS
--(2.2) DONORS IN (2) IN ('OLD') -- MUST PICK THE VALUE OF THE LAST QUARTER OF OF THE PROJECT'S FISCAL YEAR ONLY IF LOWER THAN THE SPP_OFFICIAL
WHEN SPP_ACTIVE = 1 AND FYC_QTY_SPP_DNTN > 1 AND FYC_Q4_SPP_V = 0 AND DNR_CAT IN ('OLD') 
AND RATE*12*N_SPPS <= FYC_AMT_SPP_DNTN THEN RATE*3*N_SPPS
WHEN SPP_ACTIVE = 1 AND FYC_QTY_SPP_DNTN > 1 AND FYC_Q4_SPP_V = 0 AND DNR_CAT IN ('OLD') 
AND RATE*12*N_SPPS > FYC_AMT_SPP_DNTN THEN ROUND((FYC_AMT_SPP_DNTN /4 ),0)
 
--(4) SPONSORS IN FILE WITH ONE DONATION PER YEAR AND RECORD FOR LAST QUARTER OF THE PROJECT'S FISCAL YEAR
--(4.1) VALUE OF THE UNIQUE DONATION IN THE YEAR IS AROUND THE SPP_OFFICIAL_EXP ... 
WHEN SPP_ACTIVE = 1 AND FYC_QTY_SPP_DNTN = 1 AND FYC_AMT_SPP_DNTN >= 100 THEN RATE*3*N_SPPS
--(4.2) VALUE OF THE UNIQUE DONATION IN THE YEAR IS AROUND THE SPP_OFFICIAL_RATE ...
WHEN SPP_ACTIVE = 1 AND FYC_QTY_SPP_DNTN = 1 AND FYC_AMT_SPP_DNTN < 100 
AND DNR_CAT IN ('NEW', 'REACTIVATED', 'UPGRADED') THEN RATE*3*N_SPPS
WHEN SPP_ACTIVE = 1 AND FYC_QTY_SPP_DNTN = 1 AND FYC_AMT_SPP_DNTN < 100 
AND DNR_CAT IN ('OLD') AND FYC_Q4_SPP_Q =1 THEN RATE*3*N_SPPS 
WHEN SPP_ACTIVE = 1 AND FYC_QTY_SPP_DNTN = 1 AND FYC_AMT_SPP_DNTN < 100 
AND DNR_CAT IN ('OLD') AND FYC_Q4_SPP_Q =0 THEN FYC_AMT_SPP_DNTN 
ELSE 0 END AS EST_SP_INC_QRT
FROM PRED_MODEL_FEATURE.LTDV_METRICS_FOR_SPP_AVG_CALC 

--=========================================================================================================================
---NOW LET'S REPLICATE THE PROCEDURE FOR THE PLEDGERS ... BOTH SPONSORS THAT HAVE PLEDGES AS WELL AS PROPER PLEDGERS ... 
SELECT TOP 0 * FROM [SPSS_Sandbox].[dbo].[LTDV_FY21_SPR_PLR_PL_HISTORICAL_RAW_FOR_AVG_ESTIMATION]
--==========================================================================================================================

--EVERY DONOR HAS A REFERENCE RATE THAT CAN BE USED TO IDENTIFY THE INDIVIDUAL OR GLOBAL NATURE OF THE PLEDGE PAYMENT ... 
  CREATE OR REPLACE TABLE ltdv_MAX_PLDG_RATE_PER_DNR AS
 SELECT A.DONATION_DONOR_ID
 , MAX(P.PLEDGE_AMOUNT) AS RATE
  FROM PRED_MODEL_FEATURE.LTDV_SCRIPT_SP_PL_OVERALL_ACQUISITION_INFORMATION_FNL a
  join ADOBE.RAW.D_PLEDGE p on p.pledge_id = a.pledge_id
  where p.Current_Ind = 1 and a.Donation_Income_Type_for_Donor_Metrics_Sid = 101
  GROUP BY A.Donation_Donor_Id
  ORDER BY 1
  --(25737 rows affected)
  --Completion time: 2022-11-10T05:15:18.2982042-05:00

--verify/inspect:  SELECT * FROM #LTDV_FY21_MAX_PLDG_RATE_PER_DNR

--verify/inspect:  SELECT RATE , COUNT(DONATION_DONOR_ID) FROM #LTDV_FY21_MAX_PLDG_RATE_PER_DNR GROUP BY RATE ORDER BY RATE

--CREATING THE TABLE  #LTDV_FY21_METRICS_FOR_SPP_AVG_CALC

CREATE OR REPLACE TABLE PRED_MODEL_FEATURE.LTDV_METRICS_FOR_PLD_AVG_CALC AS
select b.DONATION_DONOR_ID
, A.DNR_LABEL
, A.DNR_CAT
, B.N_PLDGS
, B.PLD_ACTIVE
, c.RATE
, CASE
  WHEN C.RATE*12*N_PLDGS >= (a.FYC_Q1_PLD_V + a.FYC_Q2_PLD_V + a.FYC_Q3_PLD_V + a.FYC_Q4_PLD_V ) THEN (a.FYC_Q1_PLD_V + a.FYC_Q2_PLD_V + a.FYC_Q3_PLD_V + a.FYC_Q4_PLD_V )
  ELSE C.RATE*12*N_PLDGS END AS MIN_12RATE_NPLD_VS_FYC_AMT_PLD_DNTN
, C.RATE*12*N_PLDGS AS EXPECTED_PMT_PER_YR
, C.RATE*3*N_PLDGS AS EXPECTED_PMT_PER_QRT
--
, a.FYC_Q1_PLD_V + a.FYC_Q2_PLD_V + a.FYC_Q3_PLD_V + a.FYC_Q4_PLD_V AS FYC_AMT_PLD_DNTN
, a.FYC_Q1_PLD_V 
, a.FYC_Q2_PLD_V 
, a.FYC_Q3_PLD_V 
, a.FYC_Q4_PLD_V
--
, a.FYC_Q1_PLD_Q + a.FYC_Q2_PLD_Q + a.FYC_Q3_PLD_Q + a.FYC_Q4_PLD_Q AS FYC_QTY_PLD_DNTN
, a.FYC_Q1_PLD_Q 
, a.FYC_Q2_PLD_Q 
, a.FYC_Q3_PLD_Q 
, a.FYC_Q4_PLD_Q
--
, a.FYC_Q1_PLD_F + a.FYC_Q2_PLD_F + a.FYC_Q3_PLD_F + a.FYC_Q4_PLD_F AS FYC_NBR_QRTS_WTH_PLD_DNTN
, a.FYC_Q1_PLD_F 
, a.FYC_Q2_PLD_F 
, a.FYC_Q3_PLD_F 
, a.FYC_Q4_PLD_F

from PRED_MODEL_FEATURE.LTDV_SPR_PLR_PL_HISTORICAL_RAW_FOR_AVG_ESTIMATION A
JOIN PRED_MODEL_FEATURE.LTDV_SPR_PLR_SPP_PL_CURRENT_STS_RAW_FOR_AVG_ESTIMATION B ON B.DONATION_DONOR_ID = A.DONOR_ID
JOIN PRED_MODEL_FEATURE.ltdv_MAX_PLDG_RATE_PER_DNR C ON C.Donation_Donor_Id = A.DONOR_ID
--(21731 rows affected)
--Completion time: 2022-11-10T05:29:08.7321673-05:00

--verify/inspect: SELECT * FROM #LTDV_FY21_METRICS_FOR_PLD_AVG_CALC

CREATE OR REPLACE TABLE PRED_MODEL_FEATURE.LTDV_PLD_AVG_INCOME_CALC AS
SELECT *
, RATE*3*N_PLDGS AS PLD_OFFICIAL_EXP
, ROUND(FYC_AMT_PLD_DNTN / 4,0) AS HIST_PLD_AVG
, FYC_Q4_PLD_V AS LATEST_QUARTER

, CASE 
--(1) SPONSORS THAT LEFT THE FILE
WHEN PLD_ACTIVE = 0 THEN 0

--(2) SPONSORS IN FILE WITH MORE THAN ONE DONATION PER YEAR AND RECORD FOR LAST QUARTER OF THE PROJECT'S FISCAL YEAR
--(2.1) DONORS IN (2) IN ('NEW','REACTIVATED','UPGRADED') -- ALL MUST PICK THE VALUE OF PLD_OFFICIAL --LAST QUARTER MUST BE UNDERSTIMATED IF THE ACQUISITION/UPGRADE HAPPENED THEN.
WHEN PLD_ACTIVE = 1 AND FYC_QTY_PLD_DNTN > 1 AND FYC_Q4_PLD_V > 0 AND DNR_CAT IN ('NEW', 'REACTIVATED', 'UPGRADED') THEN RATE*3*N_PLDGS
--(2.2) DONORS IN (2) IN ('OLD') -- MUST PICK THE VALUE OF THE LAST QUARTER OF OF THE PROJECT'S FISCAL YEAR ONLY IF LOWER THAN THE PLD_OFFICIAL
WHEN PLD_ACTIVE = 1 AND FYC_QTY_PLD_DNTN > 1 AND FYC_Q4_PLD_V > 0 AND DNR_CAT IN ('OLD') 
AND RATE*3*N_PLDGS <= FYC_Q4_PLD_V THEN RATE*3*N_PLDGS
WHEN PLD_ACTIVE = 1 AND FYC_QTY_PLD_DNTN > 1 AND FYC_Q4_PLD_V > 0 AND DNR_CAT IN ('OLD') 
AND RATE*3*N_PLDGS > FYC_Q4_PLD_V THEN FYC_Q4_PLD_V

--(3) SPONSORS IN FILE WITH MORE THAN ONE DONATION PER YEAR BUT NO RECORD FOR LAST QUARTER OF THE PROJECT'S FISCAL YEAR
--(2.1) DONORS IN (2) IN ('NEW','REACTIVATED','UPGRADED') -- ALL MUST PICK THE VALUE OF PLD_OFFICIAL --LAST QUARTER MUST BE UNDERSTIMATED IF THE ACQUISITION/UPGRADE HAPPENED THEN.
WHEN PLD_ACTIVE = 1 AND FYC_QTY_PLD_DNTN > 1 AND FYC_Q4_PLD_V > 0 AND DNR_CAT IN ('NEW', 'REACTIVATED', 'UPGRADED') THEN RATE*3*N_PLDGS
--(2.2) DONORS IN (2) IN ('OLD') -- MUST PICK THE VALUE OF THE LAST QUARTER OF OF THE PROJECT'S FISCAL YEAR ONLY IF LOWER THAN THE PLD_OFFICIAL
WHEN PLD_ACTIVE = 1 AND FYC_QTY_PLD_DNTN > 1 AND FYC_Q4_PLD_V = 0 AND DNR_CAT IN ('OLD') 
AND RATE*12*N_PLDGS <= FYC_AMT_PLD_DNTN THEN RATE*3*N_PLDGS
WHEN PLD_ACTIVE = 1 AND FYC_QTY_PLD_DNTN > 1 AND FYC_Q4_PLD_V = 0 AND DNR_CAT IN ('OLD') 
AND RATE*12*N_PLDGS > FYC_AMT_PLD_DNTN THEN ROUND((FYC_AMT_PLD_DNTN /4 ),0)
 
--(4) SPONSORS IN FILE WITH ONE DONATION PER YEAR AND RECORD FOR LAST QUARTER OF THE PROJECT'S FISCAL YEAR
--(4.1) VALUE OF THE UNIQUE DONATION IN THE YEAR IS AROUND THE PLD_OFFICIAL_EXP ... 
WHEN PLD_ACTIVE = 1 AND FYC_QTY_PLD_DNTN = 1 AND FYC_AMT_PLD_DNTN >= 100 THEN RATE*3*N_PLDGS
--(4.2) VALUE OF THE UNIQUE DONATION IN THE YEAR IS AROUND THE PLD_OFFICIAL_RATE ...
WHEN PLD_ACTIVE = 1 AND FYC_QTY_PLD_DNTN = 1 AND FYC_AMT_PLD_DNTN < 100 
AND DNR_CAT IN ('NEW', 'REACTIVATED', 'UPGRADED') THEN RATE*3*N_PLDGS
WHEN PLD_ACTIVE = 1 AND FYC_QTY_PLD_DNTN = 1 AND FYC_AMT_PLD_DNTN < 100 
AND DNR_CAT IN ('OLD') AND FYC_Q4_PLD_Q =1 THEN RATE*3*N_PLDGS 
WHEN PLD_ACTIVE = 1 AND FYC_QTY_PLD_DNTN = 1 AND FYC_AMT_PLD_DNTN < 100 
AND DNR_CAT IN ('OLD') AND FYC_Q4_PLD_Q =0 THEN FYC_AMT_PLD_DNTN 
ELSE 0 END AS EST_PL_INC_QRT
FROM PRED_MODEL_FEATURE.LTDV_METRICS_FOR_PLD_AVG_CALC 
--(21731 rows affected)
--Completion time: 2022-11-10T05:38:41.5172274-05:00


CREATE OR REPLACE TABLE PRED_MODEL_FEATURE.LTDV_SPP_PLD_AVG_QUARTERLY_INCOME AS
SELECT A.DONATION_DONOR_ID AS Donor_Id
, A.DNR_CAT
, B.DNR_LABEL
, IFNULL(C.EST_SP_INC_QRT, 0) AS EST_SP_INC_QRT
, IFNULL(D.EST_PL_INC_QRT, 0) AS EST_PL_INC_QRT

--ADDITIONAL INFORMATION
--SPONSOR INFO
, C.N_SPPS
, C.SPP_ACTIVE
, C.RATE AS SPP_MAX_RATE
, C.FYC_QTY_SPP_DNTN
, C.FYC_AMT_SPP_DNTN
, C.FYC_NBR_QRTS_WTH_SPP_DNTN
, C.SPP_OFFICIAL_EXP
, C.HIST_SPP_AVG
, C.LATEST_QUARTER AS LATEST_SPP_QRT
---PLEDGERS INFO
, D.N_PLDGS6
, D.PLD_ACTIVE
, D.RATE AS PLD_MAX_RATE
, D.FYC_QTY_PLD_DNTN
, D.FYC_AMT_PLD_DNTN
, D.FYC_NBR_QRTS_WTH_PLD_DNTN
, D.PLD_OFFICIAL_EXP
, D.HIST_PLD_AVG
, D.LATEST_QUARTER AS LATEST_PLD_QRT
FROM PRED_MODEL_FEATURE.LTDV_ADDITION AL_FEATURES A
JOIN PRED_MODEL_FEATURE.LTDV_SCRIPT_DNR_LABEL B ON B.Donation_Donor_Id = A.DONATION_DONOR_ID
LEFT JOIN PRED_MODEL_FEATURE.LTDV_SPP_AVG_INCOME_CALC C ON C.DONATION_DONOR_ID = A.DONATION_DONOR_ID
LEFT JOIN PRED_MODEL_FEATURE.LTDV_PLD_AVG_INCOME_CALC D ON D.DONATION_DONOR_ID = A.DONATION_DONOR_ID



--verify/inspect: SELECT * FROM [SPSS_SANDBOX].[DBO].[LTDV_SPP_PLD_AVG_QUARTERLY_INCOME_DEMO]


---============================================================================================
--SINGLE GIFT DONORS
--=============================================================================================

-- ===> FINAL CODE NOW FOR THE WHOLE DATABASE OF 232937 SPONSORS THAT HAVE AT LEAST ONE SINGLE GIFT IN THEIR LIFETIME


CREATE OR REPLACE TABLE PRED_MODEL_FEATURE.Latest_LTDV_SG_DN AS
select dn.Donation_Donor_Id
, SUM(Dn.Donation_Amount) as V_SG_in_Qrt_Last_SG
, count(Dn.Donation_Id) as N_SG_in_Qrt_Last_SG
, ss.Tri_Fin_Ds_Lg as Qrt_Last_SG
from ADOBE.RAW.F_DONATION DN 
join (
SELECT A.Donation_Donor_Id as did , A.LATEST_SG_DN_DATE as lsd , cal.TRI_FIN_DS_LG , cal.Tri_Fin_Dt_Deb , cal.Tri_Fin_Dt_Fin
FROM (SELECT DN.Donation_Donor_Id
, MAX(DN.Donation_Deposit_Date) AS Latest_SG_DN_Date
FROM ADOBE.RAW.F_DONATION DN
WHERE DN.Donation_Adjustment_Reason_Sid = 0
AND dn.Donation_Income_Type_for_Donor_Metrics_Sid in (103, 104, 105, 106, 107, 108)
AND DN.Donation_Donor_Id in (SELECT Donation_Donor_Id FROM PRED_MODEL_FEATURE.LTDV_SCRIPT_DNR_LABEL)
group by dn.Donation_Donor_Id) A
JOIN  ADOBE.RAW.D_Cal cal ON CAL.DT = A.LATEST_SG_DN_DATE
) ss on dn.Donation_Donor_Id = ss.did
where DN.Donation_Adjustment_Reason_Sid = 0
AND dn.Donation_Income_Type_for_Donor_Metrics_Sid in (103, 104, 105, 106, 107, 108)
and dn.Donation_Deposit_Date between ss.Tri_Fin_Dt_Deb and ss.Tri_Fin_Dt_Fin
group by dn.Donation_Donor_Id , ss.Tri_Fin_Ds_Lg
order by 1

--(259516 rows affected)    
--Completion time: 2022-11-10T06:59:23.7943608-05:00
SELECT * 
INTO  [SPSS_Sandbox].[dbo].[LTDV_FY21_SG_TOTAL_VALUE_GIFT_DEMO]
FROM #Latest_LTDV_SG_DN  --===> This is the valid temporal databse that is calculated at quarter level 
ORDER BY 1
--(259516 rows affected)
--Completion time: 2022-11-10T07:30:56.8645229-05:00

--verify/inspect: select * from [SPSS_Sandbox].[dbo].[LTDV_FY21_SG_TOTAL_VALUE_GIFT_DEMO]

--THE KEY VARIABLE NOW IS 'V_SG_in_Qrt_Last_SG'

--OK WHAT ABOUT THE AVERAGES THEN ... WILL THE CODE STILL WORK?

/*
select a.FY21_DNR_CAT as 'DNR_CAT'
, ROUND(avg(c.V_SG_in_Qrt_Last_SG),0) as 'AVG_VSG'
 FROM [SPSS_Sandbox].[dbo].[LTDV_SCRIPT_25072022_FY21_SPR_PLR_SGR_CAT_DEMO] a
 join  [SPSS_Sandbox].[dbo].[LTDV_FY21_SG_TOTAL_VALUE_GIFT_DEMO] c on c.Donation_Donor_Id = a.Donation_Donor_Id
 group by a.FY21_DNR_CAT
 */

 --=============================================================================
 --putting all future average gift estimations together
 --==============================================================================

 CREATE OR REPLACE TABLE PRED_MODEL_FEATURE.LTDV_SPP_PLD_SGD_AVG_QUARTERLY_INCOME_DEMO AS
 SELECT A.DONATION_DONOR_ID AS Donor_Id
, A.DNR_CAT
, B.DNR_LABEL
, IFNULL(C.EST_SP_INC_QRT, 0) AS EST_SP_INC_QRT
, IFNULL(D.EST_PL_INC_QRT, 0) AS EST_PL_INC_QRT
, IFNULL(E.V_SG_in_Qrt_Last_SG, 0) AS EST_SG_INC_QRT
, IFNULL(E.N_SG_in_Qrt_Last_SG, 0) AS EST_Nbr_SG_INC_QRT

--ADDITIONAL INFORMATION
--SPONSOR INFO
, C.N_SPPS
, C.SPP_ACTIVE
, C.RATE AS SPP_MAX_RATE
, C.FYC_QTY_SPP_DNTN
, C.FYC_AMT_SPP_DNTN
, C.FYC_NBR_QRTS_WTH_SPP_DNTN
, C.SPP_OFFICIAL_EXP
, C.HIST_SPP_AVG
, C.LATEST_QUARTER AS LATEST_SPP_QRT
---PLEDGERS INFO
, D.N_PLDGS
, D.PLD_ACTIVE
, D.RATE AS PLD_MAX_RATE
, D.FYC_QTY_PLD_DNTN
, D.FYC_AMT_PLD_DNTN
, D.FYC_NBR_QRTS_WTH_PLD_DNTN
, D.PLD_OFFICIAL_EXP
, D.HIST_PLD_AVG
, D.LATEST_QUARTER AS LATEST_PLD_QRT
--SINGLE GIFT INFO
, E.Qrt_Last_SG
FROM PRED_MODEL_FEATURE.LTDV_ADDITIONAL_FEATURES A
JOIN PRED_MODEL_FEATURE.LTDV_SCRIPT_DNR_LABEL B ON B.Donation_Donor_Id = A.DONATION_DONOR_ID
LEFT JOIN PRED_MODEL_FEATURE.LTDV_SPP_AVG_INCOME_CALC C ON C.DONATION_DONOR_ID = A.DONATION_DONOR_ID
LEFT JOIN PRED_MODEL_FEATURE.LTDV_PLD_AVG_INCOME_CALC D ON D.DONATION_DONOR_ID = A.DONATION_DONOR_ID
LEFT JOIN PRED_MODEL_FEATURE.LTDV_SG_TOTAL_VALUE_GIFT E ON E.Donation_Donor_Id = A.DONATION_DONOR_ID

--(333141 rows affected)
--Completion time: 2022-11-10T07:42:10.9506916-05:00

--VERIFY/INSPECT:  SELECT * FROM [SPSS_SANDBOX].[DBO].[LTDV_SPP_PLD_SGD_AVG_QUARTERLY_INCOME_DEMO]

--===========================================================================================================================================

--BLOCK 7:  GET EXPECTED SP & PL TENURE AND EXPECTED NUMBER OF SPP GIFTS AROUND TENURE AS PRE-REQUISITES TO CALCULATE FUTURE INCOME AND COSTS

--===========================================================================================================================================

--GET EXPECTED PL TENURE AND EXPECTED NUMBER OF SPP GIFTS AROUND TENURE ... 

--we need to difference the expected tenure from the number of quarters it is expected the donor will give during this expected tenure ... 
--the expected tenure comes from the survival model ... the number of quarters in which a full payment is expected comes from the Bayesian loop
-- now the probabilistic nature of the 

--THE  SPONSOR TENURE GENERATED FROM THE SURVIVAL MODEL IS IN 

select A.Donor_Id, A.EtenR_60 from [SPSS_Sandbox].[dbo].[FY21_SP_ETenure_02] A 


--THE ESTIMATION IS ADJUSTED BY THE RESULT OF THE BAYESIAN LOOP WHICH IS IN RAW FORM IN 
select * FROM [SPSS_Sandbox].[dbo].[LTDV_SP_FY21_SPP_INCOME_FLG_80QRT_DEMO]
   --PR_DONOR_ID

SELECT  A.Donor_Id 
, a.EtenR_60
, ISNULL(C.PRED_1 + C.PRED_2	+ C.PRED_3	+ C.PRED_4	+ C.PRED_5	+ C.PRED_6	+ C.PRED_7	+ C.PRED_8	+ C.PRED_9	+ C.PRED_10
+ C.PRED_11	+ C.PRED_12	+ C.PRED_13	+ C.PRED_14	+ C.PRED_15	+ C.PRED_16	+ C.PRED_17	+ C.PRED_18	+ C.PRED_19	+ C.PRED_20	
+ C.PRED_21	+ C.PRED_22	+ C.PRED_23	+ C.PRED_24	+ C.PRED_25	+ C.PRED_26	+ C.PRED_27	+ C.PRED_28	+ C.PRED_29	+ C.PRED_30	
+ C.PRED_31	+ C.PRED_32	+ C.PRED_33	+ C.PRED_34	+ C.PRED_35	+ C.PRED_36	+ C.PRED_37	+ C.PRED_38	+ C.PRED_39	+ C.PRED_40	
+ C.PRED_41	+ C.PRED_42	+ C.PRED_43	+ C.PRED_44	+ C.PRED_45	+ C.PRED_46	+ C.PRED_47	+ C.PRED_48	+ C.PRED_49	+ C.PRED_50	
+ C.PRED_51	+ C.PRED_52	+ C.PRED_53	+ C.PRED_54	+ C.PRED_55	+ C.PRED_56	+ C.PRED_57	+ C.PRED_58	+ C.PRED_59	+ C.PRED_60
+ C.PRED_61	+ C.PRED_62	+ C.PRED_63	+ C.PRED_64	+ C.PRED_65	+ C.PRED_66	+ C.PRED_67	+ C.PRED_68	+ C.PRED_69	+ C.PRED_70
+ C.PRED_71	+ C.PRED_72	+ C.PRED_73	+ C.PRED_74	+ C.PRED_75	+ C.PRED_76	+ C.PRED_77	+ C.PRED_78	+ C.PRED_79	+ C.PRED_80	+ C.PRED_81, 0)	as 'Bayesian_PRED_1'

, ISNULL(B.PRED_1 + B.PRED_2	+ B.PRED_3	+ B.PRED_4	+ B.PRED_5	+ B.PRED_6	+ B.PRED_7	+ B.PRED_8	+ B.PRED_9	+ B.PRED_10
+ B.PRED_11	+ B.PRED_12	+ B.PRED_13	+ B.PRED_14	+ B.PRED_15	+ B.PRED_16	+ B.PRED_17	+ B.PRED_18	+ B.PRED_19	+ B.PRED_20	
+ B.PRED_21	+ B.PRED_22	+ B.PRED_23	+ B.PRED_24	+ B.PRED_25	+ B.PRED_26	+ B.PRED_27	+ B.PRED_28	+ B.PRED_29	+ B.PRED_30	
+ B.PRED_31	+ B.PRED_32	+ B.PRED_33	+ B.PRED_34	+ B.PRED_35	+ B.PRED_36	+ B.PRED_37	+ B.PRED_38	+ B.PRED_39	+ B.PRED_40	
+ B.PRED_41	+ B.PRED_42	+ B.PRED_43	+ B.PRED_44	+ B.PRED_45	+ B.PRED_46	+ B.PRED_47	+ B.PRED_48	+ B.PRED_49	+ B.PRED_50	
+ B.PRED_51	+ B.PRED_52	+ B.PRED_53	+ B.PRED_54	+ B.PRED_55	+ B.PRED_56	+ B.PRED_57	+ B.PRED_58	+ B.PRED_59	+ B.PRED_60
+ B.PRED_61	+ B.PRED_62	+ B.PRED_63	+ B.PRED_64	+ B.PRED_65	+ B.PRED_66	+ B.PRED_67	+ B.PRED_68	+ B.PRED_69	+ B.PRED_70
+ B.PRED_71	+ B.PRED_72	+ B.PRED_73	+ B.PRED_74	+ B.PRED_75	+ B.PRED_76	+ B.PRED_77	+ B.PRED_78	+ B.PRED_79	+ B.PRED_80	+ B.PRED_81, 0)	as 'Bayesian_PRED_2'

, ISNULL(D.PRED_1 + D.PRED_2	+ D.PRED_3	+ D.PRED_4	+ D.PRED_5	+ D.PRED_6	+ D.PRED_7	+ D.PRED_8	+ D.PRED_9	+ D.PRED_10
+ D.PRED_11	+ D.PRED_12	+ D.PRED_13	+ D.PRED_14	+ D.PRED_15	+ D.PRED_16	+ D.PRED_17	+ D.PRED_18	+ D.PRED_19	+ D.PRED_20	
+ D.PRED_21	+ D.PRED_22	+ D.PRED_23	+ D.PRED_24	+ D.PRED_25	+ D.PRED_26	+ D.PRED_27	+ D.PRED_28	+ D.PRED_29	+ D.PRED_30	
+ D.PRED_31	+ D.PRED_32	+ D.PRED_33	+ D.PRED_34	+ D.PRED_35	+ D.PRED_36	+ D.PRED_37	+ D.PRED_38	+ D.PRED_39	+ D.PRED_40	
+ D.PRED_41	+ D.PRED_42	+ D.PRED_43	+ D.PRED_44	+ D.PRED_45	+ D.PRED_46	+ D.PRED_47	+ D.PRED_48	+ D.PRED_49	+ D.PRED_50	
+ D.PRED_51	+ D.PRED_52	+ D.PRED_53	+ D.PRED_54	+ D.PRED_55	+ D.PRED_56	+ D.PRED_57	+ D.PRED_58	+ D.PRED_59	+ D.PRED_60
+ D.PRED_61	+ D.PRED_62	+ D.PRED_63	+ D.PRED_64	+ D.PRED_65	+ D.PRED_66	+ D.PRED_67	+ D.PRED_68	+ D.PRED_69	+ D.PRED_70
+ D.PRED_71	+ D.PRED_72	+ D.PRED_73	+ D.PRED_74	+ D.PRED_75	+ D.PRED_76	+ D.PRED_77	+ D.PRED_78	+ D.PRED_79	+ D.PRED_80	+ D.PRED_81,0)	as 'Bayesian_PRED_3'
into #SPR_Tnr_and_BAYESIAN_PRED
FROM [SPSS_Sandbox].[dbo].[FY21_SP_ETenure_02] a 
LEFT join [SPSS_Sandbox].[dbo].[LTDV_SP_FY21_SPP_INCOME_FLG_80QRT_DEMO] c on a.Donor_Id = c.pr_donor_id
LEFT join [SPSS_Sandbox].[dbo].[LTDV_SP_FY21_SPP_INCOME_FLG_80QRT_1_DEMO] b on a.Donor_Id = b.pr_donor_id
LEFT join [SPSS_Sandbox].[dbo].[LTDV_SP_FY21_SPP_INCOME_FLG_80QRT_2_DEMO] d on a.Donor_Id = d.pr_donor_id 
order by C.PR_DONOR_ID 

--(271504 rows affected)
--Completion time: 2022-11-14T23:08:40.0484687-05:00

--where a.Donor_Id  IN ( 29928876,37891348,10426419)

--how to read that? closer bayesian prediction lower than EtenR_60

--BASED ON THE TABLE 

select Donor_ID 
, EtenR_60
, Bayesian_PRED_1
, Bayesian_PRED_2
, Bayesian_PRED_3

-- rules
--compare all the elements with EtenR_60 -- then gets the max element 
, case
WHEN iif(Bayesian_PRED_1 > Etenr_60, EtenR_60, Bayesian_PRED_1) >= iif(Bayesian_PRED_2 > Etenr_60, EtenR_60, Bayesian_PRED_2) 
AND iif(Bayesian_PRED_1 > Etenr_60, EtenR_60, Bayesian_PRED_1) >= iif(Bayesian_PRED_3 > Etenr_60, EtenR_60, Bayesian_PRED_3) THEN iif(Bayesian_PRED_1 > Etenr_60, EtenR_60, Bayesian_PRED_1)

WHEN iif(Bayesian_PRED_2 > Etenr_60, EtenR_60, Bayesian_PRED_2) >= iif(Bayesian_PRED_1 > Etenr_60, EtenR_60, Bayesian_PRED_1)
AND iif(Bayesian_PRED_2 > Etenr_60, EtenR_60, Bayesian_PRED_2) >=  iif(Bayesian_PRED_3 > Etenr_60, EtenR_60, Bayesian_PRED_3) THEN iif(Bayesian_PRED_2 > Etenr_60, EtenR_60, Bayesian_PRED_2)

ELSE iif(Bayesian_PRED_3 > Etenr_60, EtenR_60, Bayesian_PRED_3) END AS 'SPR_NBR_QUARTERS_SPP_GAVE'

INTO #UNK_FUTURE_SPR_TNR_AND_SPP_GIVING
from #SPR_Tnr_and_BAYESIAN_PRED
ORDER BY 1

--(271504 rows affected)
--Completion time: 2022-11-15T00:24:03.7769701-05:00
--VERIFY/INSPECT: SELECT * FROM #UNK_FUTURE_SPR_TNR_AND_SPP_GIVING

--SPONSORS: FUTURE PLEDGE GIVING
--THE  SPONSOR TENURE GENERATED FROM THE SURVIVAL MODEL IS IN 
select A.Donor_Id, A.EtenR_60 from [SPSS_Sandbox].[dbo].[FY21_SP_ETenure_02] A 

--SPONSORS THAT GAVE TO PLEDGERS IN (bAYESIAN lOOP)
select * FROM [SPSS_Sandbox].[dbo].[LTDV_SP_FY21_PL_INCOME_FLG_80QRT_DEMO]


--THEN, FOLLOWING THE PREVIOUS PROCEDURE ...

SELECT  A.DONOR_ID 
, a.EtenR_60
, ISNULL(C.PRED_1 + C.PRED_2	+ C.PRED_3	+ C.PRED_4	+ C.PRED_5	+ C.PRED_6	+ C.PRED_7	+ C.PRED_8	+ C.PRED_9	+ C.PRED_10
+ C.PRED_11	+ C.PRED_12	+ C.PRED_13	+ C.PRED_14	+ C.PRED_15	+ C.PRED_16	+ C.PRED_17	+ C.PRED_18	+ C.PRED_19	+ C.PRED_20	
+ C.PRED_21	+ C.PRED_22	+ C.PRED_23	+ C.PRED_24	+ C.PRED_25	+ C.PRED_26	+ C.PRED_27	+ C.PRED_28	+ C.PRED_29	+ C.PRED_30	
+ C.PRED_31	+ C.PRED_32	+ C.PRED_33	+ C.PRED_34	+ C.PRED_35	+ C.PRED_36	+ C.PRED_37	+ C.PRED_38	+ C.PRED_39	+ C.PRED_40	
+ C.PRED_41	+ C.PRED_42	+ C.PRED_43	+ C.PRED_44	+ C.PRED_45	+ C.PRED_46	+ C.PRED_47	+ C.PRED_48	+ C.PRED_49	+ C.PRED_50	
+ C.PRED_51	+ C.PRED_52	+ C.PRED_53	+ C.PRED_54	+ C.PRED_55	+ C.PRED_56	+ C.PRED_57	+ C.PRED_58	+ C.PRED_59	+ C.PRED_60
+ C.PRED_61	+ C.PRED_62	+ C.PRED_63	+ C.PRED_64	+ C.PRED_65	+ C.PRED_66	+ C.PRED_67	+ C.PRED_68	+ C.PRED_69	+ C.PRED_70
+ C.PRED_71	+ C.PRED_72	+ C.PRED_73	+ C.PRED_74	+ C.PRED_75	+ C.PRED_76	+ C.PRED_77	+ C.PRED_78	+ C.PRED_79	+ C.PRED_80	+ C.PRED_81, 0)	as 'BAYESIAN_PRED_1'

into #SPR_Tnr_and_PL_BAYES_PRED
FROM [SPSS_Sandbox].[dbo].[FY21_SP_ETenure_02] a 
LEFT join [SPSS_Sandbox].[dbo].[LTDV_SP_FY21_PL_INCOME_FLG_80QRT_DEMO] c on a.Donor_Id = c.pr_donor_id
order by C.PR_DONOR_ID 

--(271504 rows affected)
--Completion time: 2022-11-15T00:32:55.1180191-05:00

--verify/inspect: SELECT * FROM #SPR_Tnr_and_PL_BAYES_PRED ORDER BY 1


select Donor_ID 
, EtenR_60
, Bayesian_PRED_1
, Bayesian_PRED_2
, Bayesian_PRED_3

-- rules
--compare all the elements with EtenR_60 -- then gets the max element 
, iif(Bayesian_PRED_1 >= Etenr_60, EtenR_60, Bayesian_PRED_1) AS 'SPR_NBR_QUARTERS_PlD_GAVE'

INTO #UNK_FUTURE_SPR_TNR_AND_PLD_GIVING
from #SPR_Tnr_and_BAYESIAN_PRED
ORDER BY 1

--(271504 rows affected)
--Completion time: 2022-11-15T00:46:02.3957522-05:00

--VERIFY/INSPECT: SELECT * FROM #UNK_FUTURE_SPR_TNR_AND_PLD_GIVING

--SPONSORS AS SINGLE GIVERS

--SPONSORS: FUTURE PLEDGE GIVING
--THE  SPONSOR TENURE GENERATED FROM THE SURVIVAL MODEL IS IN 
select A.Donor_Id, A.EtenR_60 from [SPSS_Sandbox].[dbo].[FY21_SP_ETenure_02] A 

--SPONSORS THAT GAVE TO PLEDGERS IN (bAYESIAN lOOP)
select * FROM [SPSS_Sandbox].[dbo].[LTDV_SP_FY21_SG_INCOME_FLG_80QRT_DEMO]


--THEN, FOLLOWING THE PREVIOUS PROCEDURE ...

SELECT  A.DONOR_ID 
, a.EtenR_60
, ISNULL(C.PRED_1 + C.PRED_2	+ C.PRED_3	+ C.PRED_4	+ C.PRED_5	+ C.PRED_6	+ C.PRED_7	+ C.PRED_8	+ C.PRED_9	+ C.PRED_10
+ C.PRED_11	+ C.PRED_12	+ C.PRED_13	+ C.PRED_14	+ C.PRED_15	+ C.PRED_16	+ C.PRED_17	+ C.PRED_18	+ C.PRED_19	+ C.PRED_20	
+ C.PRED_21	+ C.PRED_22	+ C.PRED_23	+ C.PRED_24	+ C.PRED_25	+ C.PRED_26	+ C.PRED_27	+ C.PRED_28	+ C.PRED_29	+ C.PRED_30	
+ C.PRED_31	+ C.PRED_32	+ C.PRED_33	+ C.PRED_34	+ C.PRED_35	+ C.PRED_36	+ C.PRED_37	+ C.PRED_38	+ C.PRED_39	+ C.PRED_40	
+ C.PRED_41	+ C.PRED_42	+ C.PRED_43	+ C.PRED_44	+ C.PRED_45	+ C.PRED_46	+ C.PRED_47	+ C.PRED_48	+ C.PRED_49	+ C.PRED_50	
+ C.PRED_51	+ C.PRED_52	+ C.PRED_53	+ C.PRED_54	+ C.PRED_55	+ C.PRED_56	+ C.PRED_57	+ C.PRED_58	+ C.PRED_59	+ C.PRED_60
+ C.PRED_61	+ C.PRED_62	+ C.PRED_63	+ C.PRED_64	+ C.PRED_65	+ C.PRED_66	+ C.PRED_67	+ C.PRED_68	+ C.PRED_69	+ C.PRED_70
+ C.PRED_71	+ C.PRED_72	+ C.PRED_73	+ C.PRED_74	+ C.PRED_75	+ C.PRED_76	+ C.PRED_77	+ C.PRED_78	+ C.PRED_79	+ C.PRED_80	+ C.PRED_81, 0)	as 'BAYESIAN_PRED_1'

into #SPR_Tnr_and_SG_BAYES_PRED
FROM [SPSS_Sandbox].[dbo].[FY21_SP_ETenure_02] a 
LEFT join [SPSS_Sandbox].[dbo].[LTDV_SP_FY21_SG_INCOME_FLG_80QRT_DEMO] c on a.Donor_Id = c.pr_donor_id
order by C.PR_DONOR_ID 

--(271504 rows affected)
--Completion time: 2022-11-15T00:32:55.1180191-05:00

--verify/inspect: SELECT * FROM #SPR_Tnr_and_PL_BAYES_PRED ORDER BY 1


select Donor_ID 
, EtenR_60
, Bayesian_PRED_1
-- rules
--compare all the elements with EtenR_60 -- then gets the max element 
, iif(Bayesian_PRED_1 >= Etenr_60, EtenR_60, Bayesian_PRED_1) AS 'SPR_NBR_QUARTERS_SG_GAVE'

INTO #UNK_FUTURE_SPR_TNR_AND_SG_GIVING
from #SPR_Tnr_and_SG_BAYES_PRED
ORDER BY 1

--(271504 rows affected)
--Completion time: 2022-11-15T00:46:02.3957522-05:00

--VERIFY/INSPECT: SELECT * FROM #UNK_FUTURE_SPR_TNR_AND_SG_GIVING

--===============================================================================================
--===============================================================================================
--PLEDGERS

--PLEDGERS AS PLEDGERS
--PLEDGERS: FUTURE PLEDGE GIVING

--THE  SPONSOR TENURE GENERATED FROM THE SURVIVAL MODEL IS IN 
select A.Donor_Id, A.EtenR_60 from [SPSS_Sandbox].[dbo].[FY21_PL_ETenure_02] A --14254 records


--SPONSORS THAT GAVE TO PLEDGERS IN (bAYESIAN lOOP)
select * FROM [SPSS_Sandbox].[dbo].[LTDV_PL_FY21_PL_INCOME_FLG_80QRT_DEMO] --12647 RECORDS


--THEN, FOLLOWING THE PREVIOUS PROCEDURE ...

SELECT  A.DONOR_ID 
, a.EtenR_60
, ISNULL(C.PRED_1 + C.PRED_2	+ C.PRED_3	+ C.PRED_4	+ C.PRED_5	+ C.PRED_6	+ C.PRED_7	+ C.PRED_8	+ C.PRED_9	+ C.PRED_10
+ C.PRED_11	+ C.PRED_12	+ C.PRED_13	+ C.PRED_14	+ C.PRED_15	+ C.PRED_16	+ C.PRED_17	+ C.PRED_18	+ C.PRED_19	+ C.PRED_20	
+ C.PRED_21	+ C.PRED_22	+ C.PRED_23	+ C.PRED_24	+ C.PRED_25	+ C.PRED_26	+ C.PRED_27	+ C.PRED_28	+ C.PRED_29	+ C.PRED_30	
+ C.PRED_31	+ C.PRED_32	+ C.PRED_33	+ C.PRED_34	+ C.PRED_35	+ C.PRED_36	+ C.PRED_37	+ C.PRED_38	+ C.PRED_39	+ C.PRED_40	
+ C.PRED_41	+ C.PRED_42	+ C.PRED_43	+ C.PRED_44	+ C.PRED_45	+ C.PRED_46	+ C.PRED_47	+ C.PRED_48	+ C.PRED_49	+ C.PRED_50	
+ C.PRED_51	+ C.PRED_52	+ C.PRED_53	+ C.PRED_54	+ C.PRED_55	+ C.PRED_56	+ C.PRED_57	+ C.PRED_58	+ C.PRED_59	+ C.PRED_60
+ C.PRED_61	+ C.PRED_62	+ C.PRED_63	+ C.PRED_64	+ C.PRED_65	+ C.PRED_66	+ C.PRED_67	+ C.PRED_68	+ C.PRED_69	+ C.PRED_70
+ C.PRED_71	+ C.PRED_72	+ C.PRED_73	+ C.PRED_74	+ C.PRED_75	+ C.PRED_76	+ C.PRED_77	+ C.PRED_78	+ C.PRED_79	+ C.PRED_80	+ C.PRED_81, 0)	as 'BAYESIAN_PRED_1'

into #PLR_Tnr_and_PL_BAYES_PRED
FROM [SPSS_Sandbox].[dbo].[FY21_PL_ETenure_02]  a 
LEFT join [SPSS_Sandbox].[dbo].[LTDV_PL_FY21_PL_INCOME_FLG_80QRT_DEMO] c on a.Donor_Id = c.pr_donor_id
order by C.PR_DONOR_ID 

--(14254 rows affected)
--Completion time: 2022-11-15T01:08:45.2021349-05:00

--verify/inspect: SELECT * FROM #PLR_Tnr_and_PL_BAYES_PRED ORDER BY 1


select Donor_ID 
, EtenR_60
, Bayesian_PRED_1
-- rules
--compare all the elements with EtenR_60 -- then gets the max element 
, iif(Bayesian_PRED_1 >= Etenr_60, EtenR_60, Bayesian_PRED_1) AS 'PLR_NBR_QUARTERS_PL_GAVE'

INTO #UNK_FUTURE_PLR_TNR_AND_PL_GIVING
from #PLR_Tnr_and_PL_BAYES_PRED
ORDER BY 1

--(14254 rows affected)
--Completion time: 2022-11-15T00:46:02.3957522-05:00

--VERIFY/INSPECT: 
SELECT * FROM #UNK_FUTURE_PLR_TNR_AND_PL_GIVING

--(14254 rows affected)

--===============================================================================================
--PLEDGERS

--PLEDGERS AS SINGLE GIVERS
--PLEDGERS: FUTURE SINGLE GIFT GIVING

--THE  SPONSOR TENURE GENERATED FROM THE SURVIVAL MODEL IS IN 
select A.Donor_Id, A.EtenR_60 from [SPSS_Sandbox].[dbo].[FY21_PL_ETenure_02] A --14254 records


--SPONSORS THAT GAVE TO PLEDGERS IN (bAYESIAN lOOP)
select * FROM [SPSS_Sandbox].[dbo].[LTDV_PL_FY21_SG_INCOME_FLG_80QRT_DEMO] --12647 RECORDS


--THEN, FOLLOWING THE PREVIOUS PROCEDURE ...

SELECT  A.DONOR_ID 
, a.EtenR_60
, ISNULL(C.PRED_1 + C.PRED_2	+ C.PRED_3	+ C.PRED_4	+ C.PRED_5	+ C.PRED_6	+ C.PRED_7	+ C.PRED_8	+ C.PRED_9	+ C.PRED_10
+ C.PRED_11	+ C.PRED_12	+ C.PRED_13	+ C.PRED_14	+ C.PRED_15	+ C.PRED_16	+ C.PRED_17	+ C.PRED_18	+ C.PRED_19	+ C.PRED_20	
+ C.PRED_21	+ C.PRED_22	+ C.PRED_23	+ C.PRED_24	+ C.PRED_25	+ C.PRED_26	+ C.PRED_27	+ C.PRED_28	+ C.PRED_29	+ C.PRED_30	
+ C.PRED_31	+ C.PRED_32	+ C.PRED_33	+ C.PRED_34	+ C.PRED_35	+ C.PRED_36	+ C.PRED_37	+ C.PRED_38	+ C.PRED_39	+ C.PRED_40	
+ C.PRED_41	+ C.PRED_42	+ C.PRED_43	+ C.PRED_44	+ C.PRED_45	+ C.PRED_46	+ C.PRED_47	+ C.PRED_48	+ C.PRED_49	+ C.PRED_50	
+ C.PRED_51	+ C.PRED_52	+ C.PRED_53	+ C.PRED_54	+ C.PRED_55	+ C.PRED_56	+ C.PRED_57	+ C.PRED_58	+ C.PRED_59	+ C.PRED_60
+ C.PRED_61	+ C.PRED_62	+ C.PRED_63	+ C.PRED_64	+ C.PRED_65	+ C.PRED_66	+ C.PRED_67	+ C.PRED_68	+ C.PRED_69	+ C.PRED_70
+ C.PRED_71	+ C.PRED_72	+ C.PRED_73	+ C.PRED_74	+ C.PRED_75	+ C.PRED_76	+ C.PRED_77	+ C.PRED_78	+ C.PRED_79	+ C.PRED_80	+ C.PRED_81, 0)	as 'BAYESIAN_PRED_1'

into #PLR_Tnr_and_SG_BAYES_PRED
FROM [SPSS_Sandbox].[dbo].[FY21_PL_ETenure_02]  a 
LEFT join [SPSS_Sandbox].[dbo].[LTDV_PL_FY21_SG_INCOME_FLG_80QRT_DEMO] c on a.Donor_Id = c.pr_donor_id
order by C.PR_DONOR_ID 

--(14254 rows affected)
--Completion time: 2022-11-15T01:08:45.2021349-05:00

--verify/inspect: SELECT * FROM #PLR_Tnr_and_SG_BAYES_PRED ORDER BY 1


select Donor_ID 
, EtenR_60
, Bayesian_PRED_1
-- rules
--compare all the elements with EtenR_60 -- then gets the max element 
, iif(Bayesian_PRED_1 >= Etenr_60, EtenR_60, Bayesian_PRED_1) AS 'PLR_NBR_QUARTERS_SG_GAVE'

INTO #UNK_FUTURE_PLR_TNR_AND_SG_GIVING
from #PLR_Tnr_and_SG_BAYES_PRED
ORDER BY 1

--(14254 rows affected)
--Completion time: 2022-11-15T00:46:02.3957522-05:00

--VERIFY/INSPECT: SELECT * FROM #UNK_FUTURE_PLR_TNR_AND_PL_GIVING

--(14254 rows affected)

--===============================================================================================
--SINGLE GIFT DONORS

--SINGLE GIFT DONORS AS SINGLE GIVERS
--FUTURE SINGLE GIFT GIVING

--THE TENURE FOR SINGLE GIFT DONORS HAS A RESTRICTION OF 3 yrs OR 12 QUARTERS , 3 QUARTERs are ALREADY CALCULATED AS part of the known future so the scope for SG
--calculation just covers 9 quarters.  select * from [SPSS_Sandbox].[dbo].[LTDV_SCRIPT_FY21_25072022_Past_Value_Historical]
--single gift donors don't have proper tenures so the number of quarters aligns with the period they will be financially active 




--single givers THAT GAVe single gifts IN (bAYESIAN lOOP)
select * FROM [SPSS_Sandbox].[dbo].[LTDV_SGR_FY21_SG_INCOME_FLG_80QRT_DEMO] --20359 RECORDS


--THEN, FOLLOWING THE PREVIOUS PROCEDURE but with just one table ...

SELECT  c.PR_DONOR_ID as 'Donor_Id' 
, ISNULL(C.PRED_1 + C.PRED_2	+ C.PRED_3	+ C.PRED_4	+ C.PRED_5	+ C.PRED_6	+ C.PRED_7	+ C.PRED_8	+ C.PRED_9, 0)	as 'SGR_NBR_QUARTERS_SG_GAVE'

into #UNK_FUTURE_SGR_TNR_AND_SG_GIVING
FROM [SPSS_Sandbox].[dbo].[LTDV_SGR_FY21_SG_INCOME_FLG_80QRT_DEMO] c
order by C.PR_DONOR_ID 

--(20359 rows affected)
--Completion time: 2022-11-15T01:57:36.1160713-05:00

--verify/inspect: SELECT * FROM #uNK_FUTURE_SGR_TNR_AND_SG_GIVING ORDER BY 1






--the set of queries that come from the previous set of code:
/*
SELECT * FROM #UNK_FUTURE_SPR_TNR_AND_SPP_GIVING
SELECT * FROM #UNK_FUTURE_SPR_TNR_AND_PLD_GIVING
SELECT * FROM #UNK_FUTURE_SPR_TNR_AND_SG_GIVING
SELECT * FROM #UNK_FUTURE_PLR_TNR_AND_PL_GIVING
SELECT * FROM #UNK_FUTURE_PLR_TNR_AND_SG_GIVING
SELECT * FROM #UNK_FUTURE_SGR_TNR_AND_SG_GIVING
*/

--What about average gifts and costs ... it is time to put all together in order to get the FUTURE INCOME and FUTURE COST VARIABLES

--THE AVERAGE GIFT FOR SG HAS 2 ELEMENTS AS THE CALCULATION REQUIRES:
-- V_SG_in_Qrt_Last_SG IS THE VALUE OF SINGLE GIFTS DONATIONS PER DONOR PER QUARTER
-- N_SG_in_Qrt_Last_SG IS THE NUMBER OF SINGLE GIFT DONATIONS THAT MATCH THE VALUE OF DONATIONS IN V_SG_in_Qrt_Last_SG
-- NOT CALCULATED BUT EASY TO UNDERSTAND IS THE AVERAGE VALUE PER GIFT IN THE LAST QUARTER THAT RESULTS FROM 
-- ( V_SG_in_Qrt_Last_SG/ N_SG_in_Qrt_Last_SG )
-- VARIABLES ARE IN  FOR ALL DONORS THAT PRODUCED A SINGLE GIFT EVER ... 
-- NOT ALL  THE INFO WILL BE USED BUT JUST WHAT CORRESPONS TO WHOEVER PRODUCED SINGLE GIFT DONATIONS DURING FY21
select * from [SPSS_Sandbox].[dbo].[LTDV_FY21_SG_TOTAL_VALUE_GIFT_DEMO]

SELECT C.Donation_Donor_Id
, C.V_SG_in_Qrt_Last_SG
, C.N_SG_in_Qrt_Last_SG
, C.V_SG_in_Qrt_Last_SG / C.N_SG_in_Qrt_Last_SG AS 'AVG_V_SG_in_Qrt_Last_SG'
FROM [SPSS_Sandbox].[dbo].[LTDV_FY21_SG_TOTAL_VALUE_GIFT_DEMO] C
ORDER BY 1

--THIS TABLE CONTAINS THE VARIABLES EST_SP_INC_QRT AND EST_PL_INC_QRT 
SELECT  A.Donor_Id, A.EST_SP_INC_QRT 
, A.EST_PL_INC_QRT
FROM [SPSS_SANDBOX].[DBO].[LTDV_SPP_PLD_AVG_QUARTERLY_INCOME_DEMO] A
ORDER BY 1

SELECT A.Donor_Id, A.DNR_LABEL, A.DNR_CAT
--ESTIMATION OF QUARTERLY SPONSORSHIP INCOME PER DONOR
, ISNULL(A.EST_SP_INC_QRT,0) AS 'EST_SP_INC_QRT'
--ESTIMATION OF QUARTERLY PLEDGE INCOME PER DONOR
, ISNULL(A.EST_PL_INC_QRT,0) AS 'EST_PL_INC_QRT'
----ESTIMATION OF QUARTERLY SG INCOME, NUMBER OF sg GIFTS AND AVERAGE VALUE OF A SG DONATION PER DONOR
, ISNULL(C.V_SG_in_Qrt_Last_SG,0) AS 'V_SG_in_Qrt_Last_SG'
, ISNULL(C.N_SG_in_Qrt_Last_SG,0) AS 'N_SG_in_Qrt_Last_SG'
, IIF(C.N_SG_in_Qrt_Last_SG > 0, C.V_SG_in_Qrt_Last_SG / C.N_SG_in_Qrt_Last_SG, 0) AS 'AVG_V_SG_in_Qrt_Last_SG'
INTO #LTDV_FY21_AVG_GIFTS_SPP_PLD_SGD
FROM [SPSS_SANDBOX].[DBO].[LTDV_SPP_PLD_AVG_QUARTERLY_INCOME_DEMO] A
LEFT JOIN [SPSS_Sandbox].[dbo].[LTDV_FY21_SG_TOTAL_VALUE_GIFT_DEMO] C ON C.Donation_Donor_Id = A.DONOR_ID
ORDER BY 1
--(333141 rows affected)
--Completion time: 2022-11-15T23:27:43.2326143-05:00


--WHAT ABOUT THE COSTS?
--THE NUMBER OF GIFTS PER DONOR WILL BE MULTIPLIED BY THE UNIT COST PER SG (22.12) TO GET THE ESTIMATION OF SG COST PER DONOR
--UNIT SINGLE  GIFT COST IN ==> FY21_UNIT_COST_SG = 22.12
SELECT A.FY21_UNIT_COST_SG AS 'UNIT_COST_SG' FROM [SPSS_Sandbox].[dbo].[LTDV_SCRIPT_FY21_FINANCE_SG_COST_TOTAL_AND_UNIT] A

--UNIT SUPPORT COST PER DONOR PER QQ IS 2.71
 SELECT UNIT_SUPPORT_COST_PER_QQ FROM  [SPSS_Sandbox].[dbo].[LTDV_SCRIPT_FY21_FINANCE_SUPPORT_COST_PER DONOR_PER_QUARTER]
--THE SUPPORT COST MUST BE APPLIED TO ALL DONORS ... THE VARIABLE TO ATTACH THE UNIT COST TOO IS THE QUARTERLY TENURE 

--UNIT RETENTION COSTS PER DONOR PER QQ ARE 1.24 FOR spp AND 0.28 PER PLEDGE
--MUST BE APPLIED TO ALL SPONSORS AND PLEDGERS AS THEY OWN A spp OR pld ... 

--NOW IT COMES THE sg COST THAT APPLIES TO ALL DONORS THAT ARE PREDICTED TO PRODUCE SINGLE GIFTS IN THEIR FUTURE TENURE



SELECT UNIT_SP_RETENTION_COST_PER_QQ , UNIT_PL_RETENTION_COST_PER_QQ  FROM  [SPSS_Sandbox].[dbo].[LTDV_SCRIPT_FY21_FINANCE_SP_PL_RETENTION_COST_PER DONOR_PER_QUARTER]

--UNIT_SP_RETENTION_COST_PER_QQ  = 1.24
--UNIT_PL_RETENTION_COST_PER_QQ  = 0.28
--NOT IN TABLE BUT
----UNIT_SG_RETENTION_COST_PER_QQ = 0.00

--SO FUTURE UNKNOWN INCOME AND COSTS
--WILL USE THE TABLE  [SPSS_Sandbox].[dbo].[LTDV_FY21_SG_TOTAL_VALUE_GIFT_DEMO] AS THE REFERENCE FOR ALL DONOR INFORMATION ---333141 RECORDS

--================================================================================================================================================

--BLOCK 8: PUT ALL THE DATA TOGETHER TO GET THE FUTURE INCOME AND FUTURE COST VARIABLES PER DONOR

--===========================================================================================================================================
--ADD THE VALUE OF RETENTION COMING FROM THE SURVIVAL MODEL AND THE NUMBER OF SPP GIVING QUARTERS AS RESULTING FROM THE BAYESIAN LOOP, 

SELECT A.DONATION_DONOR_ID
      ,A.DNR_LABEL
      ,A.DNR_CAT
	  --SURVIVAL MODEL
      ,A.PAST_TNR_DD
      ,A.PAST_TNR_QQ
      ,A.PAST_TNR_YY
	  --FUTURE_TENURE - SURVIVAL MODEL
	  ,I.EtenR_60 AS 'FUTURE_Unknown_TENURE_QQ' ---this is the unknown component ... a different table deal with the future known 
      ,A.LTDV_FY21_PAST_INCOME
      ,A.LTDV_FY21_FUTURE_KNOWN_INCOME_EOFY22Q4
	  --BAYESIAN LOOPS
	  ,B.SPR_NBR_QUARTERS_SPP_GAVE
	  ,C.SPR_NBR_QUARTERS_PlD_GAVE
	  ,G.SPR_NBR_QUARTERS_SG_GAVE
	  ,D.PLR_NBR_QUARTERS_PL_GAVE
	  ,F.PLR_NBR_QUARTERS_SG_GAVE
	  ,E.SGR_NBR_QUARTERS_SG_GAVE
	  --AVERAGE INCOME_ESTIMATIONS
	  ,H.EST_SP_INC_QRT
	  ,H.EST_PL_INC_QRT
	  ,H.N_SG_in_Qrt_Last_SG AS 'EST_NBR_SG_QRT'
	  ,IIF(H.V_SG_in_Qrt_Last_SG > 0, H.V_SG_in_Qrt_Last_SG /H.N_SG_in_Qrt_Last_SG, 0)  AS 'EST_UNIT_SG_INC'

--FUTURE UNKNOWN INCOME (KNOWN INCOME CORRESPONDS TO QUARTER INFORMATION ALREADY AVAILABLE AS HISTORICAL RECORDS ... 
 --SPONSORSHIP INCOME
 , CASE
 WHEN A.DNR_LABEL = 'SPONSOR' THEN B.SPR_NBR_QUARTERS_SPP_GAVE*H.EST_SP_INC_QRT
 ELSE 0 END AS 'FUTURE_UNKNOWN_SPP_INCOME'

 --PLEDGE INCOME
, CASE
 WHEN A.DNR_LABEL = 'SPONSOR' THEN C.SPR_NBR_QUARTERS_PlD_GAVE*IIF(H.V_SG_in_Qrt_Last_SG > 0, H.V_SG_in_Qrt_Last_SG /H.N_SG_in_Qrt_Last_SG, 0)
 WHEN  A.DNR_LABEL ='PLEDGER' THEN D.PLR_NBR_QUARTERS_PL_GAVE*H.EST_PL_INC_QRT
 ELSE 0 END AS 'FUTURE_UNKNOWN_PLD_INCOME'

--SINGLE GIFT INCOME
, CASE 
--SPONSORS
 WHEN A.DNR_LABEL = 'SPONSOR' THEN
 G.SPR_NBR_QUARTERS_SG_GAVE * H.N_SG_in_Qrt_Last_SG * IIF(H.V_SG_in_Qrt_Last_SG > 0, H.V_SG_in_Qrt_Last_SG /H.N_SG_in_Qrt_Last_SG, 0)
--PLEDGERS
 WHEN A.DNR_LABEL = 'PLEDGER' THEN
 F.PLR_NBR_QUARTERS_SG_GAVE * H.N_SG_in_Qrt_Last_SG * IIF(H.V_SG_in_Qrt_Last_SG > 0, H.V_SG_in_Qrt_Last_SG /H.N_SG_in_Qrt_Last_SG, 0)
--SINGLE GIFT DONORS
ELSE E.SGR_NBR_QUARTERS_SG_GAVE * H.N_SG_in_Qrt_Last_SG * IIF(H.V_SG_in_Qrt_Last_SG > 0, H.V_SG_in_Qrt_Last_SG /H.N_SG_in_Qrt_Last_SG, 0)
END AS 'FUTURE_UNKNOWN_SG_INC'

--ALL COMPONENTS OF UNKNOWN FUTURE INCOME TOGETHER
--LTDV_FY21_UNKNOWN_FUTURE_INCOME
 , CASE
 WHEN A.DNR_LABEL = 'SPONSOR' THEN B.SPR_NBR_QUARTERS_SPP_GAVE*H.EST_SP_INC_QRT
 ELSE 0 END  +
 CASE
 WHEN A.DNR_LABEL = 'SPONSOR' THEN C.SPR_NBR_QUARTERS_PlD_GAVE*IIF(H.V_SG_in_Qrt_Last_SG > 0, H.V_SG_in_Qrt_Last_SG /H.N_SG_in_Qrt_Last_SG, 0)
 WHEN  A.DNR_LABEL ='PLEDGER' THEN D.PLR_NBR_QUARTERS_PL_GAVE*H.EST_PL_INC_QRT
 ELSE 0 END  +
 CASE 
--SPONSORS
 WHEN A.DNR_LABEL = 'SPONSOR' THEN
 G.SPR_NBR_QUARTERS_SG_GAVE * H.N_SG_in_Qrt_Last_SG * IIF(H.V_SG_in_Qrt_Last_SG > 0, H.V_SG_in_Qrt_Last_SG /H.N_SG_in_Qrt_Last_SG, 0)
--PLEDGERS
 WHEN A.DNR_LABEL = 'PLEDGER' THEN
 F.PLR_NBR_QUARTERS_SG_GAVE * H.N_SG_in_Qrt_Last_SG * IIF(H.V_SG_in_Qrt_Last_SG > 0, H.V_SG_in_Qrt_Last_SG /H.N_SG_in_Qrt_Last_SG, 0)
--SINGLE GIFT DONORS
ELSE E.SGR_NBR_QUARTERS_SG_GAVE * H.N_SG_in_Qrt_Last_SG * IIF(H.V_SG_in_Qrt_Last_SG > 0, H.V_SG_in_Qrt_Last_SG /H.N_SG_in_Qrt_Last_SG, 0)
END 

AS 'LTDV_FY21_FUTURE_UNKNOWN_INCOME'

--PUTTING ALL (KNOWN + UNKNOWN) FUTURE INCOME TOGETHER
, A.LTDV_FY21_FUTURE_KNOWN_INCOME_EOFY22Q4 + 
 ( CASE
 WHEN A.DNR_LABEL = 'SPONSOR' THEN B.SPR_NBR_QUARTERS_SPP_GAVE*H.EST_SP_INC_QRT
 ELSE 0 END  +
 CASE
 WHEN A.DNR_LABEL = 'SPONSOR' THEN C.SPR_NBR_QUARTERS_PlD_GAVE*IIF(H.V_SG_in_Qrt_Last_SG > 0, H.V_SG_in_Qrt_Last_SG /H.N_SG_in_Qrt_Last_SG, 0)
 WHEN  A.DNR_LABEL ='PLEDGER' THEN D.PLR_NBR_QUARTERS_PL_GAVE*H.EST_PL_INC_QRT
 ELSE 0 END ) +
 (CASE 
--SPONSORS
 WHEN A.DNR_LABEL = 'SPONSOR' THEN
 G.SPR_NBR_QUARTERS_SG_GAVE * H.N_SG_in_Qrt_Last_SG * IIF(H.V_SG_in_Qrt_Last_SG > 0, H.V_SG_in_Qrt_Last_SG /H.N_SG_in_Qrt_Last_SG, 0)
--PLEDGERS
 WHEN A.DNR_LABEL = 'PLEDGER' THEN
 F.PLR_NBR_QUARTERS_SG_GAVE * H.N_SG_in_Qrt_Last_SG * IIF(H.V_SG_in_Qrt_Last_SG > 0, H.V_SG_in_Qrt_Last_SG /H.N_SG_in_Qrt_Last_SG, 0)
--SINGLE GIFT DONORS
ELSE E.SGR_NBR_QUARTERS_SG_GAVE * H.N_SG_in_Qrt_Last_SG * IIF(H.V_SG_in_Qrt_Last_SG > 0, H.V_SG_in_Qrt_Last_SG /H.N_SG_in_Qrt_Last_SG, 0)
END) AS 'LTDV_FY21_FUTURE_INCOME'

--UNIT RETENTION COSTS PER DONOR
, CASE
	  WHEN A.DNR_LABEL = 'SPONSOR' THEN 
	  (SELECT UNIT_SP_RETENTION_COST_PER_QQ  FROM  [SPSS_Sandbox].[dbo].[LTDV_SCRIPT_FY21_FINANCE_SP_PL_RETENTION_COST_PER DONOR_PER_QUARTER]) 
	  WHEN A.DNR_LABEL = 'PLEDGER' THEN 
	  (SELECT UNIT_PL_RETENTION_COST_PER_QQ  FROM  [SPSS_Sandbox].[dbo].[LTDV_SCRIPT_FY21_FINANCE_SP_PL_RETENTION_COST_PER DONOR_PER_QUARTER]) 
	  ELSE 0 END AS 'FUTURE_UNIT_RETENTION_COST'

--UNIT SUPPORT COST PER DONOR
, (SELECT UNIT_SUPPORT_COST_PER_QQ FROM  [SPSS_Sandbox].[dbo].[LTDV_SCRIPT_FY21_FINANCE_SUPPORT_COST_PER DONOR_PER_QUARTER]) AS 'FUTURE_UNIT_SUPPORT_COST'

--UNIT COST PER SG
, (SELECT [FY21_UNIT_COST_SG FLOAT] FROM [SPSS_Sandbox].[dbo].[LTDV_SCRIPT_FY21_FINANCE_SG_COST_TOTAL_AND_UNIT]) AS 'UNIT_COST_SG'

--TOTAL RETENTION COST PER DONOR --COST APPLIES TO ALL SPONSORS & PLEDGERS 
, ROUND(CASE
	  WHEN A.DNR_LABEL IN('SPONSOR', 'PLEDGER') THEN
	  ISNULL(i.EtenR_60, 0) *
	  (SELECT UNIT_SP_RETENTION_COST_PER_QQ  FROM  [SPSS_Sandbox].[dbo].[LTDV_SCRIPT_FY21_FINANCE_SP_PL_RETENTION_COST_PER DONOR_PER_QUARTER]) 
	   ELSE 0 END, 0 ) AS 'FUTURE_UNKNOWN_RETENTION COST'

--TOTAL SUPPORT_COST_PER_DONOR -- COST APPLIES TO ALL DONORS

, ROUND (CASE
       WHEN A.DNR_LABEL IN('SPONSOR', 'PLEDGER') THEN 
	   ISNULL(B.SPR_NBR_QUARTERS_SPP_GAVE, 0) *
	  (SELECT UNIT_SUPPORT_COST_PER_QQ FROM  [SPSS_Sandbox].[dbo].[LTDV_SCRIPT_FY21_FINANCE_SUPPORT_COST_PER DONOR_PER_QUARTER]) 
	   WHEN A.DNR_LABEL = 'SINGLE_GIVER' THEN 
	   ISNULL(E.SGR_NBR_QUARTERS_SG_GAVE, 0 ) *
	  (SELECT UNIT_SUPPORT_COST_PER_QQ FROM  [SPSS_Sandbox].[dbo].[LTDV_SCRIPT_FY21_FINANCE_SUPPORT_COST_PER DONOR_PER_QUARTER]) 
	  ELSE 0 END , 0 ) AS 'FUTURE_UNKNOWN_SUPPORT_COST'

--SG COSTS --APPLIES TO ALL DONORS THAT ARE EXPECTED TO PRODUCE SINGLE GIFTS -- (UNIT_COST_PER_SG)*(NBR_SG_PER_QRT)*(COST PER SG)
--WITH COST = (SELECT A.[FY21_UNIT_COST_SG] AS 'UNIT_COST_SG' FROM [SPSS_Sandbox].[dbo].[LTDV_SCRIPT_FY21_FINANCE_SG_COST_TOTAL_AND_UNIT] A)
, CASE 
--SPONSORS
 WHEN A.DNR_LABEL = 'SPONSOR' THEN
 G.SPR_NBR_QUARTERS_SG_GAVE * H.N_SG_in_Qrt_Last_SG * (SELECT [FY21_UNIT_COST_SG FLOAT] FROM [SPSS_Sandbox].[dbo].[LTDV_SCRIPT_FY21_FINANCE_SG_COST_TOTAL_AND_UNIT])
--PLEDGERS
 WHEN A.DNR_LABEL = 'PLEDGER' THEN
 F.PLR_NBR_QUARTERS_SG_GAVE * H.N_SG_in_Qrt_Last_SG * (SELECT [FY21_UNIT_COST_SG FLOAT] FROM [SPSS_Sandbox].[dbo].[LTDV_SCRIPT_FY21_FINANCE_SG_COST_TOTAL_AND_UNIT])
--SINGLE GIFT DONORS
ELSE E.SGR_NBR_QUARTERS_SG_GAVE * H.N_SG_in_Qrt_Last_SG * (SELECT [FY21_UNIT_COST_SG FLOAT] FROM [SPSS_Sandbox].[dbo].[LTDV_SCRIPT_FY21_FINANCE_SG_COST_TOTAL_AND_UNIT])
END AS 'FUTURE_uNKNOWN_RET_COST_SG'

--TOTAL RETENTION RECURRENT & SG & SUPPORT COST PER DONOR 
, ROUND(CASE
	  WHEN A.DNR_LABEL IN('SPONSOR', 'PLEDGER') THEN
	  ISNULL(i.EtenR_60, 0) *
	  (SELECT UNIT_SP_RETENTION_COST_PER_QQ  FROM  [SPSS_Sandbox].[dbo].[LTDV_SCRIPT_FY21_FINANCE_SP_PL_RETENTION_COST_PER DONOR_PER_QUARTER]) 
	   ELSE 0 END, 0 )
+ ROUND (CASE
       WHEN A.DNR_LABEL IN('SPONSOR', 'PLEDGER') THEN 
	   ISNULL(B.SPR_NBR_QUARTERS_SPP_GAVE, 0) *
	  (SELECT UNIT_SUPPORT_COST_PER_QQ FROM  [SPSS_Sandbox].[dbo].[LTDV_SCRIPT_FY21_FINANCE_SUPPORT_COST_PER DONOR_PER_QUARTER]) 
	   WHEN A.DNR_LABEL = 'SINGLE_GIVER' THEN 
	   ISNULL(E.SGR_NBR_QUARTERS_SG_GAVE, 0 ) *
	  (SELECT UNIT_SUPPORT_COST_PER_QQ FROM  [SPSS_Sandbox].[dbo].[LTDV_SCRIPT_FY21_FINANCE_SUPPORT_COST_PER DONOR_PER_QUARTER]) 
	  ELSE 0 END , 0 ) 
+  ROUND (CASE 
	--SPONSORS
	 WHEN A.DNR_LABEL = 'SPONSOR' THEN
	 G.SPR_NBR_QUARTERS_SG_GAVE * H.N_SG_in_Qrt_Last_SG * (SELECT [FY21_UNIT_COST_SG FLOAT] FROM [SPSS_Sandbox].[dbo].[LTDV_SCRIPT_FY21_FINANCE_SG_COST_TOTAL_AND_UNIT])
	--PLEDGERS
	 WHEN A.DNR_LABEL = 'PLEDGER' THEN
	 F.PLR_NBR_QUARTERS_SG_GAVE * H.N_SG_in_Qrt_Last_SG * (SELECT [FY21_UNIT_COST_SG FLOAT] FROM [SPSS_Sandbox].[dbo].[LTDV_SCRIPT_FY21_FINANCE_SG_COST_TOTAL_AND_UNIT])
	--SINGLE GIFT DONORS
	ELSE E.SGR_NBR_QUARTERS_SG_GAVE * H.N_SG_in_Qrt_Last_SG * (SELECT [FY21_UNIT_COST_SG FLOAT] FROM [SPSS_Sandbox].[dbo].[LTDV_SCRIPT_FY21_FINANCE_SG_COST_TOTAL_AND_UNIT])
	END, 0)  AS 'FUTURE_UNKNOWN_RET_COST_FULL'	  
	  

INTO [SPSS_Sandbox].[dbo].[LTDV_SCRIPT_FY21_FUTURE_INCOME_TNR_DEMO]

  FROM [SPSS_Sandbox].[dbo].[LTDV_SCRIPT_FY21_25072022_PAST_INCOME_TNR_DEMO] A
  --TABLES WITH EXPECTED NUMBER OF QUARTERS WITH GIVING
  LEFT JOIN #UNK_FUTURE_SPR_TNR_AND_SPP_GIVING B ON B.dONOR_ID = A.DONATION_DONOR_ID
  LEFT JOIN #UNK_FUTURE_SPR_TNR_AND_PLD_GIVING C ON C.DONOR_ID = A.DONATION_DONOR_ID
  LEFT JOIN #UNK_FUTURE_SPR_TNR_AND_SG_GIVING G ON G.DONOR_ID = A.DONATION_DONOR_ID
  LEFT JOIN #UNK_FUTURE_PLR_TNR_AND_PL_GIVING D ON D.Donor_Id = A.DONATION_DONOR_ID
  LEFT JOIN #UNK_FUTURE_PLR_TNR_AND_SG_GIVING F ON F.Donor_Id = A.DONATION_DONOR_ID
  LEFT JOIN #UNK_FUTURE_SGR_TNR_AND_SG_GIVING E ON E.dONOR_ID = A.DONATION_DONOR_ID
  --TABLES WITH AVERAGE GIFTS PER TYPE OF PRODUCT PER DONOR
  LEFT JOIN #LTDV_FY21_AVG_GIFTS_SPP_PLD_SGD H ON H.Donor_Id = A.DONATION_DONOR_ID
  --TABLE WITH RAW AND ADJUSTED TENURE
  LEFT JOIN [SPSS_Sandbox].[dbo].[FY21_PL_ETenure_02] I ON I.Donor_Id = A.DONATION_DONOR_ID
  ORDER BY A.DONATION_DONOR_ID

  

 -- (333141 rows affected)
 --Completion time: 2022-11-16T05:27:55.2092219-05:00


 --VERIFY/INSPECT: SELECT top 20 * FROM [SPSS_Sandbox].[dbo].[LTDV_SCRIPT_FY21_FUTURE_INCOME_TNR_DEMO] ORDER BY 1

--================================================================================================================================================

--BLOCK 9: INTEGRATING PAST AND FUTURE COMPONENTS OF THE LONG TERM DONOR VALUE


--================================================================================================================================================
-- SUMMARY OF THE RESULTS FROM THE PAST COMPONENT SECTION OF THE PROJECT

select DISTINCT a.Donation_Donor_Id  AS 'did'
, case
when a.FY21_DNR_LABEL = 'SPONSOR' then 'ltsv'
when a.FY21_DNR_LABEL = 'PLEDGER' then 'ltpv'
else 'ltgv' end AS 'block'
, b.FINANCE_ACQ_CHANNEL AS 'FINANCE_ACQ_CHANNEL'
, c.FY21_DNR_CAT AS 'Donor_Category'
, d.CDN_Mkt_Seg_AGG AS 'CDN_Mkt_Seg_AGG'
, d.D_Region_7 AS 'GEO_Region_7'
, E.LTDV_FY21_PAST_INCOME AS 'LT_INCOME_P'
, f.LT_COST_P AS'LT_COST_P'
, - H.ACQ_COST_SP_PL_ALL_YY_T + E.LTDV_FY21_PAST_INCOME - F.LT_COST_P AS 'NET_LTDV_P'
, g.PAST_TNR_QQ AS 'TENURE_YY_P'
, h.ACQ_COST_SP_ALL_YY_T AS 'ACQ_COST_SP_ALL_YY_T'
, h.ACQ_COST_PL_ALL_YY_T AS 'ACQ_COST_PL_ALL_YY_T'
, h.ACQ_COST_SP_PL_ALL_YY_T AS 'ACQ_COST_SP_PL_ALL_YY_T'
, h.SP_NBR_ACQ_POINTS_ALL_YY AS 'SP_NBR_ACQ_POINTS_ALL_YY'
, h.PL_NBR_ACQ_POINTS_ALL_YY AS 'PL_NBR_ACQ_POINTS_ALL_YY'
, h.SP_PL_NBR_ACQ_POINTS_ALL_YY AS 'SP_PL_NBR_ACQ_POINTS_ALL_YY'
, i.NBR_SG_GIFTS_ALL_YY AS 'NBR_SG_GIFTS_ALL_YY'
, h.SP_NBR_ACQ_POINTS_FY_ONLY AS 'SP_NBR_ACQ_POINTS_FY_ONLY'
, h.PL_NBR_ACQ_POINTS_FY_ONLY AS 'PL_NBR_ACQ_POINTS_FY_ONLY'
, H.SP_PL_NBR_ACQ_POINTS_FY_ONLY AS 'SP_PL_NBR_ACQ_POINTS_FY_ONLY'
, i.NBR_SG_GIFTS_FY_ONLY AS 'NBR_SG_GIFTS_FY_ONLY'
, b.FIN_ACQ_CHANNEL_EXTENDED AS 'FIN_ACQ_CHANNEL_EXTENDED'



--INTO [SPSS_Sandbox].[dbo].[LTDV_FY21_CORE_CONSOL_INFO_PAST_COMPONENT_OCTOBER_17_2022] --[SPSS_Sandbox].[dbo].[LTDV_FY21_CORE_CONSOL_INFO_OCTOBER_17_2021]

from
[SPSS_Sandbox].[dbo].[LTDV_SCRIPT_FY21_25072022_DNR_LABEL_DEMO] a
join [SPSS_Sandbox].[dbo].[LTDV_SCRIPT_FY21_SP_PL_OVERALL_FIRST_ACQUISITION_INFORMATION_DEMO]  b on b.Donation_Donor_Id = a.Donation_Donor_Id
join [SPSS_Sandbox].[dbo].[LTDV_SCRIPT_25072022_FY21_SPR_PLR_SGR_CAT_DEMO] c on c.Donation_Donor_Id = a.Donation_Donor_Id
join [SPSS_Sandbox].[dbo].[LTDV_FY21_ADDITIONAL_FEATURES_DEMO] d on d.Donation_Donor_Id = a.Donation_Donor_Id
join [SPSS_Sandbox].[dbo].[LTDV_SCRIPT_FY21_25072022_Past_INCOME_DEMO] e on e.DONATION_DONOR_ID = a.Donation_Donor_Id
join [SPSS_Sandbox].[dbo].[LTDV_SCRIPT_FY21_LONG_TERM_PAST_RETENTION_COST_PER DONOR_DEMO] f on f.Donation_Donor_Id = a.Donation_Donor_Id
join [SPSS_Sandbox].[dbo].[LTDV_SCRIPT_FY21_25072022_PAST_INCOME_TNR_DEMO] g on g.Donation_Donor_Id = a.Donation_Donor_Id
join [SPSS_Sandbox].[dbo].[LTDV_SCRIPT_FY21_SP_PL_OVERALL_ACQUISITION_INFORMATION_PER_dONOR_DEMO]  h on h.Donation_Donor_Id = a.Donation_Donor_Id
join [SPSS_Sandbox].[dbo].[LTDV_SCRIPT_FY21_RET_COST_SG_COST_PER DONOR_DEMO] i on i.Donation_Donor_Id = a.Donation_Donor_Id
ORDER BY A.Donation_Donor_Id


select * from [SPSS_Sandbox].[dbo].[LTDV_FY21_CORE_CONSOL_INFO_PAST_COMPONENT_OCTOBER_17_2022]

--there are just a few elements related with the future value that must be included in the table to complete the deliverable of the LTDV project
/*
LT_INCOME_F
LT_INCOME_T

LT_COST_F
LT_COST_T

NET_LTDV_F
NET_LTDV_T

TENURE_YY_F
TENURE_YY_T

*/

SELECT * FROM [SPSS_Sandbox].[dbo].[LTDV_SCRIPT_FY21_FUTURE_INCOME_TNR_DEMO] ORDER BY 1

--TENURE_YY_F ==>  Future known tenure + future unknown tenure

--LT_INCOME_F ==> Future known income + future unknown income

--LT_COST_F ==> 

select a.DONATION_DONOR_ID
--future known costs 
, b.FUT_KNOWN_COST as 'LT_KNOWN_COST_F'
--future unknown costs
, a.FUTURE_UNKNOWN_RET_COST_FULL  as 'LT_UNKNOWN_COST_F'
-- future costs total
, b.FUT_KNOWN_COST + a.FUTURE_UNKNOWN_RET_COST_FULL as 'LT_COST_F'

--future known income
, b.FUT_KNOWN_INCOME,0 as 'LT_KNOWN_INCOME'
--future unknown income
, a.LTDV_FY21_FUTURE_UNKNOWN_INCOME AS 'LT_UNKNOWN_INCOME'
--future income total
, b.FUT_KNOWN_INCOME + a.LTDV_FY21_FUTURE_UNKNOWN_INCOME as 'LT_Income_F'

--future tenure known
, isnull(b.TNR_KNOWN_FUTURE_QQ,0)/4 as 'KNOWN_TENURE_YY_F'
--future tenure unknown
, isnull(a.FUTURE_Unknown_TENURE_QQ,0)/4 as 'UNKNOWN_TENURE_YY_F'
--future tenure total
, (isnull(b.TNR_KNOWN_FUTURE_QQ,0) + isnull(a.FUTURE_Unknown_TENURE_QQ,0))/4 as 'TENURE_YY_F'

into [SPSS_Sandbox].[dbo].[LTDV_script_FY21_FUTURE_METRICS_INC_COST_TNR_DEMO]

from [SPSS_Sandbox].[dbo].[LTDV_SCRIPT_FY21_FUTURE_INCOME_TNR_DEMO] a
join [SPSS_SANDBOX].[dbo].[LTDV_FY21_SCRIPT_Known_Future_Metrics] b on b.Donation_Donor_Id = a.DONATION_DONOR_ID

--(276037 rows affected)
--Completion time: 2022-11-17T12:21:25.0756379-05:00

--verify/inspect: select * FROM [SPSS_Sandbox].[dbo].[LTDV_script_FY21_FUTURE_METRICS_INC_COST_TNR_DEMO]

--=================================================================================================
--DELIVERABLE FOR THE REPORTING SYSTEM
--now finally the deliverable consolidated table that must feed the reporting system
--==================================================================================================
select DISTINCT a.Donation_Donor_Id  AS 'did'
, case
when a.FY21_DNR_LABEL = 'SPONSOR' then 'ltsv'
when a.FY21_DNR_LABEL = 'PLEDGER' then 'ltpv'
else 'ltgv' end AS 'block'
, b.FINANCE_ACQ_CHANNEL AS 'FINANCE_ACQ_CHANNEL'
, c.FY21_DNR_CAT AS 'Donor_Category'
, d.CDN_Mkt_Seg_AGG AS 'CDN_Mkt_Seg_AGG'
, d.D_Region_7 AS 'GEO_Region_7'
, E.LTDV_FY21_PAST_INCOME AS 'LT_INCOME_P'
, J.LT_Income_F AS 'LT_INCOME_F'
, E.LTDV_FY21_PAST_INCOME + J.LT_Income_F AS 'LT_INCOME_T'
, f.LT_COST_P AS'LT_COST_P'
, J.LT_COST_F 
, f.LT_COST_P + f.LT_COST_P AS 'LT_COST_T'
, - H.ACQ_COST_SP_PL_ALL_YY_T + E.LTDV_FY21_PAST_INCOME - F.LT_COST_P AS 'NET_LTDV_P'
, J.LT_Income_F - J.LT_COST_F AS 'NET_LTDV_F'
, - H.ACQ_COST_SP_PL_ALL_YY_T + E.LTDV_FY21_PAST_INCOME - F.LT_COST_P + J.LT_Income_F - J.LT_COST_F AS 'NET_LTDV_T'
, g.PAST_TNR_QQ/4 AS 'TENURE_YY_P'
, J.TENURE_YY_F  AS 'TENURE_YY_F'
, g.PAST_TNR_QQ/4 + J.TENURE_YY_F  AS 'TENURE_YY_T'
, h.ACQ_COST_SP_ALL_YY_T AS 'ACQ_COST_SP_ALL_YY_T'
, h.ACQ_COST_PL_ALL_YY_T AS 'ACQ_COST_PL_ALL_YY_T'
, h.ACQ_COST_SP_PL_ALL_YY_T AS 'ACQ_COST_SP_PL_ALL_YY_T'
, h.SP_NBR_ACQ_POINTS_ALL_YY AS 'SP_NBR_ACQ_POINTS_ALL_YY'
, h.PL_NBR_ACQ_POINTS_ALL_YY AS 'PL_NBR_ACQ_POINTS_ALL_YY'
, h.SP_PL_NBR_ACQ_POINTS_ALL_YY AS 'SP_PL_NBR_ACQ_POINTS_ALL_YY'
, i.NBR_SG_GIFTS_ALL_YY AS 'NBR_SG_GIFTS_ALL_YY'
, h.SP_NBR_ACQ_POINTS_FY_ONLY AS 'SP_NBR_ACQ_POINTS_FY_ONLY'
, h.PL_NBR_ACQ_POINTS_FY_ONLY AS 'PL_NBR_ACQ_POINTS_FY_ONLY'
, H.SP_PL_NBR_ACQ_POINTS_FY_ONLY AS 'SP_PL_NBR_ACQ_POINTS_FY_ONLY'
, i.NBR_SG_GIFTS_FY_ONLY AS 'NBR_SG_GIFTS_FY_ONLY'
, b.FIN_ACQ_CHANNEL_EXTENDED AS 'FIN_ACQ_CHANNEL_EXTENDED'


INTO [SPSS_Sandbox].[dbo].[LTDV_FY21_CORE_CONSOL_INFO_NOVEMBER_17_2021]

from
[SPSS_Sandbox].[dbo].[LTDV_SCRIPT_FY21_25072022_DNR_LABEL_DEMO] a
join [SPSS_Sandbox].[dbo].[LTDV_SCRIPT_FY21_SP_PL_OVERALL_FIRST_ACQUISITION_INFORMATION_DEMO]  b on b.Donation_Donor_Id = a.Donation_Donor_Id
join [SPSS_Sandbox].[dbo].[LTDV_SCRIPT_25072022_FY21_SPR_PLR_SGR_CAT_DEMO] c on c.Donation_Donor_Id = a.Donation_Donor_Id
join [SPSS_Sandbox].[dbo].[LTDV_FY21_ADDITIONAL_FEATURES_DEMO] d on d.Donation_Donor_Id = a.Donation_Donor_Id
join [SPSS_Sandbox].[dbo].[LTDV_SCRIPT_FY21_25072022_Past_INCOME_DEMO] e on e.DONATION_DONOR_ID = a.Donation_Donor_Id
join [SPSS_Sandbox].[dbo].[LTDV_SCRIPT_FY21_LONG_TERM_PAST_RETENTION_COST_PER DONOR_DEMO] f on f.Donation_Donor_Id = a.Donation_Donor_Id
join [SPSS_Sandbox].[dbo].[LTDV_SCRIPT_FY21_25072022_PAST_INCOME_TNR_DEMO] g on g.Donation_Donor_Id = a.Donation_Donor_Id
join [SPSS_Sandbox].[dbo].[LTDV_SCRIPT_FY21_SP_PL_OVERALL_ACQUISITION_INFORMATION_PER_dONOR_DEMO]  h on h.Donation_Donor_Id = a.Donation_Donor_Id
join [SPSS_Sandbox].[dbo].[LTDV_SCRIPT_FY21_RET_COST_SG_COST_PER DONOR_DEMO] i on i.Donation_Donor_Id = a.Donation_Donor_Id
JOIN [SPSS_SANDBOX].[DBO].[LTDV_script_FY21_FUTURE_METRICS_INC_COST_TNR_DEMO] J ON J.DONATION_DONOR_ID = A.Donation_Donor_Id
ORDER BY A.Donation_Donor_Id

--(276037 rows affected)
--Completion time: 2022-11-17T12:34:16.2087762-05:00