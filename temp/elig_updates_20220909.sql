set V_CALENDAR_DATE_CST =  current_date();

BEGIN TRANSACTION;

delete from {{ params.curated_database_name }}.SUMMARY.IPL_ELIGIBILITY_DAILY_SUMMARY
  where CALENDAR_DATE_CST = $V_CALENDAR_DATE_CST;


insert into {{ params.curated_database_name }}.SUMMARY.IPL_ELIGIBILITY_DAILY_SUMMARY
(CALENDAR_DATE_CST,
CLIENT_ID,
ACTIVATION_CODE,
FIRST_NAME,
LAST_NAME,
MAILING_ADDRESS,
CITY,
STATE,
ZIP_CODE,
EMAIL_ADDRESS,
TELEPHONE_NUMBER,
DOB,
SSN,
DRAFT_AMOUNT,
PAYMENT_FREQUENCY,
LAST_PAYMENT_DATE,
LAST_PAYMENT_DATE2,
AMOUNT_FINANCED,
ESTIMATED_BEYOND_PROGRAM_FEES,
TOTAL_DEPOSITS,
SETTLEMENT_AMOUNT,
TRADELINE_NAME,
TRADELINE_ACCOUNT_NUMBER,
CFT_ROUTING_NUMBER,
CFT_ACCOUNT_NUMBER,
CFT_BANK_NAME,
CFT_ACCOUNT_HOLDER_NAME,
EXTERNAL_ID,
CO_CLIENT,
PROGRAM_ID,
MONTHS_SINCE_ENROLLMENT,
NEXT_PAYMENT_DATE,
TOTAL_AMOUNT_ENROLLED_DEBT,
BEYOND_ENROLLMENT_DATE,
BEYOND_ENROLLMENT_STATUS,
NSFS_3_MONTHS,
ORIGINAL_CREDITOR,
SETTLEMENT_PERCENT,
SETTLED_TRADELINED_FLAG,
PAYMENT_ADHERENCE_RATIO_3_MONTHS,
PAYMENT_ADHERENCE_RATIO_4_MONTHS,
PAYMENT_ADHERENCE_RATIO_6_MONTHS,
HOLDERS_NAME,
BANK_NAME,
BANK_ACCOUNT_NUMBER,
BANK_ROUTING_NUMBER,
BANK_ACCOUNT_TYPE,
HISTORICAL_SETTLEMENT_PERCENT,
BF_REMAINING_DEPOSITS,
SOURCE_SYSTEM,
SERVICE_ENTITY_NAME
)
select $V_CALENDAR_DATE_CST CALENDAR_DATE_CST,ipl.*,p.SOURCE_SYSTEM,p.SERVICE_ENTITY_NAME
from
(select client_id, activation_code, FIRST_NAME, LAST_NAME, Mailing_Address, CITY, STATE, ZIP_Code, Email_Address, Telephone_Number, DOB, SSN,
       DRAFT_AMOUNT, NU_DSE_PAYMENT_FREQUENCY_C, last_payment_date, last_payment_date2, Amount_Financed, estimated_beyond_program_fees,
       total_deposits, settlement_amount, tradeline_name, tradeline_account_number
       ,CFT_ROUTING_NUMBER
       ,CFT_ACCOUNT_NUMBER
       ,CFT_BANK_NAME
       ,CFT_ACCOUNT_HOLDER_NAME
       ,external_id, CO_CLIENT,
       program_id, months_since_enrollment, next_payment_date, total_amount_enrolled_debt, beyond_enrollment_date, beyond_enrollment_status,
       nsfs_3_months, original_creditor, settlement_percent, settled_tradelined_flag, payment_adherence_ratio_3_months, payment_adherence_ratio_4_months, payment_adherence_ratio_6_months,
       NU_DSE_HOLDER_S_NAME_C, NU_DSE_BANK_NAME_C, NU_DSE_BANK_ACCOUNT_NUMBER_C, NU_DSE_ROUTING_NUMBER_C, NU_DSE_ACCOUNT_TYPE_C,
       HISTORICAL_SETTLEMENT_PERCENT, BF_REMAINING_DEPOSITS
from (
  with
  last_nsf as (
    select program.id                         program_id,
           program.Name                       program_name,
           count(payment.id)                  nsfs,
           count(case when dateadd(month, -3, cast($V_CALENDAR_DATE_CST - 1 as date)) < NU_DSE_SCHEDULE_DATE_C then payment.id end) nsf_3_mos,
           --count(case when datediff(month, NU_DSE_SCHEDULE_DATE_C, cast($V_CALENDAR_DATE_CST - 1 as date)) <= 3 then payment.id end) nsf_3_mos,
           max(NU_DSE_SCHEDULE_DATE_C)        last_nsf_dt
    from {{ params.refined_database_name }}.SALESFORCE.NU_DSE_PROGRAM_C program
    join {{ params.refined_database_name }}.SALESFORCE.NU_DSE_PAYMENT_C payment on payment.NU_DSE_PROGRAM_C = program.id
    where payment.NU_DSE_PAYMENT_TYPE_C in ('Draft', 'Deposit')
      and payment.NU_DSE_TRANSACTION_STATUS_C = 'Failed'
    group by program.id, program.name
  ),
  current_draft_amt as (
    select a.program_id
        , a.program_name
        , a.principal_amount_including_fees_per_frequency as Per_freq_amount
         , case when PAYMENT_FREQUENCY in ('Twice Monthly','Semi-Monthly') then Per_freq_amount*24/12
                when PAYMENT_FREQUENCY in ('Bi-Weekly') then Per_freq_amount*26/12
                when PAYMENT_FREQUENCY in ('Monthly') then Per_freq_amount end as amount
        , CASE
              WHEN datediff(MONTH, ENROLLED_DATE_CST, current_date) <= 3 THEN ((POWER(1 + (0.27 / 12), 60) - 1)) /
                                                                              ((0.27 / 12) * (POWER(1 + (0.27 / 12), 60)))
              WHEN datediff(MONTH, ENROLLED_DATE_CST, current_date) = 4 THEN ((POWER(1 + (0.256 / 12), 60) - 1)) /
                                                                             ((0.2560 / 12) * (POWER(1 + (0.2560 / 12), 60)))
              WHEN datediff(MONTH, ENROLLED_DATE_CST, current_date) = 5 THEN ((POWER(1 + (0.2435 / 12), 60) - 1)) /
                                                                             ((0.2435 / 12) * (POWER(1 + (0.2435 / 12), 60)))
              WHEN datediff(MONTH, ENROLLED_DATE_CST, current_date) >= 6 THEN ((POWER(1 + (0.229 / 12), 60) - 1)) /
                                                                              ((0.229 / 12) * (POWER(1 + (0.229 / 12), 60)))
              END AS DISCOUNT_FACTOR
    from (
--          select distinct
--             program_id
--           , program_name
--           , principal_amount_per_frequency
--           , principal_amount_including_fees_per_frequency
--           , monthly_principal_amount
--           , monthly_principal_amount_including_fees
--           from {{ params.curated_database_name }}.crm.program
--           where is_current_record_flag = TRUE
Select P.PROGRAM_NAME,P.PROGRAM_ID,P.PAYMENT_FREQUENCY,P.ENROLLED_DATE_CST,
       S.TA+nvl(P.MONTHLY_TRUST_ACCOUNT_SERVICE_FEE,0)+nvl(P.MONTHLY_LEGAL_SERVICE_FEE,0) principal_amount_including_fees_per_frequency
FROM {{ params.curated_database_name }}.CRM.PROGRAM P
LEFT JOIN (
    Select PROGRAM_NAME,
           TA,
           count(PROGRAM_NAME) CT,
           row_number()        over (PARTITION BY PROGRAM_NAME ORDER BY CT DESC,TA DESC) as rnk
    FROM (Select PROGRAM_NAME, last_day(SCHEDULED_DATE_CST), avg(BASE_AMOUNT)::DECIMAL(38,2) as TA
          FROM {{ params.curated_database_name }}.CRM.SCHEDULED_TRANSACTION
          WHERE IS_CURRENT_RECORD_FLAG = TRUE
            and TRANSACTION_TYPE = 'Deposit'
            and TRANSACTION_STATUS = 'Scheduled'
            and SCHEDULED_DATE_CST >= $V_CALENDAR_DATE_CST
            and ORIGINAL_SOURCE_SYSTEM = 'LEGACY'
            and IS_DELETED_FLAG = FALSE

          GROUP BY 1, 2)

    GROUP BY 1, 2
    qualify rnk=1
    ORDER BY PROGRAM_NAME) S on S.PROGRAM_NAME=P.PROGRAM_NAME
WHERE P.IS_CURRENT_RECORD_FLAG=TRUE and P.PROGRAM_STATUS in ('Active','New')

        ) a
--    left join {{ params.curated_database_name }}.crm.program p on P.Is_CURRENT_RECORD_FLAG=TRUE and P.PROGRAM_NAME =a.PROGRAM_NAME
  ),
  next_draft_date as (
    select program.id program_id,
          program.name program_name,
          MIN(payment.NU_DSE_SCHEDULE_DATE_C) next_draft_date
    from {{ params.refined_database_name }}.SALESFORCE.NU_DSE_PROGRAM_C program
    join {{ params.refined_database_name }}.SALESFORCE.NU_DSE_PAYMENT_C payment on payment.NU_DSE_PROGRAM_C = program.id
    where payment.NU_DSE_PAYMENT_TYPE_C = 'Deposit'
    and payment.NU_DSE_TRANSACTION_STATUS_C in ('Scheduled')
    group by program.id, program.name
  ),
  num_of_settlements as (
    select tl.NU_DSE_PROGRAM_C program_id,
          count(*) ct_of_settlements,
          SUM(cast(tl.creditor_payments_outstanding_c as decimal(10, 2))) term_pay_balance,
          SUM(tl.fees_outstanding_c) fees_outstanding
    from {{ params.refined_database_name }}.SALESFORCE.NU_DSE_TRADE_LINE_C tl
    join {{ params.refined_database_name }}.SALESFORCE.NU_DSE_OFFER_C offer on offer.NU_DSE_TRADE_LINE_C = tl.id
    where lower(offer.NU_DSE_STATUS_C) like '%accepted%'
    group by 1
  ),
  active_debts as (
    select tl.NU_DSE_PROGRAM_C program_id,
          count(distinct tl.id) active_debts,
          sum(cast(nvl(nvl(tl.VERIFIED_BALANCE_2_C, tl.NU_DSE_CURRENT_BALANCE_C), tl.NU_DSE_ORIGINAL_DEBT_C) as decimal(38, 2))) current_debt_balance,
          sum(nvl(offer.NU_DSE_OFFER_AMOUNT_C, 0)) offer_amount,
          case
              when sum(nvl(offer.NU_DSE_OFFER_AMOUNT_C, 0)) is not null then
                sum(cast(nvl(nvl(tl.VERIFIED_BALANCE_2_C, tl.NU_DSE_CURRENT_BALANCE_C), tl.NU_DSE_ORIGINAL_DEBT_C) as decimal(38, 2)))
              else 0 end Unsettled_Debt
    from {{ params.refined_database_name }}.SALESFORCE.NU_DSE_TRADE_LINE_C tl
    left join {{ params.refined_database_name }}.SALESFORCE.NU_DSE_OFFER_C offer on offer.NU_DSE_TRADE_LINE_C = tl.id
        and lower(offer.NU_DSE_STATUS_C) like '%accepted%'
    where tl.NU_DSE_INCLUDE_IN_THE_PROGRAM_C = true
    and offer.id is null
    group by 1
  ),
  Settlement_Schedule as (
    Select PROGRAM_NAME,PROGRAM_ID, OFFER_ID,OFFER_NAME, TRANSACTION_STATUS, TRANSACTION_TYPE,-1*SUM(TRANSACTION_AMOUNT) as Amt
    FROM {{ params.curated_database_name }}.CFT.ACTUAL_TRANSACTION
    WHERE OFFER_ID is not null and TRANSACTION_TYPE in ('Settlement Fee','Payment')
          and TRANSACTION_STATUS in ('Completed','Scheduled','In_Transit','Pending')
          and IS_CURRENT_RECORD_FLAG=TRUE
    GROUP BY PROGRAM_NAME, PROGRAM_ID, OFFER_ID, OFFER_NAME, TRANSACTION_STATUS, TRANSACTION_TYPE
  ),
  tl_list as (
    select tl.TRADELINE_NAME,
    tl.PROGRAM_NAME,
    coalesce(c_orig.creditor_name, ca_orig.creditor_alias_name) original_creditor,
    coalesce(tl.collection_agency_parent_name,tl.collection_agency_name, c_curr.creditor_name,ca_curr.creditor_alias_name,c_orig.creditor_name,ca_orig.creditor_alias_name) current_creditor,
    case when tl.negotiation_balance = 0 then null else cast(tl.NEGOTIATION_BALANCE as decimal(18,2)) end as NEGOTIATION_BALANCE,
    cast(tl2.NU_DSE_ORIGINAL_DEBT_C as decimal(18,2)) original_balance,
    SSP.Amt CREDITOR_PAYMENTS_OUTSTANDING_AMOUNT,
    cast(tl2.estimated_fee_c as decimal(18,2)) estimated_fees,
    SSF.Amt fees_outstanding_amount,
    offer.offer_name,
    offer.settlement_amount,
    nvl(tl2.NU_DSE_NEW_ACCOUNT_NUMBER_C, tl2.NU_DSE_ORIGINAL_ACCOUNT_NUMBER_C) account_number,
    tl.tradeline_settlement_status,
    tl.tradeline_settlement_sub_status,
    tl.INCLUDE_IN_PROGRAM_FLAG,
    tl.FEE_BASIS_BALANCE
    from {{ params.curated_database_name }}.crm.TRADELINE tl
    left join {{ params.refined_database_name }}.SALESFORCE.NU_DSE_TRADE_LINE_C_VW tl2 on tl.TRADELINE_ID = tl2.id
    left join {{ params.curated_database_name }}.crm.offer offer on tl.TRADELINE_NAME = offer.TRADELINE_NAME
              and offer.IS_CURRENT_RECORD_FLAG = true
              and offer.IS_CURRENT_OFFER = true
    left join {{ params.curated_database_name }}.crm.creditor c_orig  on tl.original_creditor_id = c_orig.creditor_id
              and c_orig.is_current_record_flag
    left join {{ params.curated_database_name }}.crm.creditor_alias ca_orig on tl.original_creditor_alias_id = ca_orig.creditor_alias_id
              and ca_orig.is_current_record_flag
    left join {{ params.curated_database_name }}.crm.creditor c_curr on tl.current_creditor_id = c_curr.creditor_id
              and c_curr.is_current_record_flag
    left join {{ params.curated_database_name }}.crm.creditor_alias ca_curr on tl.current_creditor_alias_id = ca_curr.creditor_alias_id
              and ca_curr.is_current_record_flag
    left join Settlement_Schedule SSP on offer.OFFER_ID=SSP.OFFER_ID
              and SSP.TRANSACTION_STATUS ='Scheduled' and SSP.TRANSACTION_TYPE='Payment'
    left join Settlement_Schedule SSF on offer.OFFER_ID=SSF.OFFER_ID
              and SSF.TRANSACTION_STATUS ='Scheduled' and SSF.TRANSACTION_TYPE='Settlement Fee'
    where tl.IS_CURRENT_RECORD_FLAG = true
    and not(tl.tradeline_settlement_status = 'SETTLED' and tl.tradeline_settlement_sub_status = 'PAID OFF')
  ),
  program_no_cred as (
        select distinct
            program_name,
            original_creditor,
            current_creditor
        from tl_list
        where 1=1
            and program_name is not null
            and original_creditor is null
            and current_creditor is null
    ),
  historical_settlement_percent as (
    select
        PROGRAM_NAME
        , sum(coalesce(tl_list.NEGOTIATION_BALANCE, tl_list.FEE_BASIS_BALANCE, tl_list.ORIGINAL_BALANCE)) AS SETTLED_BALANCE_TOTAL
        , sum(tl_list.SETTLEMENT_AMOUNT) AS SETTLEMENT_AMOUNT_TOTAL
        , case when SETTLED_BALANCE_TOTAL > 0 then (SETTLEMENT_AMOUNT_TOTAL / SETTLED_BALANCE_TOTAL) else null end AS HISTORICAL_SETTLEMENT_PERCENT
    from tl_list
    where TRADELINE_SETTLEMENT_STATUS = 'SETTLED'
    and TRADELINE_SETTLEMENT_SUB_STATUS NOT ILIKE 'BUSTED%'
    group by 1
  ),
  deferral as (
    select t.*,
          ((datediff(month, min_dt, max_dt) + 1) - ct) deferral_ct
    from (
          select
            program.id program_id,
            min(last_day(payment.NU_DSE_SCHEDULE_DATE_C)) min_dt,
            max(last_day(payment.NU_DSE_SCHEDULE_DATE_C)) max_dt,
            count(distinct last_day(payment.NU_DSE_SCHEDULE_DATE_C)) ct
          from {{ params.refined_database_name }}.SALESFORCE.NU_DSE_PROGRAM_C program
          join {{ params.refined_database_name }}.SALESFORCE.NU_DSE_PAYMENT_C payment on payment.NU_DSE_PROGRAM_C = program.id
          where payment.NU_DSE_PAYMENT_TYPE_C in ('Deposit', 'Draft')
          and payment.NU_DSE_TRANSACTION_STATUS_C = 'Completed'
          group by program.id) t
  ),
  payments as (
    select * from (
        select
          program.id program_id,
          program.NU_DSE_PAYMENT_FREQUENCY_C,
          payment.NU_DSE_SCHEDULE_DATE_C payment_date,
          row_number() over (partition by program_id order by payment_date desc) seq,
          count(payment.NU_DSE_SCHEDULE_DATE_C)
             over (partition by program_id, last_day(payment.NU_DSE_SCHEDULE_DATE_C)
             order by payment.NU_DSE_SCHEDULE_DATE_C desc) payments_count
        from {{ params.refined_database_name }}.SALESFORCE.NU_DSE_PROGRAM_C program
        join {{ params.refined_database_name }}.SALESFORCE.NU_DSE_PAYMENT_C payment on payment.NU_DSE_PROGRAM_C = program.id
        where payment.NU_DSE_PAYMENT_TYPE_C in ('Deposit', 'Draft')
        and payment.NU_DSE_TRANSACTION_STATUS_C = 'Completed')
    where seq > case when NU_DSE_PAYMENT_FREQUENCY_C = 'Monthly' then 6 else 12 end
    order by program_id
  ),
  recent_payments as (
    select * from (
        select
          program.id program_id,
          program.NU_DSE_PAYMENT_FREQUENCY_C,
          payment.NU_DSE_SCHEDULE_DATE_C payment_date,
          datediff(month,payment.NU_DSE_SCHEDULE_DATE_C,$V_CALENDAR_DATE_CST),
          row_number() over (partition by program_id order by payment_date desc) seq,
          count(payment.NU_DSE_SCHEDULE_DATE_C)
             over (partition by program_id, last_day(payment.NU_DSE_SCHEDULE_DATE_C)
             order by payment.NU_DSE_SCHEDULE_DATE_C desc) payments_count
        from {{ params.refined_database_name }}.SALESFORCE.NU_DSE_PROGRAM_C program
        join {{ params.refined_database_name }}.SALESFORCE.NU_DSE_PAYMENT_C payment on payment.NU_DSE_PROGRAM_C = program.id
        where payment.NU_DSE_PAYMENT_TYPE_C in ('Deposit', 'Draft')
        and payment.NU_DSE_TRANSACTION_STATUS_C in ('Completed','In Progress','In Process')
            )
     where seq = case when NU_DSE_PAYMENT_FREQUENCY_C = 'Monthly' then 3 else 6 end
     and datediff(month,payment_date,$V_CALENDAR_DATE_CST) < 4
  ),
  payment_dates as (
    select * from
      (select * from (
          select
            program.id program_id,
            program.NU_DSE_PAYMENT_FREQUENCY_C,
            payment.NU_DSE_SCHEDULE_DATE_C,
            row_number() over (partition by program.id order by payment.NU_DSE_SCHEDULE_DATE_C desc) seq
          from {{ params.refined_database_name }}.SALESFORCE.NU_DSE_PROGRAM_C program
          join {{ params.refined_database_name }}.SALESFORCE.NU_DSE_PAYMENT_C payment on payment.NU_DSE_PROGRAM_C = program.id
          where payment.NU_DSE_PAYMENT_TYPE_C in ('Deposit', 'Draft')
          and payment.NU_DSE_TRANSACTION_STATUS_C = 'Completed')
       where 1 = 1
       and seq <= case when NU_DSE_PAYMENT_FREQUENCY_C = 'Monthly' then 1 else 2 end) tt
       pivot (max(nu_dse_schedule_date_c) for seq in (1,2)) as p (program_id, payment_frequency, Payment_date1, payment_date2)
  ),
  schedule_adherence as (
    select
      *
      , case when (total_payments) <> 0 then cast(successful_payments as float)/(total_payments) end as schedule_adherence
    from (
      select distinct program.id program_id,
           sum(case when payment.NU_DSE_SCHEDULE_DATE_C < $V_CALENDAR_DATE_CST and nu_dse_transaction_status_c not in ('Failed', 'Cancelled', 'Scheduled')
               then 1 else 0 end) over(partition by program_id) as successful_payments,
           sum(case when payment.NU_DSE_SCHEDULE_DATE_C < $V_CALENDAR_DATE_CST and nu_dse_transaction_status_c in ('Failed', 'Cancelled')
               then 1 else 0 end) over(partition by program_id) as incomplete_payments,
           sum(case when payment.NU_DSE_SCHEDULE_DATE_C < $V_CALENDAR_DATE_CST
               then 1 else null end) over(partition by program_id) as total_payments
      from {{ params.refined_database_name }}.SALESFORCE.NU_DSE_PROGRAM_C program
      join {{ params.refined_database_name }}.SALESFORCE.NU_DSE_PAYMENT_C payment on payment.NU_DSE_PROGRAM_C = program.id
      where payment.NU_DSE_PAYMENT_TYPE_C in ('Deposit', 'Draft')) a
  ),
  creditor_settlements as (
    select
        original_creditor
        , current_creditor
        , sum(settlement_amount)    "Settlement Amount"
        , sum(tradeline_original_amount)    "Tradeline Original Amount"
        , avg(settlement_pct_original)      settlement_pct_original
        , round(avg(settlement_amount)) average_settlement_amt
        , count(tradeline_name)       tradeline_count
        , avg(settlement_pct_original) average_settlement_pct
        , percentile_cont(0.25) within group (order by settlement_pct_original) bottom_25_percent_settlement_pct
        , percentile_cont(0.50) within group (order by settlement_pct_original) middle_50_percent_settlement_pct
        , percentile_cont(0.75) within group (order by settlement_pct_original) top_25_percent_settlement_pct
        , percentile_cont(0.9) within group (order by settlement_pct_original) top_90_percent_settlement_pct
    from (
          select distinct --ifnull(t.original_creditor_parent_name,t.original_creditor_name) original_creditor
                coalesce(c_orig.creditor_name,ca_orig.creditor_alias_name) original_creditor
                -- , coalesce(t.collection_agency_parent_name,t.collection_agency_name, t.current_creditor_parent_name,t.current_creditor_name,t.original_creditor_parent_name,t.original_creditor_name) current_creditor
                , coalesce(t.collection_agency_parent_name,t.collection_agency_name, c_curr.creditor_name,ca_curr.creditor_alias_name,c_orig.creditor_name,ca_orig.creditor_alias_name) current_creditor
                , o.tradeline_original_amount
                , o.settlement_amount
                , case when coalesce(t.NEGOTIATION_BALANCE, t.FEE_BASIS_BALANCE, t.ENROLLED_BALANCE) <> 0 then (cast(o.settlement_amount as float)/ coalesce(t.NEGOTIATION_BALANCE, t.FEE_BASIS_BALANCE, t.ENROLLED_BALANCE)) end as settlement_pct_original
                , case when o.tradeline_original_amount <> 0 then (cast(o.settlement_service_fee as float)/ o.tradeline_original_amount) end as settlement_fee_pct_original
                , o.settlement_service_fee
                , o.tradeline_name
                , o.offer_accepted_date_cst
          from {{ params.curated_database_name }}.crm.offer o
          left join {{ params.curated_database_name }}.crm.tradeline t on o.tradeline_name = t.tradeline_name
          left join {{ params.curated_database_name }}.crm.creditor c_orig on t.original_creditor_id = c_orig.creditor_id
              and c_orig.is_current_record_flag
          left join {{ params.curated_database_name }}.crm.creditor_alias ca_orig on t.original_creditor_alias_id = ca_orig.creditor_alias_id
              and ca_orig.is_current_record_flag
          left join {{ params.curated_database_name }}.crm.creditor c_curr on t.current_creditor_id = c_curr.creditor_id
              and c_curr.is_current_record_flag
          left join {{ params.curated_database_name }}.crm.creditor_alias ca_curr on t.current_creditor_alias_id = ca_curr.creditor_alias_id
              and ca_curr.is_current_record_flag
          where datediff(day,o.offer_accepted_date_cst,$V_CALENDAR_DATE_CST) <= 90
          and o.is_current_record_flag = 'TRUE'
          and o.is_current_offer = 'TRUE'
          and t.is_current_record_flag = 'TRUE'
          and o.CFT_PAYMENT_SCHEDULED_CREATED_DATE_CST is not null) a
    where settlement_pct_original < 1 and settlement_pct_original > 0.05
    group by original_creditor
            , current_creditor
  ),
  fee_template as (
      select
          distinct NU_DSE_PROGRAM_C.ID program_id
          , NU_DSE_PROGRAM_C.NAME
          , nu_dse_settlement_fee_percentage_c settlement_fee_pct
      from  {{ params.refined_database_name }}.SALESFORCE.NU_DSE_PROGRAM_C
      inner join {{ params.refined_database_name }}.SALESFORCE.NU_DSE_FEE_TEMPLATE_C_VW on NU_DSE_PROGRAM_C.NU_DSE_FEE_TEMPLATE_C = NU_DSE_FEE_TEMPLATE_C_VW.ID
      where NU_DSE_PROGRAM_C.is_deleted_flag = FALSE
      and NU_DSE_FEE_TEMPLATE_C_VW.is_deleted_flag = FALSE
   ),
  termination_requested as (
      select distinct coalesce(SALESFORCE_PROGRAM_ID, NU_DSE_PROGRAM_C_ID) AS program_id
      from {{ params.refined_database_name }}.FIVE9.CALL
      where DISPOSITION ILIKE '%term%'
  ),
  dnc as (
      select distinct cast(dnc_number as nvarchar) dnc_number
      from {{ params.refined_database_name }}.FIVE9.DNC
  ),
  prior_loan_applicant as (
      SELECT DISTINCT
              P.ID AS PROGRAM_ID
            , P.LOAN_INTEREST_STATUS_C
            , P.LOAN_INTEREST_RESPONSE_DATE_C_CST
            , DSMR.CURRENT_STATUS
            , DSMR.APP_SUBMIT_DATE
      FROM {{ params.refined_database_name }}.SALESFORCE.NU_DSE_PROGRAM_C AS P
      LEFT JOIN (
                  SELECT *
                  FROM {{ params.refined_database_name }}.ABOVE_LENDING.AGL_COMBINED_DETAIL
                  QUALIFY rank() OVER (PARTITION BY PROGRAM_NAME ORDER BY APP_SUBMIT_DATE DESC) = 1
                ) DSMR ON DSMR.PROGRAM_NAME = P.NAME
      WHERE P.LOAN_INTEREST_STATUS_C IN ('Graduated', 'Funded')
            OR DSMR.CURRENT_STATUS IN ('ONBOARDED')
            OR (DSMR.CURRENT_STATUS IN ('BACK_END_DECLINED') AND datediff(DAY, DSMR.APP_SUBMIT_DATE, current_date) <= 90)
            OR (DSMR.CURRENT_STATUS IN ('FRONT_END_DECLINED') AND datediff(DAY, DSMR.APP_SUBMIT_DATE, current_date) <= 90)
  ),
  cft_monthly_fees as (
      select programs.id as program_id
          , SUM(AT.TRANSACTION_AMOUNT) as cft_monthly_fees
      FROM {{ params.refined_database_name }}.SALESFORCE.NU_DSE_PROSPECT_C AS prospects
      LEFT JOIN {{ params.refined_database_name }}.SALESFORCE.NU_DSE_PROGRAM_C AS programs ON programs.PROSPECT_ID_C = prospects.ID
      left join {{ params.curated_database_name }}.CFT.ACTUAL_TRANSACTION AT on programs.id = AT.PROGRAM_ID
      where AT.TRANSACTION_STATUS='Completed'
      AND AT.transaction_group='Fee'
      AND AT.transaction_type='Monthly Service Fees'
      and datediff(month,TRANSACTION_DATE_CST,$V_CALENDAR_DATE_CST) = 1
      GROUP BY programs.id
  ),
  blp_monthly_fee as (
      select programs.id as program_id
          , SUM(AT.TRANSACTION_AMOUNT) as blp_monthly_fee
      FROM {{ params.refined_database_name }}.SALESFORCE.NU_DSE_PROSPECT_C AS prospects
      LEFT JOIN {{ params.refined_database_name }}.SALESFORCE.NU_DSE_PROGRAM_C AS programs ON programs.PROSPECT_ID_C = prospects.ID
      left join {{ params.curated_database_name }}.CFT.ACTUAL_TRANSACTION AT on programs.id = AT.PROGRAM_ID
      where AT.TRANSACTION_STATUS='Completed'
      AND AT.transaction_group='Fee'
      AND AT.transaction_type='Monthly Legal Service Fee'
      and datediff(month,TRANSACTION_DATE_CST,$V_CALENDAR_DATE_CST) = 1
      GROUP BY programs.id
  ),
  cft_account_balance as (
      select distinct programs.id as program_id
              , DA.CURRENT_BALANCE
              , DA.AVAILABLE_BALANCE
      FROM {{ params.refined_database_name }}.SALESFORCE.NU_DSE_PROSPECT_C AS prospects
      LEFT JOIN {{ params.refined_database_name }}.SALESFORCE.NU_DSE_PROGRAM_C AS programs ON programs.PROSPECT_ID_C = prospects.ID
      LEFT JOIN {{ params.curated_database_name }}.CFT.DEPOSIT_ACCOUNT_BALANCE DA ON programs.id=DA.program_id
      where AS_OF_DATE_CST = $V_CALENDAR_DATE_CST
  ),
cft_prior_month_payment as (
    select programs.id as program_id
           ,  SUM(AT.TRANSACTION_AMOUNT) as cft_monthly
    FROM {{ params.refined_database_name }}.SALESFORCE.NU_DSE_PROSPECT_C AS prospects
    LEFT JOIN {{ params.refined_database_name }}.SALESFORCE.NU_DSE_PROGRAM_C AS programs ON programs.PROSPECT_ID_C = prospects.ID
    left join {{ params.curated_database_name }}.CFT.ACTUAL_TRANSACTION AT on programs.id = AT.PROGRAM_ID
    where AT.TRANSACTION_STATUS='Completed'
    AND AT.transaction_group='Deposit'
    AND AT.transaction_type='Deposit'
    and datediff(month,TRANSACTION_DATE_CST,$V_CALENDAR_DATE_CST) = 1
    GROUP BY programs.id
),
  fico_score as (
    select distinct program.id as program_id
        , coalesce(fp.nu_dse_credit_score_c,program.credit_score_c) as credit_score
    from {{ params.refined_database_name }}.salesforce.nu_dse_program_c_vw program
    left join {{ params.refined_database_name }}.salesforce.nu_dse_prospect_c_vw prospect on program.prospect_id_c = prospect.id
    left join {{ params.refined_database_name }}.salesforce.nu_dse_financial_profile_c_vw fp on prospect.id = fp.nu_dse_prospect_c
    where coalesce(fp.nu_dse_credit_score_c,program.credit_score_c) is not null
  ),
  new_or_aged_book as (
    select distinct program.id as program_id
         ,case when datediff(month,program.enrolled_date_c,$V_CALENDAR_DATE_CST) >= 6 then 'Aged Book'
                when datediff(month,program.enrolled_date_c,$V_CALENDAR_DATE_CST) < 6 then 'New Book'
         end as Program_Age_Bucket
    ,program.enrolled_date_c
    from {{ params.refined_database_name }}.salesforce.nu_dse_program_c_vw program
  ),
  beyond_fees as (
    select distinct program_id
        , sum(case when transaction_status = 'Scheduled' then TOTAL_AMOUNT * -1 else 0 end) as remaining_beyond_fees
    from {{ params.curated_database_name }}.crm.scheduled_transaction
    where transaction_type = 'Fee Withdrawal'
    and is_current_record_flag = 'TRUE'
    group by program_id
  ),
fees_outstanding as (
    select program_id
          , sum(TOTAL_AMOUNT * -1) as fees_outstanding
    from {{ params.curated_database_name }}.crm.scheduled_transaction
    where is_current_record_flag = 'TRUE'
    and transaction_status = 'Scheduled'
    and transaction_type = 'Fee'
    group by program_id
  ),
  CCPA_phone as  (
    select distinct REPLACE(regexp_replace(contact.phone,'[.,\/#!$%\^&\*:{}=\_`~()-]'), ' ', '') as CCPA_Phone
    from {{ params.refined_database_name }}.salesforce.compliance_request_c_vw compliance
    left join {{ params.refined_database_name }}.salesforce.contact on compliance.contact_c = contact.id
    and contact.phone is not null
    union
    select REPLACE(regexp_replace(contact.mobile_phone,'[.,\/#!$%\^&\*:{}=\_`~()-]'), ' ', '') as CCPA_Phone
    from {{ params.refined_database_name }}.salesforce.compliance_request_c_vw compliance
    left join {{ params.refined_database_name }}.salesforce.contact on compliance.contact_c = contact.id
    and contact.mobile_phone is not null
    union
    select REPLACE(regexp_replace(contact.home_phone,'[.,\/#!$%\^&\*:{}=\_`~()-]'), ' ', '') as CCPA_Phone
    from {{ params.refined_database_name }}.salesforce.compliance_request_c_vw compliance
    left join {{ params.refined_database_name }}.salesforce.contact on compliance.contact_c = contact.id
    and contact.home_phone is not null
  ),
  CCPA_email as  (
    select distinct compliance.from_address_c as CCPA_Email
    from {{ params.refined_database_name }}.salesforce.compliance_request_c_vw compliance
    left join {{ params.refined_database_name }}.salesforce.contact on compliance.contact_c = contact.id
    and compliance.from_address_c is not null
    union
    select contact.email as CCPA_Email
    from {{ params.refined_database_name }}.salesforce.compliance_request_c_vw compliance
    left join {{ params.refined_database_name }}.salesforce.contact on compliance.contact_c = contact.id
    and contact.email is not null
  ),
  Full_payments as  (
    select program_id,
           program_name,
           count(*)
    from (
        select *,
              row_number() over (partition by program_id order by pay_month desc) as seqnum1
        from (
            select distinct scheduled_transaction.program_id,
                   scheduled_transaction.program_name,
                   last_day(scheduled_transaction.scheduled_date_cst) pay_month,
                    program.monthly_principal_amount_including_fees as Planned_Deposits,
                    sum(case when scheduled_transaction.transaction_status in ('Completed', 'Cleared') then scheduled_transaction.Total_Amount else 0 end) as Completed_Deposits
            from {{ params.curated_database_name }}.crm.scheduled_transaction
            inner join {{ params.curated_database_name }}.crm.program
            on scheduled_transaction.program_id = program.program_id
            and scheduled_transaction.is_current_record_flag = TRUE
            and program.is_current_record_flag = TRUE
            where scheduled_transaction.transaction_type = 'Deposit'
            and scheduled_transaction.transaction_status in ('Completed', 'Cleared')
            and last_day(scheduled_transaction.scheduled_date_cst) < last_day($V_CALENDAR_DATE_CST)
            group by scheduled_transaction.program_id
                    , scheduled_transaction.program_name
                    , last_day(scheduled_transaction.scheduled_date_cst)
                    , program.monthly_principal_amount_including_fees
            having completed_deposits > 0
          ) tt
        where completed_deposits >= planned_deposits
        qualify seqnum1 <= 3
      ) ttt
  where datediff(month,pay_month,$V_CALENDAR_DATE_CST) <= 3
  group by program_id
          , program_name
  having count(*) = 3
  ),


    Deposit_Adherence as (
        With

        Dates as (
            Select
            90 Term, DATEADD('day',-90,CURRENT_DATE()) as StartDate, CURRENT_DATE() as EndDate
            Union All
            Select
            120 Term, DATEADD('day',-120,CURRENT_DATE()) as StartDate, CURRENT_DATE() as EndDate
            Union All
            Select
            180 Term,  DATEADD('day',-180,CURRENT_DATE()) as StartDate, CURRENT_DATE() as EndDate
        ),


        Prg as (
            Select P.PROGRAM_NAME, Last_Day(P.ENROLLED_DATE_CST) as Vintage, P.ENROLLED_DATE_CST, P.PROGRAM_STATUS, E.EFFECTIVE_DATE
            FROM {{ params.curated_database_name }}.CRM.PROGRAM P
            LEFT JOIN (
                Select S.PROGRAM_NAME, min(S.RECORD_EFFECTIVE_START_DATE_TIME_CST) as EFFECTIVE_DATE
                From {{ params.curated_database_name }}.CRM.SCHEDULED_TRANSACTION S
                LEFT Join (
                    Select PROGRAM_NAME, ENROLLED_DATE_CST FROM {{ params.curated_database_name }}.CRM.PROGRAM WHERE IS_CURRENT_RECORD_FLAG = TRUE) P
                    On S.PROGRAM_NAME = P.PROGRAM_NAME
                Where TRANSACTION_TYPE = 'Deposit' AND SCHEDULED_DATE_CST is not NULL AND TRANSACTION_NUMBER is not NULL
                  AND S.SCHEDULED_DATE_CST>= P.ENROLLED_DATE_CST
                GROUP BY S.PROGRAM_NAME) E on P.PROGRAM_NAME = E.PROGRAM_NAME
            WHERE P.IS_CURRENT_RECORD_FLAG = TRUE and P.ORIGINAL_SOURCE_SYSTEM='LEGACY'),

        Schedules as (
            Select Term,StartDate,EndDate,PROGRAM_NAME, TRANSACTION_TYPE, ENROLLED_DATE_CST,
                       count(TOTAL_AMOUNT) SCHEDULED_COUNT,Sum(TOTAL_AMOUNT) as SCHEDULED_AMOUNT
                       from (


                        Select Dates.Term,Dates.StartDate,Dates.EndDate,s.PROGRAM_NAME, s.TRANSACTION_TYPE, s.TOTAL_AMOUNT, s.TRANSACTION_STATUS, s.SCHEDULED_DATE_CST, P.ENROLLED_DATE_CST,
                               row_number() over (PARTITION BY TRANSACTION_NUMBER,Dates.Term ORDER BY RECORD_EFFECTIVE_START_DATE_TIME_CST DESC) rnk
                            From {{ params.curated_database_name }}.CRM.SCHEDULED_TRANSACTION S
                        Inner Join Prg P On S.PROGRAM_NAME = P.PROGRAM_NAME
                        CROSS JOIN Dates
                        WHERE 1=1
                          AND S.RECORD_EFFECTIVE_START_DATE_TIME_CST<=greatest(Dates.StartDate,P.EFFECTIVE_DATE)
                        Qualify rnk = 1)
            Where TRANSACTION_STATUS  in ('Scheduled', 'In Progress', 'Pending', 'Tentative','Completed','Failed','Processing Failed','Returned','In_Transit','Suspended')
            AND TRANSACTION_TYPE = 'Deposit' and SCHEDULED_DATE_CST>= StartDate AND SCHEDULED_DATE_CST<= EndDate
            Group BY Term,PROGRAM_NAME, TRANSACTION_TYPE, ENROLLED_DATE_CST, StartDate, EndDate
            ),

        Actuals as (
            Select Term,StartDate, EndDate, PROGRAM_NAME, ENROLLED_DATE_CST,
                   count(TRANSACTION_AMOUNT) ACTUAL_COUNT,Sum(TRANSACTION_AMOUNT) as ACTUAL_AMOUNT
                   from (
                       Select Dates.Term, Dates.StartDate,Dates.EndDate,A.PROGRAM_NAME, A.TRANSACTION_TYPE, A.TRANSACTION_AMOUNT, A.TRANSACTION_STATUS, S.SCHEDULED_DATE_CST As TRANSACTION_DATE_CST, P.ENROLLED_DATE_CST
                       From {{ params.curated_database_name }}.CFT.ACTUAL_TRANSACTION A
                       CROSS JOIN Dates
                       LEFT JOIN {{ params.curated_database_name }}.CRM.SCHEDULED_TRANSACTION S ON S.TRANSACTION_NUMBER=A.TRANSACTION_NUMBER AND s.IS_CURRENT_RECORD_FLAG = TRUE
                       Inner Join Prg P On A.PROGRAM_NAME = P.PROGRAM_NAME
                       WHERE S.SCHEDULED_DATE_CST>= Dates.StartDate AND S.SCHEDULED_DATE_CST<= Dates.EndDate
                         AND A.IS_CURRENT_RECORD_FLAG = TRUE
                         )
            Where upper(TRANSACTION_TYPE) in ('DEPOSIT') And upper(TRANSACTION_STATUS) in ('COMPLETED','IN_TRANSIT','SCHEDULED','IN PROGRESS')
            Group BY Term, PROGRAM_NAME, ENROLLED_DATE_CST, StartDate,EndDate
            )

        Select P.PROGRAM_NAME,Dates.Term, any_value(Dates.StartDate), any_value(Dates.EndDate),any_value(Vintage), nvl(sum(A.ACTUAL_AMOUNT),0) as ACTUAL_AMOUNT ,
          sum(S.SCHEDULED_AMOUNT) as SCHEDULED_AMOUNT,
          case when sum(S.SCHEDULED_AMOUNT)>0 then nvl(sum(A.ACTUAL_AMOUNT),0) / sum(S.SCHEDULED_AMOUNT) end as DepAdherence
        From Prg P
        Inner JOIN Schedules S on P.PROGRAM_NAME = S.PROGRAM_NAME
        INNER JOIN Dates On S.Term = Dates.Term
        LEFT JOIN Actuals A on P.PROGRAM_NAME = A.PROGRAM_NAME AND S.StartDate=A.StartDate AND A.Term = S.Term and A.Term = Dates.Term
        WHERE P.PROGRAM_STATUS not in ('Closed','Sold to 3rd Party')
        GROUP BY Dates.Term, P.PROGRAM_NAME
    ),

  bank_account as (
    select
      NU_DSE_HOLDER_S_NAME_C,
      NU_DSE_BANK_NAME_C,
      NU_DSE_BANK_ACCOUNT_NUMBER_C,
      NU_DSE_ROUTING_NUMBER_C,
      NU_DSE_ACCOUNT_TYPE_C,
      NU_DSE_PROGRAM_C
    from {{ params.refined_database_name }}.salesforce.NU_DSE_BANK_ACCOUNT_C bank_account
    where lower(NU_DSE_BANK_EXTERNAL_STATUS_C) = 'active'
    and lower(NU_DSE_BANK_EXTERNAL_STATUS_REASON_C) = 'verified'
    and lower(BANK_ACCOUNT_VALIDATION_STATUS_C) = 'valid'
  ),
  remaining_deposits as (
    select
        T.PROGRAM_NAME,
        sum(TOTAL_AMOUNT) AS BF_REMAINING_DEPOSITS
    from {{ params.curated_database_name }}.CRM.SCHEDULED_TRANSACTION T
    join {{ params.curated_database_name }}.CRM.PROGRAM P ON T.PROGRAM_NAME = P.PROGRAM_NAME AND P.IS_CURRENT_RECORD_FLAG
    where T.IS_CURRENT_RECORD_FLAG
    and TRANSACTION_TYPE = 'Deposit'
    and SCHEDULED_DATE_CST::DATE > $V_CALENDAR_DATE_CST
    and TRANSACTION_STATUS IS DISTINCT FROM 'Cancelled'
    Group by 1
    ),
  SwapCo_client as (

  Select
  program.id client_id,
        act.NU_DSE_CO_FIRST_NAME_C First_Name_CO,
        act.NU_DSE_CO_LAST_NAME_C Last_Name_CO,
        coalesce(act.NU_DSE_CO_ADDRESS_LINE_C,act.PERSON_MAILING_STREET) Mailing_Address_CO,
        coalesce(act.NU_DSE_CO_CITY_C,act.PERSON_MAILING_CITY) CITY_CO,
        coalesce(act.NU_DSE_CO_STATE_C,act.PERSON_MAILING_STATE) STATE_CO,
        coalesce(act.NU_DSE_CO_POSTAL_CODE_C,act.PERSON_MAILING_POSTAL_CODE) ZIP_Code_CO,
        coalesce(act.NU_DSE_CO_EMAIL_ADDRESS_C,program.email_address_c ) Email_Address_CO,
        coalesce(REPLACE(regexp_replace(nvl(act.NU_DSE_CO_CELL_PHONE_C, nvl(act.NU_DSE_CO_HOME_PHONE_C, act.NU_DSE_CO_WORK_PHONE_C)),'[.,\/#!$%\^&\*:{}=\_`~()-]'), ' ', ''),
            REPLACE(regexp_replace(nvl(program.cell_phone_c, nvl(program.home_phone_c, program.work_phone_c)),'[.,\/#!$%\^&\*:{}=\_`~()-]'), ' ', '')) Telephone_Number_CO,
        act.NU_DSE_CO_DATE_OF_BIRTH_C DOB_CO,
        act.NU_DSE_CO_SSN_C SSN_CO,
        coalesce(program.CO_CLIENT_CREDIT_SCORE_C,PR.CO_CLIENT_CREDIT_SCORE_C) FICO
  FROM {{ params.refined_database_name }}.SALESFORCE.NU_DSE_PROGRAM_C program
  join {{ params.refined_database_name }}.SALESFORCE.ACCOUNT act on act.id = program.NU_DSE_ACCOUNT_C
  left join {{ params.refined_database_name }}.SALESFORCE.NU_DSE_PROSPECT_C pr on Pr.id=program.PROSPECT_ID_C
  WHERE
  program.has_co_client_c=TRUE and program.CO_APPLICANT_IS_PRIMARY_FOR_ABOVE_LOAN_F_C=True
      and act.NU_DSE_CO_SSN_C is not Null
      and act.NU_DSE_CO_DATE_OF_BIRTH_C is not null
      and act.NU_DSE_CO_FIRST_NAME_C is not null
      and act.NU_DSE_CO_LAST_NAME_C is not null
  and (FICO is not null or datediff(day,program.enrolled_date_c,$V_CALENDAR_DATE_CST) >= 365)
  ),
  creditor_matrix_settlement_logic as (
        --Getting DQ when creditors will first offer settlement
        with FIRST_ELIGIBLE_INFO as (

        select distinct
            CREDITOR_BUCKET_NAME,
            ORIGINAL_CREDITOR_ALIAS_ID,
            min(DAYS_DELINQUENT_MIN) as DAYS_DELINQUENT_MIN,
            NEGOTIATION_BALANCE_MIN,
            NEGOTIATION_BALANCE_MAX
        from {{ params.refined_database_name }}.CONFIGURATION.CREDITOR_TERMS
        where IPL_USE_FLAG = TRUE
        group by 1,2,4,5
        order by 1

        ),

        --Grabbing term, offer rate and min payment for intial offers
        FIRST_ELIGIBLE_INFO_2 as (

        select distinct
            a.*,
            coalesce(ct_alias_lump.current_creditor_bucketed,ct_no_alias_lump.current_creditor_bucketed) as current_creditor_bucketed,
            coalesce(ct_alias_lump.OFFER_PERCENT,ct_no_alias_lump.OFFER_PERCENT) as OFFER_PERCENT_NOT_LEGAL,
            coalesce(ct_alias_lump.OFFER_PERCENT,ct_no_alias_lump.OFFER_PERCENT) + coalesce(ct_alias_lump.LEGAL_RATE_INCREASE,ct_no_alias_lump.LEGAL_RATE_INCREASE,0) as OFFER_PERCENT_IS_LEGAL,
            coalesce(ct_alias_lump.AVG_OFFER_TERM,ct_no_alias_lump.AVG_OFFER_TERM) as AVG_OFFER_TERM,
            coalesce(ct_alias_lump.OFFER_MINIMUM_PAYMENT,ct_no_alias_lump.OFFER_MINIMUM_PAYMENT) as OFFER_MINIMUM_PAYMENT,
            coalesce(ct_alias_lump.CREDITOR_BUCKET_NAME,ct_no_alias_lump.CREDITOR_BUCKET_NAME) as CREDITOR_BUCKET_NAME_2

        from FIRST_ELIGIBLE_INFO as a

        --Only matching in IPL
        --when matching on original alias required - When lump sum offer expected (For IPL)
        left join {{ params.refined_database_name }}.CONFIGURATION.CREDITOR_TERMS as ct_alias_lump on ct_alias_lump.CREDITOR_BUCKET_NAME = a.CREDITOR_BUCKET_NAME
            and a.DAYS_DELINQUENT_MIN = ct_alias_lump.DAYS_DELINQUENT_MIN
            and a.NEGOTIATION_BALANCE_MIN = ct_alias_lump.NEGOTIATION_BALANCE_MIN
            and a.ORIGINAL_CREDITOR_ALIAS_ID = ct_alias_lump.ORIGINAL_CREDITOR_ALIAS_ID and ct_alias_lump.IPL_USE_FLAG = TRUE
        --when matching on original alias is not required - When lump sum offer expected (For IPL)
        left join {{ params.refined_database_name }}.CONFIGURATION.CREDITOR_TERMS as ct_no_alias_lump on ct_no_alias_lump.CREDITOR_BUCKET_NAME = a.CREDITOR_BUCKET_NAME
            and a.DAYS_DELINQUENT_MIN = ct_no_alias_lump.DAYS_DELINQUENT_MIN
            and a.NEGOTIATION_BALANCE_MIN = ct_no_alias_lump.NEGOTIATION_BALANCE_MIN
            and ct_no_alias_lump.ORIGINAL_CREDITOR_ALIAS_ID is null and ct_no_alias_lump.IPL_USE_FLAG = TRUE

        ),

        Tradelines as (
            Select P.SOURCE_SYSTEM,
                   P.Program_name,
                   P.ENROLLED_DATE_CST,
                   TL.TRADELINE_NAME,
                   tl.is_legal_flag,
                   coalesce(case
                                when btl.LAST_PAYMENT_DATE_C < '1990-01-01' then crl.last_activity_date_c
                                else btl.LAST_PAYMENT_DATE_C end, crl.last_activity_date_c,tl.current_last_payment_date_cst)           as last_payment_date,
                   datediff(day, coalesce(last_payment_date, p.ENROLLED_DATE_CST), current_date) - 15 as DQ,
                   coalesce(tl.CURRENT_CREDITOR_ID, tl.CURRENT_CREDITOR_ALIAS_ID, tl.ORIGINAL_CREDITOR_ID, tl.ORIGINAL_CREDITOR_ALIAS_ID) as CURRENT_CREDITOR_ID,
                   tl.ORIGINAL_CREDITOR_ALIAS_ID,

                   coalesce(tl.NEGOTIATION_BALANCE, tl.FEE_BASIS_BALANCE, tl.ENROLLED_BALANCE) as negotiation_balance
            FROM {{ params.curated_database_name }}.CRM.TRADELINE tl
                     Left Join {{ params.curated_database_name }}.CRM.PROGRAM P
                               on P.PROGRAM_NAME = TL.PROGRAM_NAME and P.IS_CURRENT_RECORD_FLAG = TRUE
                     left join {{ params.refined_database_name }}.bedrock.program_tradeline_c as btl
                               on btl.NAME = tl.TRADELINE_NAME and btl.is_deleted = 'FALSE'
                     left join {{ params.refined_database_name }}.BEDROCK.opportunity_tradeline_c ot
                               on btl.opportunity_tradeline_id_c = ot.id and ot.is_deleted = FALSE
                     left join {{ params.refined_database_name }}.BEDROCK.cr_liability_c crl
                               on crl.id = ot.cr_liability_id_c and crl.is_deleted = FALSE
            Where tl.TRADELINE_SETTLEMENT_STATUS not in ('SETTLED','ATTRITED','NOT ENROLLED')
                 and TL.IS_CURRENT_RECORD_FLAG = TRUE
                 and P.SOURCE_SYSTEM = 'LEGACY'
                 and P.PROGRAM_STATUS in ('Active','New','Enrolled') and tl.INCLUDE_IN_PROGRAM_FLAG=TRUE
        ),

        TRADELINES_W_EST_OFFER as (

          select distinct
              t.program_name,
              t.tradeline_name,
              t.negotiation_balance,
              t.CURRENT_CREDITOR_ID,
              c.creditor_name,
              t.DQ as CURRENT_DQ,
              case when cb.CREDITOR_BUCKET_NAME is null then 'N' else 'Y' end as TOP_50_CREDITOR,
              case when coalesce(ct_alias_lump.OFFER_PERCENT,ct_no_alias_lump.OFFER_PERCENT) is null then 'N' else 'Y' end as SETTLEMENT_ELIGIBLE_NOW, --i.e. meeting matrix eligibility criteria
              case
                  when SETTLEMENT_ELIGIBLE_NOW = 'Y' and t.IS_LEGAL_FLAG = FALSE then coalesce(ct_alias_lump.OFFER_PERCENT,ct_no_alias_lump.OFFER_PERCENT)
                  when SETTLEMENT_ELIGIBLE_NOW = 'Y' and t.IS_LEGAL_FLAG = TRUE then (coalesce(ct_alias_lump.OFFER_PERCENT,ct_no_alias_lump.OFFER_PERCENT) + coalesce(ct_alias_lump.LEGAL_RATE_INCREASE,ct_no_alias_lump.LEGAL_RATE_INCREASE,0))
                  when SETTLEMENT_ELIGIBLE_NOW = 'N' and t.IS_LEGAL_FLAG = FALSE then coalesce(ctf_alias_lump.OFFER_PERCENT_NOT_LEGAL,ctf_no_alias_lump.OFFER_PERCENT_NOT_LEGAL)
                  when SETTLEMENT_ELIGIBLE_NOW = 'N' and t.IS_LEGAL_FLAG = TRUE then coalesce(ctf_alias_lump.OFFER_PERCENT_IS_LEGAL,ctf_no_alias_lump.OFFER_PERCENT_IS_LEGAL)
              else null end as EST_OFFER_PERCENT,
              case
                  when SETTLEMENT_ELIGIBLE_NOW = 'Y' then coalesce(ct_alias_lump.CREDITOR_BUCKET_NAME,ct_no_alias_lump.CREDITOR_BUCKET_NAME)
                  when SETTLEMENT_ELIGIBLE_NOW = 'N' then coalesce(ctf_alias_lump.CREDITOR_BUCKET_NAME,ctf_no_alias_lump.CREDITOR_BUCKET_NAME)
              else null end as CREDITOR_BUCKET_NAME

          from TRADELINES as t
          LEFT JOIN {{ params.curated_database_name }}.CRM.CREDITOR C
              ON T.CURRENT_CREDITOR_ID = C.CREDITOR_ID AND C.IS_CURRENT_RECORD_FLAG
          left join {{ params.refined_database_name }}.CONFIGURATION.CREDITOR_BUCKETS as cb on t.CURRENT_CREDITOR_ID = cb.CURRENT_CREDITOR_ID

          --currently eligibile tradelines
          --when matching on original alias required - When lump sum offer expected (For IPL)
          left join {{ params.refined_database_name }}.CONFIGURATION.CREDITOR_TERMS as ct_alias_lump on ct_alias_lump.CREDITOR_BUCKET_NAME = cb.CREDITOR_BUCKET_NAME
              and t.DQ >= ct_alias_lump.DAYS_DELINQUENT_MIN and (ct_alias_lump.DAYS_DELINQUENT_MAX is null or t.DQ < ct_alias_lump.DAYS_DELINQUENT_MAX)
              and t.negotiation_balance >= ct_alias_lump.NEGOTIATION_BALANCE_MIN and (ct_alias_lump.NEGOTIATION_BALANCE_MAX is null or t.negotiation_balance < ct_alias_lump.NEGOTIATION_BALANCE_MAX)
              and t.ORIGINAL_CREDITOR_ALIAS_ID = ct_alias_lump.ORIGINAL_CREDITOR_ALIAS_ID and ct_alias_lump.IPL_USE_FLAG = TRUE
          --when matching on original alias is not required - When lump sum offer expected (For IPL)
          left join {{ params.refined_database_name }}.CONFIGURATION.CREDITOR_TERMS as ct_no_alias_lump on ct_no_alias_lump.CREDITOR_BUCKET_NAME = cb.CREDITOR_BUCKET_NAME
              and t.DQ >= ct_no_alias_lump.DAYS_DELINQUENT_MIN and (ct_no_alias_lump.DAYS_DELINQUENT_MAX is null or t.DQ < ct_no_alias_lump.DAYS_DELINQUENT_MAX)
              and t.negotiation_balance >= ct_no_alias_lump.NEGOTIATION_BALANCE_MIN and (ct_no_alias_lump.NEGOTIATION_BALANCE_MAX is null or t.negotiation_balance < ct_no_alias_lump.NEGOTIATION_BALANCE_MAX)
              and ct_no_alias_lump.ORIGINAL_CREDITOR_ALIAS_ID is null and ct_no_alias_lump.IPL_USE_FLAG = TRUE

          --For matching info with future eligibile programs
          --when matching on original alias required - (IPL)
          left join FIRST_ELIGIBLE_INFO_2 as ctf_alias_lump on ctf_alias_lump.CREDITOR_BUCKET_NAME = cb.CREDITOR_BUCKET_NAME
              and t.negotiation_balance >= ctf_alias_lump.NEGOTIATION_BALANCE_MIN and (ctf_alias_lump.NEGOTIATION_BALANCE_MAX is null or t.negotiation_balance < ctf_alias_lump.NEGOTIATION_BALANCE_MAX)
              and t.ORIGINAL_CREDITOR_ALIAS_ID = ctf_alias_lump.ORIGINAL_CREDITOR_ALIAS_ID
          --when no matching on original alias required - (IPL)
          left join FIRST_ELIGIBLE_INFO_2 as ctf_no_alias_lump on ctf_no_alias_lump.CREDITOR_BUCKET_NAME = cb.CREDITOR_BUCKET_NAME
              and t.negotiation_balance >= ctf_no_alias_lump.NEGOTIATION_BALANCE_MIN and (ctf_no_alias_lump.NEGOTIATION_BALANCE_MAX is null or t.negotiation_balance < ctf_no_alias_lump.NEGOTIATION_BALANCE_MAX)
              and ctf_no_alias_lump.ORIGINAL_CREDITOR_ALIAS_ID is null

        )

        select PROGRAM_NAME, TRADELINE_NAME, creditor_name,
        --add case when to limit settlement rate to 100% max
        case when ((EST_OFFER_PERCENT + 3) / 100) > 1 then 1 else ((EST_OFFER_PERCENT + 3) / 100) end as OFFER_PERCENT --adding 3% buffer
        from TRADELINES_W_EST_OFFER
        where EST_OFFER_PERCENT is not null

      )

   --new book legacy

  select distinct
      program.id client_id,
      right(program.id, 6) activation_code,
      COALESCE(swc.First_Name_CO,act.FIRST_NAME) FIRST_NAME,
      COALESCE(swc.Last_Name_CO,act.LAST_NAME) LAST_NAME,
      COALESCE(swc.Mailing_Address_CO,act.PERSON_MAILING_STREET) Mailing_Address,
      COALESCE(swc.CITY_CO,act.PERSON_MAILING_CITY) CITY,
      COALESCE(swc.STATE_CO,act.PERSON_MAILING_STATE) STATE,
      left(COALESCE(swc.ZIP_Code_CO,act.PERSON_MAILING_POSTAL_CODE),5) ZIP_Code,
      COALESCE(swc.Email_Address_CO,program.email_address_c) Email_Address,
      COALESCE(swc.Telephone_Number_CO,
      REPLACE(regexp_replace(nvl(program.cell_phone_c, nvl(program.home_phone_c, program.work_phone_c)),'[.,\/#!$%\^&\*:{}=\_`~()-]'), ' ', '')) Telephone_Number,
      COALESCE(swc.DOB_CO,prospect.nu_dse_dob_c) DOB,
      COALESCE(swc.SSN_CO,act.nu_dse_ssn_c) SSN,
      nvl(current_draft_amt.per_freq_amount, 0) DRAFT_AMOUNT,
      Case when program.NU_DSE_PAYMENT_FREQUENCY_C='Semi-Monthly' then 'Twice Monthly' else program.NU_DSE_PAYMENT_FREQUENCY_C end NU_DSE_PAYMENT_FREQUENCY_C,
      pd.payment_date1                 last_payment_date,
      pd.payment_date2                 last_payment_date2,
      cast(sum(case when upper(tl_list.tradeline_settlement_status) not in ('SETTLED','ATTRITED')
                  then (coalesce(tl_list.NEGOTIATION_BALANCE, tl_list.FEE_BASIS_BALANCE, tl_list.original_balance)*1.00) * coalesce(matrix.OFFER_PERCENT,c.top_90_percent_settlement_pct,.70)
              when upper(tl_list.tradeline_settlement_status) in ('SETTLED') and tl_list.tradeline_settlement_sub_status not like 'BUSTED%' and tl_list.tradeline_settlement_sub_status != 'PAID OFF'
                  then tl_list.CREDITOR_PAYMENTS_OUTSTANDING_AMOUNT
              when upper(tl_list.tradeline_settlement_status) in ('SETTLED') and tl_list.tradeline_settlement_sub_status like 'BUSTED%'
                  then (coalesce(tl_list.NEGOTIATION_BALANCE, tl_list.FEE_BASIS_BALANCE, tl_list.original_balance)*1.00) * coalesce(matrix.OFFER_PERCENT,c.top_90_percent_settlement_pct,.70)
              else 0 end) over (partition by program.id)
          +
          sum(case when upper(tl_list.tradeline_settlement_status) not in ('SETTLED','ATTRITED')
                      then tl_list.estimated_fees
                  when upper(tl_list.tradeline_settlement_status) in ('SETTLED') and tl_list.tradeline_settlement_sub_status != 'PAID OFF'
                      then tl_list.fees_outstanding_amount
                  else 0 end) over (partition by program.id)
          -
          cft_account_balance.available_balance as decimal(18,2))
          + 6*(nvl(CP.MONTHLY_LEGAL_SERVICE_FEE,0)+nvl(CP.MONTHLY_TRUST_ACCOUNT_SERVICE_FEE,0)) as
          Amount_Financed,

      sum(case when upper(tl_list.tradeline_settlement_status) not in ('SETTLED','ATTRITED')
                  then tl_list.estimated_fees
              when upper(tl_list.tradeline_settlement_status) in ('SETTLED') and tl_list.tradeline_settlement_sub_status != 'PAID OFF'
                  then tl_list.fees_outstanding_amount
              else 0 end) over (partition by program.id)
      + 6*(nvl(CP.MONTHLY_LEGAL_SERVICE_FEE,0)+nvl(CP.MONTHLY_TRUST_ACCOUNT_SERVICE_FEE,0))  as estimated_beyond_program_fees,

      cft_account_balance.available_balance total_deposits,

      coalesce(cast(sum(case when upper(tl_list.tradeline_settlement_status) not in ('SETTLED','ATTRITED')
                  then (coalesce(tl_list.NEGOTIATION_BALANCE, tl_list.FEE_BASIS_BALANCE, tl_list.original_balance)*1.00) * coalesce(matrix.OFFER_PERCENT,c.top_90_percent_settlement_pct,.70)
              when upper(tl_list.tradeline_settlement_status) in ('SETTLED') and tl_list.tradeline_settlement_sub_status not like 'BUSTED%' and tl_list.tradeline_settlement_sub_status != 'PAID OFF'
                  then tl_list.CREDITOR_PAYMENTS_OUTSTANDING_AMOUNT
              when upper(tl_list.tradeline_settlement_status) in ('SETTLED') and tl_list.tradeline_settlement_sub_status like 'BUSTED%'
                  then (coalesce(tl_list.NEGOTIATION_BALANCE, tl_list.FEE_BASIS_BALANCE, tl_list.original_balance)*1.00) * coalesce(matrix.OFFER_PERCENT,c.top_90_percent_settlement_pct,.70)
              else 0 end) over (partition by program.id, tl_list.tradeline_name, tl_list.current_creditor) as decimal(18,2)),0) as settlement_amount,

      tl_list.current_creditor tradeline_name,
      '' tradeline_account_number,
      program.NU_DSE_ROUTABLE_PHONE_SETTLEMENT_ROUTING_NUMBER_C cft_routing_number,
      program.NU_DSE_ROUTABLE_PHONE_SETTLEMENT_ACCOUNT_NUMBER_C cft_account_number,
      case when NU_DSE_ROUTABLE_PHONE_SETTLEMENT_ROUTING_NUMBER_C = 053101561 then 'WELLS FARGO BANK'
              when NU_DSE_ROUTABLE_PHONE_SETTLEMENT_ROUTING_NUMBER_C = 053112505 then 'AXOS BANK'
              else null
              end as cft_bank_name,
      concat(FIRST_NAME,' ',LAST_NAME) cft_account_holder_name,
      NU_DSE_EXTERNAL_CFT_REST_ID_C    external_id,
      CASE WHEN program.has_co_client_c=TRUE THEN TRUE ELSE FALSE END as CO_CLIENT,
      program.name  program_id,
      datediff(month,program.enrolled_date_c,$V_CALENDAR_DATE_CST) months_since_enrollment,
      next_draft_date.next_draft_date next_payment_date
      ,nvl(active_debts.Unsettled_Debt,0) + nvl(num_of_settlements.term_pay_balance,0) as total_amount_enrolled_debt
      ,program.enrolled_date_c as beyond_enrollment_date
      ,program.NU_DSE_PROGRAM_STATUS_C as beyond_enrollment_status
      ,coalesce(last_nsf.nsf_3_mos,0) as nsfs_3_months
      ,tl_list.original_creditor
      ,coalesce(matrix.OFFER_PERCENT,c.top_90_percent_settlement_pct,.70) as settlement_percent
      ,tl_list.tradeline_settlement_status || ' - ' || tl_list.tradeline_settlement_sub_status as settled_tradelined_flag
      ,coalesce(da90.DepAdherence,0) as payment_adherence_ratio_3_months
      ,coalesce(da120.DepAdherence,0) as payment_adherence_ratio_4_months
      ,coalesce(da180.DepAdherence,0) as payment_adherence_ratio_6_months
      ,bank_account.NU_DSE_HOLDER_S_NAME_C
      ,bank_account.NU_DSE_BANK_NAME_C
      ,bank_account.NU_DSE_BANK_ACCOUNT_NUMBER_C
      ,bank_account.NU_DSE_ROUTING_NUMBER_C
      ,bank_account.NU_DSE_ACCOUNT_TYPE_C
      ,historical_settlement_percent.HISTORICAL_SETTLEMENT_PERCENT
      ,remaining_deposits.BF_REMAINING_DEPOSITS
      ,current_draft_amt.amount as MonthlyAmt
      ,tl_list.TRADELINE_NAME as TL_NAME

  FROM {{ params.refined_database_name }}.SALESFORCE.NU_DSE_PROSPECT_C prospect
  JOIN {{ params.refined_database_name }}.SALESFORCE.NU_DSE_PROGRAM_C program ON prospect.id = program.prospect_id_c and program.BEDROCK_MIGRATION_TARGET_UUID_C IS NULL
  join {{ params.refined_database_name }}.SALESFORCE.ACCOUNT act on act.id = program.NU_DSE_ACCOUNT_C and act.BEDROCK_MIGRATION_TARGET_UUID_C IS NULL
  left join {{ params.curated_database_name }}.crm.program cp on cp.program_id=program.id and cp.is_current_record_flag=true
  left join last_nsf on last_nsf.program_id = program.id
  left join current_draft_amt on current_draft_amt.program_id = program.id
  left join num_of_settlements on num_of_settlements.program_id = program.id
  left join active_debts on active_debts.program_id = program.id
  left join payment_dates pd on pd.program_id = program.id
  left join tl_list on tl_list.PROGRAM_NAME = program.name and tl_list.include_in_program_flag=true
  left join deferral on deferral.program_id = program.id
  left join {{ params.refined_database_name }}.SALESFORCE.NU_DSE_FEE_TEMPLATE_C ft ON ft.id = program.NU_DSE_FEE_TEMPLATE_C
  left join next_draft_date ndd on ndd.program_name = program.name
  left join creditor_settlements c on c.original_creditor = tl_list.original_creditor
      and c.current_creditor = tl_list.current_creditor
  left join termination_requested on termination_requested.program_id = program.id
  left join dnc on dnc.dnc_number = REPLACE(regexp_replace(nvl(program.cell_phone_c, nvl(program.home_phone_c, program.work_phone_c)),'[.,\/#!$%\^&\*:{}=\_`~()-]'), ' ', '')
  left join prior_loan_applicant on prior_loan_applicant.program_id = program.id
  left join cft_monthly_fees on cft_monthly_fees.program_id = program.id
  left join blp_monthly_fee on blp_monthly_fee.program_id = program.id
  left join cft_account_balance on cft_account_balance.program_id = program.id
  left join cft_prior_month_payment on cft_prior_month_payment.program_id = program.id
  left join fico_score on fico_score.program_id = program.id
  left join new_or_aged_book on new_or_aged_book.program_id = program.id
  left join recent_payments on recent_payments.program_id = program.id
  left join schedule_adherence on schedule_adherence.program_id = program.id
  left join beyond_fees on beyond_fees.program_id = program.id
  left join fees_outstanding on fees_outstanding.program_id = program.id
  left join fee_template on fee_template.program_id = program.id
  left join ccpa_phone ccpa1 on REPLACE(regexp_replace(program.cell_phone_c,'[.,\/#!$%\^&\*:{}=\_`~()-]'), ' ', '') = ccpa1.ccpa_phone
  left join ccpa_phone ccpa2 on REPLACE(regexp_replace(program.home_phone_c,'[.,\/#!$%\^&\*:{}=\_`~()-]'), ' ', '') = ccpa2.ccpa_phone
  left join ccpa_phone ccpa3 on REPLACE(regexp_replace(program.work_phone_c,'[.,\/#!$%\^&\*:{}=\_`~()-]'), ' ', '') = ccpa3.ccpa_phone
  left join ccpa_email ccpa4 on program.email_address_c = ccpa4.ccpa_email
  left join full_payments on full_payments.program_id = program.id
  left join next_draft_date on next_draft_date.program_id = program.id
  left join deposit_adherence da90 on da90.PROGRAM_NAME=program.name and da90.term=90
  left join deposit_adherence da120 on da120.PROGRAM_NAME=program.name and da120.term=120
  left join deposit_adherence da180 on da180.PROGRAM_NAME=program.name and da180.term=180
  left join bank_account on bank_account.NU_DSE_PROGRAM_C = program.id
  left join historical_settlement_percent on historical_settlement_percent.program_name = program.name
  left join remaining_deposits on remaining_deposits.program_name = program.name
  left join SwapCo_client SWC on SWC.client_id=program.id
  left join ccpa_phone ccpa5 on SWC.Telephone_Number_CO = ccpa5.ccpa_phone
  left join ccpa_email ccpa6 on SWC.Email_Address_CO = ccpa6.ccpa_email
  left join dnc dnc2 on dnc2.dnc_number = swc.Telephone_Number_CO
  left join program_no_cred on program.name = program_no_cred.program_name
  left join creditor_matrix_settlement_logic as matrix on tl_list.tradeline_name = matrix.tradeline_name
  where 1 = 1
  and prospect.BEDROCK_MIGRATION_TARGET_UUID_C IS NULL
  and new_or_aged_book.program_age_bucket = 'New Book'
  and program.NU_DSE_PROGRAM_STATUS_C IN ('Active', 'New Client')
  and state IN ('CA','MI','TX','IN','NC','MO','AL','NM','TN','MS','MT','KY','FL','SD','AK','DC','OK','WI','NY','PA','VA','AZ','AR','UT','ID','LA','MD','NE','MA','GA','SC','OH')
  and (last_nsf.last_nsf_dt is null or last_nsf.last_nsf_dt < dateadd(month, -3, cast($V_CALENDAR_DATE_CST - 1 as date)))
  -- and act.NU_DSE_CFT_CO_CLIENT_ID_C is null
  and dnc.dnc_number is null
  and prior_loan_applicant.program_id is null
  and (tl_list.tradeline_settlement_status not in ('ATTRITED', 'NOT ENROLLED')
       or (tl_list.tradeline_settlement_status != 'SETTLED' and tl_list.tradeline_settlement_sub_status != 'PAID OFF')
       or tl_list.tradeline_settlement_status is null)
  and program.CREATED_DATE_CST < dateadd(month, -3, cast($V_CALENDAR_DATE_CST - 1 as date))
  and recent_payments.program_id is not null
  and coalesce(DA90.DEPADHERENCE,0) >= 0.95
  and termination_requested.program_id is null
  and coalesce(swc.FICO,fico_score.credit_score) >= 540
  and ccpa1.ccpa_phone is null
  and ccpa2.ccpa_phone is null
  and ccpa3.ccpa_phone is null
  and ccpa4.ccpa_email is null
  and ccpa5.CCPA_Phone is null
  and ccpa6.CCPA_Email is null
  and dnc2.dnc_number is null
  and program_no_cred.program_name is null
  and not (state = 'MI' and program.CREATED_DATE_CST >= dateadd(month, -5, cast($V_CALENDAR_DATE_CST - 1 as date)))
  and COALESCE(swc.Email_Address_CO,program.email_address_c) is not null
  and COALESCE(swc.Telephone_Number_CO,program.cell_phone_c,program.home_phone_c,program.work_phone_c) is not null
  and COALESCE(swc.SSN_CO,act.nu_dse_ssn_c) is not null
  -- and full_payments.program_id is not null
  qualify (1- (MonthlyAmt - ((Amount_financed/.95) / current_draft_amt.discount_factor))
          / (case when MonthlyAmt > 0 then MonthlyAmt end)) <= 1.30

  union
  --aged book legacy
  select distinct
      program.id client_id,
      right(program.id, 6) activation_code,
      COALESCE(swc.First_Name_CO,act.FIRST_NAME) FIRST_NAME,
      COALESCE(swc.Last_Name_CO,act.LAST_NAME) LAST_NAME,
      COALESCE(swc.Mailing_Address_CO,act.PERSON_MAILING_STREET) Mailing_Address,
      COALESCE(swc.CITY_CO,act.PERSON_MAILING_CITY) CITY,
      COALESCE(swc.STATE_CO,act.PERSON_MAILING_STATE) STATE,
      left(COALESCE(swc.ZIP_Code_CO,act.PERSON_MAILING_POSTAL_CODE),5) ZIP_Code,
      COALESCE(swc.Email_Address_CO,program.email_address_c) Email_Address,
      COALESCE(swc.Telephone_Number_CO,
          REPLACE(regexp_replace(nvl(program.cell_phone_c, nvl(program.home_phone_c, program.work_phone_c)),'[.,\/#!$%\^&\*:{}=\_`~()-]'), ' ', '')) Telephone_Number,
      COALESCE(swc.DOB_CO,prospect.nu_dse_dob_c) DOB,
      COALESCE(swc.SSN_CO,act.nu_dse_ssn_c) SSN,
      nvl(current_draft_amt.per_freq_amount, 0) DRAFT_AMOUNT,
      Case when program.NU_DSE_PAYMENT_FREQUENCY_C='Semi-Monthly' then 'Twice Monthly' else program.NU_DSE_PAYMENT_FREQUENCY_C end NU_DSE_PAYMENT_FREQUENCY_C,
      pd.payment_date1 last_payment_date,
      pd.payment_date2 last_payment_date2,
      cast(sum(case when upper(tl_list.tradeline_settlement_status) not in ('SETTLED','ATTRITED')
                  then (coalesce(tl_list.NEGOTIATION_BALANCE, tl_list.FEE_BASIS_BALANCE, tl_list.original_balance)*1.00) * coalesce(matrix.OFFER_PERCENT,c.top_90_percent_settlement_pct,.70)
              when upper(tl_list.tradeline_settlement_status) in ('SETTLED') and tl_list.tradeline_settlement_sub_status not like 'BUSTED%' and tl_list.tradeline_settlement_sub_status != 'PAID OFF'
                  then tl_list.CREDITOR_PAYMENTS_OUTSTANDING_AMOUNT
              when upper(tl_list.tradeline_settlement_status) in ('SETTLED') and tl_list.tradeline_settlement_sub_status like 'BUSTED%'
                  then (coalesce(tl_list.NEGOTIATION_BALANCE, tl_list.FEE_BASIS_BALANCE, tl_list.original_balance)*1.00) * coalesce(matrix.OFFER_PERCENT,c.top_90_percent_settlement_pct,.70)
              else 0 end) over (partition by program.id)
          +
          sum(case when upper(tl_list.tradeline_settlement_status) not in ('SETTLED','ATTRITED')
                  then tl_list.estimated_fees
              when upper(tl_list.tradeline_settlement_status) in ('SETTLED') and tl_list.tradeline_settlement_sub_status != 'PAID OFF'
                  then tl_list.fees_outstanding_amount
              else 0 end) over (partition by program.id)
          -
          cft_account_balance.available_balance as decimal(18,2))
          + 6*(nvl(CP.MONTHLY_LEGAL_SERVICE_FEE,0)+nvl(CP.MONTHLY_TRUST_ACCOUNT_SERVICE_FEE,0))
          as Amount_Financed,

      sum(case when upper(tl_list.tradeline_settlement_status) not in ('SETTLED','ATTRITED')
                  then tl_list.estimated_fees
              when upper(tl_list.tradeline_settlement_status) in ('SETTLED') and tl_list.tradeline_settlement_sub_status != 'PAID OFF'
                  then tl_list.fees_outstanding_amount
              else 0 end) over (partition by program.id)
      + 6*(nvl(CP.MONTHLY_LEGAL_SERVICE_FEE,0)+nvl(CP.MONTHLY_TRUST_ACCOUNT_SERVICE_FEE,0))  as estimated_beyond_program_fees,

      cft_account_balance.available_balance                             total_deposits,

      coalesce(cast(sum(case when upper(tl_list.tradeline_settlement_status) not in ('SETTLED','ATTRITED')
                  then (coalesce(tl_list.NEGOTIATION_BALANCE, tl_list.FEE_BASIS_BALANCE, tl_list.original_balance)*1.00) * coalesce(matrix.OFFER_PERCENT,c.top_90_percent_settlement_pct,.70)
              when upper(tl_list.tradeline_settlement_status) in ('SETTLED') and tl_list.tradeline_settlement_sub_status not like 'BUSTED%' and tl_list.tradeline_settlement_sub_status != 'PAID OFF'
                  then tl_list.CREDITOR_PAYMENTS_OUTSTANDING_AMOUNT
              when upper(tl_list.tradeline_settlement_status) in ('SETTLED') and tl_list.tradeline_settlement_sub_status like 'BUSTED%'
                  then (coalesce(tl_list.NEGOTIATION_BALANCE, tl_list.FEE_BASIS_BALANCE, tl_list.original_balance)*1.00) * coalesce(matrix.OFFER_PERCENT,c.top_90_percent_settlement_pct,.70)
              else 0 end) over (partition by program.id, tl_list.tradeline_name, tl_list.current_creditor) as decimal(18,2)),0) as settlement_amount,

      tl_list.current_creditor tradeline_name,
      '' tradeline_account_number,
      program.NU_DSE_ROUTABLE_PHONE_SETTLEMENT_ROUTING_NUMBER_C cft_routing_number,
      program.NU_DSE_ROUTABLE_PHONE_SETTLEMENT_ACCOUNT_NUMBER_C cft_account_number,
      case when program.NU_DSE_ROUTABLE_PHONE_SETTLEMENT_ROUTING_NUMBER_C = 053101561 then 'WELLS FARGO BANK'
              when program.NU_DSE_ROUTABLE_PHONE_SETTLEMENT_ROUTING_NUMBER_C = 053112505 then 'AXOS BANK'
              else null
              end as cft_bank_name
      ,concat(FIRST_NAME,' ',LAST_NAME) cft_account_holder_name
      ,NU_DSE_EXTERNAL_CFT_REST_ID_C external_id,
      CASE WHEN program.has_co_client_c=TRUE THEN TRUE ELSE FALSE END as CO_CLIENT,
      program.name  program_id,
      datediff(month,program.enrolled_date_c,$V_CALENDAR_DATE_CST) months_since_enrollment,
      next_draft_date.next_draft_date next_payment_date
      ,nvl(active_debts.Unsettled_Debt,0) + nvl(num_of_settlements.term_pay_balance,0) as total_amount_enrolled_debt
      ,program.enrolled_date_c as beyond_enrollment_date
      ,program.NU_DSE_PROGRAM_STATUS_C as beyond_enrollment_status
      ,coalesce(last_nsf.nsf_3_mos,0) as nsfs_3_months
      ,tl_list.original_creditor
      ,coalesce(matrix.OFFER_PERCENT,c.top_90_percent_settlement_pct,.70) as settlement_percent
      ,tl_list.tradeline_settlement_status || ' - ' || tl_list.tradeline_settlement_sub_status as settled_tradelined_flag
      ,coalesce(da90.DepAdherence,0) as payment_adherence_ratio_3_months
      ,coalesce(da120.DepAdherence,0) as payment_adherence_ratio_4_months
      ,coalesce(da180.DepAdherence,0) as payment_adherence_ratio_6_months
      ,bank_account.NU_DSE_HOLDER_S_NAME_C
      ,bank_account.NU_DSE_BANK_NAME_C
      ,bank_account.NU_DSE_BANK_ACCOUNT_NUMBER_C
      ,bank_account.NU_DSE_ROUTING_NUMBER_C
      ,bank_account.NU_DSE_ACCOUNT_TYPE_C
      ,historical_settlement_percent.HISTORICAL_SETTLEMENT_PERCENT
      ,remaining_deposits.BF_REMAINING_DEPOSITS
      ,current_draft_amt.amount as MonthlyAmt
      ,tl_list.tradeline_name as TL_NAME

  FROM {{ params.refined_database_name }}.SALESFORCE.NU_DSE_PROSPECT_C prospect
  JOIN {{ params.refined_database_name }}.SALESFORCE.NU_DSE_PROGRAM_C program ON prospect.id = program.prospect_id_c and program.BEDROCK_MIGRATION_TARGET_UUID_C IS NULL
  join {{ params.refined_database_name }}.SALESFORCE.ACCOUNT act on act.id = program.NU_DSE_ACCOUNT_C and act.BEDROCK_MIGRATION_TARGET_UUID_C IS NULL
  left join {{ params.curated_database_name }}.crm.program cp on cp.program_id=program.id and cp.is_current_record_flag=true
  left join last_nsf on last_nsf.program_id = program.id
  left join current_draft_amt on current_draft_amt.program_id = program.id
  left join num_of_settlements on num_of_settlements.program_id = program.id
  left join active_debts on active_debts.program_id = program.id
  left join payment_dates pd on pd.program_id = program.id
  left join tl_list on tl_list.PROGRAM_NAME = program.name and tl_list.include_in_program_flag=TRUE
  left join deferral on deferral.program_id = program.id
  left join {{ params.refined_database_name }}.SALESFORCE.NU_DSE_FEE_TEMPLATE_C ft ON ft.id = program.NU_DSE_FEE_TEMPLATE_C
  left join next_draft_date ndd on ndd.program_name = program.name
  left join creditor_settlements c on c.original_creditor = tl_list.original_creditor
      and c.current_creditor = tl_list.current_creditor
  left join termination_requested on termination_requested.program_id = program.id
  left join dnc on dnc.dnc_number = REPLACE(regexp_replace(nvl(program.cell_phone_c, nvl(program.home_phone_c, program.work_phone_c)),'[.,\/#!$%\^&\*:{}=\_`~()-]'), ' ', '')
  left join prior_loan_applicant on prior_loan_applicant.program_id = program.id
  left join cft_monthly_fees on cft_monthly_fees.program_id = program.id
  left join blp_monthly_fee on blp_monthly_fee.program_id = program.id
  left join cft_account_balance on cft_account_balance.program_id = program.id
  left join cft_prior_month_payment on cft_prior_month_payment.program_id = program.id
  left join fico_score on fico_score.program_id = program.id
  left join new_or_aged_book on new_or_aged_book.program_id = program.id
  left join recent_payments on recent_payments.program_id = program.id
  left join schedule_adherence on schedule_adherence.program_id = program.id
  left join beyond_fees on beyond_fees.program_id = program.id
  left join fees_outstanding on fees_outstanding.program_id = program.id
  left join fee_template on fee_template.program_id = program.id
  left join ccpa_phone ccpa1 on REPLACE(regexp_replace(program.cell_phone_c,'[.,\/#!$%\^&\*:{}=\_`~()-]'), ' ', '') = ccpa1.ccpa_phone
  left join ccpa_phone ccpa2 on REPLACE(regexp_replace(program.home_phone_c,'[.,\/#!$%\^&\*:{}=\_`~()-]'), ' ', '') = ccpa2.ccpa_phone
  left join ccpa_phone ccpa3 on REPLACE(regexp_replace(program.work_phone_c,'[.,\/#!$%\^&\*:{}=\_`~()-]'), ' ', '') = ccpa3.ccpa_phone
  left join ccpa_email ccpa4 on program.email_address_c = ccpa4.ccpa_email
  left join full_payments  on full_payments.program_id = program.id
  left join next_draft_date on next_draft_date.program_id = program.id
  left join deposit_adherence da90 on da90.PROGRAM_NAME=program.name and da90.term=90
  left join deposit_adherence da120 on da120.PROGRAM_NAME=program.name and da120.term=120
  left join deposit_adherence da180 on da180.PROGRAM_NAME=program.name and da180.term=180
  left join bank_account on bank_account.NU_DSE_PROGRAM_C = program.id
  left join historical_settlement_percent on historical_settlement_percent.program_name = program.name
  left join remaining_deposits on remaining_deposits.program_name = program.name
  left join SwapCo_client SWC on SWC.client_id=program.id
  left join ccpa_phone ccpa5 on SWC.Telephone_Number_CO = ccpa5.ccpa_phone
  left join ccpa_email ccpa6 on SWC.Email_Address_CO = ccpa6.ccpa_email
  left join dnc dnc2 on dnc2.dnc_number = swc.Telephone_Number_CO
  left join program_no_cred on program.name = program_no_cred.program_name
  left join creditor_matrix_settlement_logic as matrix on tl_list.tradeline_name = matrix.tradeline_name
  where 1 = 1
  and prospect.BEDROCK_MIGRATION_TARGET_UUID_C IS NULL
  and new_or_aged_book.program_age_bucket = 'Aged Book'
  and program.NU_DSE_PROGRAM_STATUS_C IN ('Active', 'New Client')
  and state IN ('CA','MI','TX','IN','NC','MO','AL','NM','TN','MS','MT','KY','FL','SD','AK','DC','OK','WI','NY','PA','VA','AZ','AR','UT','ID','LA','MD','NE','MA','GA','SC','OH')
  and (last_nsf.last_nsf_dt is null or last_nsf.last_nsf_dt < dateadd(month, -3, cast($V_CALENDAR_DATE_CST - 1 as date)))
  -- and act.NU_DSE_CFT_CO_CLIENT_ID_C is null
  and dnc.dnc_number is null
  and prior_loan_applicant.program_id is null
  and (tl_list.tradeline_settlement_status not in ('ATTRITED', 'NOT ENROLLED')
       or (tl_list.tradeline_settlement_status != 'SETTLED' and tl_list.tradeline_settlement_sub_status != 'PAID OFF')
       or tl_list.tradeline_settlement_status is null)
  and recent_payments.program_id is not null
  and coalesce(da180.DEPADHERENCE,0) >= 0.80
  and ccpa1.ccpa_phone is null
  and ccpa2.ccpa_phone is null
  and ccpa3.ccpa_phone is null
  and ccpa4.ccpa_email is null
  and ccpa5.CCPA_Phone is null
  and ccpa6.CCPA_Email is null
  and dnc2.dnc_number is null
  and program_no_cred.program_name is null
  and COALESCE(swc.Email_Address_CO,program.email_address_c) is not null
  and COALESCE(swc.Telephone_Number_CO,program.cell_phone_c,program.home_phone_c,program.work_phone_c) is not null
  and COALESCE(swc.SSN_CO,act.nu_dse_ssn_c) is not null
  -- and full_payments.program_id is not null
  qualify (1- (MonthlyAmt - ((Amount_financed/.95) / current_draft_amt.discount_factor))
          / (case when MonthlyAmt > 0 then MonthlyAmt end)) <= 1.48

  ) a

where a.amount_financed >= (case when a.state = 'CA' then 5000 else 1000 end) and a.amount_financed <= 71250
--order by a.client_id, a.tradeline_name

UNION ALL
--Bedrock
select client_id, activation_code, FIRST_NAME, LAST_NAME, Mailing_Address, CITY, STATE, ZIP_Code, Email_Address, Telephone_Number, BIRTHDATE, SSN, DRAFT_AMOUNT,
       PAYMENT_FREQUENCY, last_payment_date, last_payment_date2, Amount_Financed, estimated_beyond_program_fees, total_deposits, settlement_amount, tradeline_name, 
       tradeline_account_number
       ,cft_routing_number
       ,cft_account_number
       ,cft_bank_name 
       ,cft_account_holder_name 
       , external_id, CO_CLIENT, program_id, months_since_enrollment, next_payment_date,
       total_amount_enrolled_debt, beyond_enrollment_date, beyond_enrollment_status, nsfs_3_months, original_creditor, settlement_percent, settled_tradelined_flag,
       payment_adherence_ratio_3_months, payment_adherence_ratio_4_months, payment_adherence_ratio_6_months, HOLDER_S_NAME_C, BANK_NAME_C, ACCOUNT_NUMBER_C, ROUTING_NUMBER_C, TYPE_C,
       HISTORICAL_SETTLEMENT_PERCENT, BF_REMAINING_DEPOSITS
from (
  with
  last_nsf as (
--    select program.id                         program_id,
--           program.Name                       program_name,
--           count(payment.id)                  nsfs,
--           count(case when datediff(month, NU_DSE_SCHEDULE_DATE_C, cast($V_CALENDAR_DATE_CST - 1 as date)) <= 3 then payment.id end) nsf_3_mos,
--           max(NU_DSE_SCHEDULE_DATE_C)        last_nsf_dt
--    from {{ params.refined_database_name }}.SALESFORCE.NU_DSE_PROGRAM_C program
--    join {{ params.refined_database_name }}.SALESFORCE.NU_DSE_PAYMENT_C payment on payment.NU_DSE_PROGRAM_C = program.id
--    where payment.NU_DSE_PAYMENT_TYPE_C in ('Draft', 'Deposit')
--      and payment.NU_DSE_TRANSACTION_STATUS_C = 'Failed'
--    group by program.id, program.name
        select program.PROGRAM_ID                         program_id,
           program.program_NAME                       program_name,
           count(payment.TRANSACTION_ID)                  nsfs,
           count(case when dateadd(month, -3, cast($V_CALENDAR_DATE_CST - 1 as date)) < SCHEDULED_DATE_CST then payment.TRANSACTION_ID end) nsf_3_mos,
--           count(case when datediff(month, SCHEDULED_DATE_CST, cast($V_CALENDAR_DATE_CST - 1 as date)) <= 3 then payment.TRANSACTION_ID end) nsf_3_mos,
           max(SCHEDULED_DATE_CST)        last_nsf_dt
    from {{ params.curated_database_name }}.CRM.PROGRAM program
    join {{ params.curated_database_name }}.CRM.SCHEDULED_TRANSACTION payment on payment.PROGRAM_ID = program.PROGRAM_id and payment.IS_CURRENT_RECORD_FLAG=TRUE
    where 1=1
      and payment.TRANSACTION_TYPE in ('Deposit')
      and payment.TRANSACTION_STATUS in ('Failed','Returned')
      and program.IS_CURRENT_RECORD_FLAG=TRUE
      and program.SOURCE_SYSTEM='BEDROCK'
    group by program.PROGRAM_ID, program.PROGRAM_NAME

  ),
  current_draft_amt as (
    select a.program_id
        , a.program_name
         , p.PROGRAM_STATUS
         ,p.PAYMENT_FREQUENCY
        , a.principal_amount_including_fees_per_frequency as Per_freq_amount
         , case when p.PAYMENT_FREQUENCY in ('Twice Monthly','Semi-Monthly') then Per_freq_amount*24/12
                when p.PAYMENT_FREQUENCY in ('Bi-Weekly') then Per_freq_amount*26/12
                when p.PAYMENT_FREQUENCY in ('Monthly') then Per_freq_amount end as amount
        , CASE
              WHEN datediff(MONTH, P.ENROLLED_DATE_CST, current_date) <= 3 THEN ((POWER(1 + (0.27 / 12), 60) - 1)) /
                                                                              ((0.27 / 12) * (POWER(1 + (0.27 / 12), 60)))
              WHEN datediff(MONTH, P.ENROLLED_DATE_CST, current_date) = 4 THEN ((POWER(1 + (0.256 / 12), 60) - 1)) /
                                                                             ((0.2560 / 12) * (POWER(1 + (0.2560 / 12), 60)))
              WHEN datediff(MONTH, P.ENROLLED_DATE_CST, current_date) = 5 THEN ((POWER(1 + (0.2435 / 12), 60) - 1)) /
                                                                             ((0.2435 / 12) * (POWER(1 + (0.2435 / 12), 60)))
              WHEN datediff(MONTH, P.ENROLLED_DATE_CST, current_date) >= 6 THEN ((POWER(1 + (0.229 / 12), 60) - 1)) /
                                                                              ((0.229 / 12) * (POWER(1 + (0.229 / 12), 60)))
              END AS DISCOUNT_FACTOR
    from (
        select distinct
             program_id,
             program_name,
--           , principal_amount_per_frequency
--           , principal_amount_including_fees_per_frequency
--           , monthly_principal_amount
--           , monthly_principal_amount_including_fees
--           from {{ params.curated_database_name }}.crm.program
--           where is_current_record_flag = TRUE and SOURCE_SYSTEM='BEDROCK'
            MODE(TOTAL_AMOUNT) as principal_amount_including_fees_per_frequency
            FROM {{ params.curated_database_name }}.CRM.SCHEDULED_TRANSACTION
            WHERE IS_CURRENT_RECORD_FLAG=TRUE
              and TRANSACTION_TYPE='Deposit'
              and SOURCE_SYSTEM='BEDROCK'
              and SCHEDULED_DATE_CST>=$V_CALENDAR_DATE_CST
              and TRANSACTION_STATUS not in ('Cancelled','Suspended')
        GROUP BY PROGRAM_NAME, PROGRAM_ID
        ) a
      left join {{ params.curated_database_name }}.CRM.PROGRAM P on P.IS_CURRENT_RECORD_FLAG=TRUE and P.PROGRAM_NAME=a.PROGRAM_NAME
  ),
  next_draft_date as (
--    select program.id program_id,
--          program.name program_name,
--          MIN(payment.NU_DSE_SCHEDULE_DATE_C) next_draft_date
--    from {{ params.refined_database_name }}.SALESFORCE.NU_DSE_PROGRAM_C program
--    join {{ params.refined_database_name }}.SALESFORCE.NU_DSE_PAYMENT_C payment on payment.NU_DSE_PROGRAM_C = program.id
--    where payment.NU_DSE_PAYMENT_TYPE_C = 'Deposit'
--    and payment.NU_DSE_TRANSACTION_STATUS_C in ('Scheduled')
--    group by program.id, program.name
      SELECT
        program_id,
        program_name,

        min(SCHEDULED_DATE_CST) as next_draft_date
        FROM {{ params.curated_database_name }}.CRM.SCHEDULED_TRANSACTION
        WHERE 1=1
          and IS_CURRENT_RECORD_FLAG=TRUE
          and TRANSACTION_TYPE='Deposit'
          and SOURCE_SYSTEM='BEDROCK'
          and TRANSACTION_STATUS in ('Scheduled')
    GROUP BY PROGRAM_NAME, PROGRAM_ID
  ),
  Settlement_Schedule as (
      Select PROGRAM_NAME,PROGRAM_ID, OFFER_ID,OFFER_NAME, TRANSACTION_STATUS, TRANSACTION_TYPE,-1*SUM(TRANSACTION_AMOUNT) as Amt
      FROM {{ params.curated_database_name }}.CFT.ACTUAL_TRANSACTION
      WHERE OFFER_ID is not null and TRANSACTION_TYPE in ('Settlement Fee','Payment')
        and TRANSACTION_STATUS in ('Completed','Scheduled','In_Transit','Pending')
        and IS_CURRENT_RECORD_FLAG=TRUE
      GROUP BY PROGRAM_NAME, PROGRAM_ID, OFFER_ID, OFFER_NAME, TRANSACTION_STATUS, TRANSACTION_TYPE
  ),
  num_of_settlements as (
 --   select tl.NU_DSE_PROGRAM_C program_id,
 --         count(*) ct_of_settlements,
 --         SUM(cast(tl.creditor_payments_outstanding_c as decimal(10, 2))) term_pay_balance,
 --         SUM(tl.fees_outstanding_c) fees_outstanding
 --   from {{ params.refined_database_name }}.SALESFORCE.NU_DSE_TRADE_LINE_C tl
 --   join {{ params.refined_database_name }}.SALESFORCE.NU_DSE_OFFER_C offer on offer.NU_DSE_TRADE_LINE_C = tl.id
 --   where lower(offer.NU_DSE_STATUS_C) like '%accepted%'
 --   group by 1
      Select T.Program_name, T.Program_id,
            count(*) as ct_of_Settlements,
            SUM(SSP.amt) term_pay_balance,
            SUM(SSF.amt) fees_outstanding
      FROM {{ params.curated_database_name }}.CRM.TRADELINE T
      LEFT JOIN {{ params.curated_database_name }}.CRM.OFFER O on T.TRADELINE_NAME=O.TRADELINE_NAME and O.IS_CURRENT_RECORD_FLAG=TRUE and O.IS_CURRENT_OFFER=TRUE
      LEFT JOIN Settlement_Schedule SSP on O.OFFER_ID=SSP.OFFER_ID and SSP.TRANSACTION_TYPE='Payment' and SSP.TRANSACTION_STATUS in ('Pending','Scheduled')
      LEFT JOIN Settlement_Schedule SSF on O.OFFER_ID=SSF.OFFER_ID and SSF.TRANSACTION_TYPE='Settlement Fee' and SSF.TRANSACTION_STATUS in ('Pending','Scheduled')
      WHERE T.IS_CURRENT_RECORD_FLAG = TRUE
      and (T.INCLUDE_IN_PROGRAM_FLAG=TRUE or (T.CONDITIONAL_DEBT_STATUS in ('Pending') and T.tradeline_settlement_status='ENROLLED'))
      and O.CFT_PAYMENT_SCHEDULED_CREATED_DATE_CST is not null
      and coalesce(O.SETTLEMENT_BUST_DATE_CST, O.OFFER_CANCELLED_DATE_CST) is null
      Group by T.Program_name, T.Program_id
  ),
  active_debts as (
    --select tl.NU_DSE_PROGRAM_C program_id,
    --      count(distinct tl.id) active_debts,
    --      sum(cast(nvl(nvl(tl.VERIFIED_BALANCE_2_C, tl.NU_DSE_CURRENT_BALANCE_C), tl.NU_DSE_ORIGINAL_DEBT_C) as decimal(38, 2))) current_debt_balance,
    --      sum(nvl(offer.NU_DSE_OFFER_AMOUNT_C, 0)) offer_amount,
    --      case
    --          when sum(nvl(offer.NU_DSE_OFFER_AMOUNT_C, 0)) is not null then
    --            sum(cast(nvl(nvl(tl.VERIFIED_BALANCE_2_C, tl.NU_DSE_CURRENT_BALANCE_C), tl.NU_DSE_ORIGINAL_DEBT_C) as decimal(38, 2)))
    --          else 0 end Unsettled_Debt
    --from {{ params.refined_database_name }}.SALESFORCE.NU_DSE_TRADE_LINE_C tl
    --left join {{ params.refined_database_name }}.SALESFORCE.NU_DSE_OFFER_C offer on offer.NU_DSE_TRADE_LINE_C = tl.id
    --    and lower(offer.NU_DSE_STATUS_C) like '%accepted%'
    --where tl.NU_DSE_INCLUDE_IN_THE_PROGRAM_C = true
    --and offer.id is null
    --group by 1
    Select T.PROGRAM_ID,
            count(distinct T.Program_Id) active_debts,
            sum(coalesce(T.NEGOTIATION_BALANCE,T.FEE_BASIS_BALANCE,T.ENROLLED_BALANCE)) as current_debt_balance,
           sum(nvl(O.SETTLEMENT_AMOUNT,0)) as offer_amount,
           case
                when offer_amount is not null then current_debt_balance else 0 end as Unsettled_debt
      FROM {{ params.curated_database_name }}.CRM.TRADELINE T
      LEFT JOIN {{ params.curated_database_name }}.CRM.OFFER O on O.TRADELINE_NAME=T.TRADELINE_NAME and O.IS_CURRENT_RECORD_FLAG=True and O.IS_CURRENT_OFFER=TRUE and O.OFFER_ACCEPTED_DATE_CST is not null and coalesce(O.OFFER_CANCELLED_DATE_CST,O.SETTLEMENT_BUST_DATE_CST) is null
      WHERE T.IS_CURRENT_RECORD_FLAG=TRUE and (T.INCLUDE_IN_PROGRAM_FLAG=TRUE or (T.CONDITIONAL_DEBT_STATUS in ('Pending') and T.tradeline_settlement_status='ENROLLED')) and O.OFFER_ID is null
    Group by 1
  ),
  tl_list as (
    select tl.TRADELINE_NAME,
    tl.PROGRAM_NAME,
    coalesce(c_orig.creditor_name, ca_orig.creditor_alias_name) original_creditor,
    coalesce(tl.collection_agency_parent_name,tl.collection_agency_name, c_curr.creditor_name,ca_curr.creditor_alias_name,c_orig.creditor_name,ca_orig.creditor_alias_name) current_creditor,
    cast(lltb.amount_c as decimal(18,2)) as latest_tl_balance_amount,
    case
            when tl.original_source_system in ('LEGACY') then case when tl.negotiation_balance = 0 then null else cast(tl.NEGOTIATION_BALANCE as decimal(18,2)) end
            when tl.negotiation_balance is null and latest_tl_balance_amount > tl.ENROLLED_BALANCE then latest_tl_balance_amount
            when tl.negotiation_balance > 0 and nvl(tl.negotiation_balance,0) <= nvl(latest_tl_balance_amount,0) then latest_tl_balance_amount
            when tl.negotiation_balance > 0 and nvl(tl.negotiation_balance,0) >= nvl(latest_tl_balance_amount,0) then cast(tl.NEGOTIATION_BALANCE as decimal(18,2))
        else null end as NEGOTIATION_BALANCE,
--    cast(tl.NU_DSE_ORIGINAL_DEBT_C as decimal(18,2)) original_balance,
    cast(tl.ENROLLED_BALANCE as decimal(18,2)) original_balance,
--    cast(tl.CREDITOR_PAYMENTS_OUTSTANDING_AMOUNT as decimal(18,2)) CREDITOR_PAYMENTS_OUTSTANDING_AMOUNT,
    SSP.Amt CREDITOR_PAYMENTS_OUTSTANDING_AMOUNT,
    SCP.amt Completed_payments,
--    cast(tl2.estimated_fee_c as decimal(18,2)) estimated_fees,
    cast(p.SETTLEMENT_FEE_PERCENTAGE*coalesce(tl.fee_basis_balance,tl.Enrolled_balance)/100 as decimal(18,2)) estimated_fees,
--    cast(tl.FEES_OUTSTANDING_AMOUNT as decimal(18,2)) fees_outstanding_amount,
    SSF.Amt fees_outstanding_amount,
    SCf.amt Completed_fees,
    offer.offer_name,
    offer.settlement_amount,
    offer.SETTLEMENT_SERVICE_FEE,
--    nvl(tl2.NU_DSE_NEW_ACCOUNT_NUMBER_C, tl2.NU_DSE_ORIGINAL_ACCOUNT_NUMBER_C) account_number,
    nvl(tl.CURRENT_ACCOUNT_NUMBER, tl.ORIGINAL_ACCOUNT_NUMBER) account_number,
    tl.tradeline_settlement_status,
    tl.tradeline_settlement_sub_status,
    tl.CONDITIONAL_DEBT_STATUS,
    tl.INCLUDE_IN_PROGRAM_FLAG,
    tl.FEE_BASIS_BALANCE
    from {{ params.curated_database_name }}.crm.TRADELINE tl
--    left join {{ params.refined_database_name }}.SALESFORCE.NU_DSE_TRADE_LINE_C_VW tl2 on tl.TRADELINE_ID = tl2.id
    left join {{ params.curated_database_name }}.crm.offer offer on tl.TRADELINE_NAME = offer.TRADELINE_NAME
              and offer.IS_CURRENT_RECORD_FLAG = true
              and offer.IS_CURRENT_OFFER = true and Offer.CFT_PAYMENT_SCHEDULED_CREATED_DATE_CST is not null
    left join {{ params.curated_database_name }}.crm.creditor c_orig  on tl.original_creditor_id = c_orig.creditor_id
              and c_orig.is_current_record_flag
    left join {{ params.curated_database_name }}.crm.creditor_alias ca_orig on tl.original_creditor_alias_id = ca_orig.creditor_alias_id
              and ca_orig.is_current_record_flag
    left join {{ params.curated_database_name }}.crm.creditor c_curr on tl.current_creditor_id = c_curr.creditor_id
              and c_curr.is_current_record_flag
    left join {{ params.curated_database_name }}.crm.creditor_alias ca_curr on tl.current_creditor_alias_id = ca_curr.creditor_alias_id
              and ca_curr.is_current_record_flag
    left join {{ params.curated_database_name }}.crm.program P on P.PROGRAM_ID=tl.PROGRAM_ID and P.IS_CURRENT_RECORD_FLAG=TRUE
    left join {{ params.refined_database_name }}.bedrock.program_tradeline_c as tlr  on tl.tradeline_id = tlr.id and tlr.is_deleted=FALSE
    left join
        (select
            id,
            PROGRAM_TRADELINE_ID_C,
            amount_c,
            coalesce(BALANCE_AS_OF_DATE_TIME_C_CST,CREATED_DATE_CST) as eff_date,
            row_number() over (partition by program_Tradeline_id_c order by eff_date desc ,amount_c desc) as rn
         from {{ params.refined_database_name }}.BEDROCK.TRADELINE_BALANCE_C
         where amount_c is not null
         qualify rn=1
        ) as ltb on ltb.program_tradeline_id_c=tlr.id
    left join {{ params.refined_database_name }}.BEDROCK.TRADELINE_BALANCE_C as lltb on lltb.id=tlr.LATEST_TRADELINE_BALANCE_ID_C
    left join Settlement_Schedule SSP on offer.OFFER_ID=SSP.OFFER_ID and SSP.TRANSACTION_STATUS ='Scheduled' and SSP.TRANSACTION_TYPE='Payment'
    left join Settlement_Schedule SSF on offer.OFFER_ID=SSF.OFFER_ID and SSF.TRANSACTION_STATUS ='Scheduled' and SSF.TRANSACTION_TYPE='Settlement Fee'
    left join Settlement_Schedule SCP on offer.OFFER_ID=SCP.OFFER_ID and SCP.TRANSACTION_STATUS ='Completed' and SCP.TRANSACTION_TYPE='Payment'
    left join Settlement_Schedule SCF on offer.OFFER_ID=SCF.OFFER_ID and SCF.TRANSACTION_STATUS ='Completed' and SCF.TRANSACTION_TYPE='Settlement Fee'
    where tl.IS_CURRENT_RECORD_FLAG = true
    and not(tl.tradeline_settlement_status = 'SETTLED' and tl.tradeline_settlement_sub_status = 'PAID OFF') and tl.Source_system='BEDROCK'
  ),
  program_no_cred as (
        select distinct
            program_name,
            original_creditor,
            current_creditor
        from tl_list
        where 1=1
            and program_name is not null
            and original_creditor is null
            and current_creditor is null
    ),
  historical_settlement_percent as (
    select
        PROGRAM_NAME
        , sum(coalesce(tl_list.NEGOTIATION_BALANCE, tl_list.FEE_BASIS_BALANCE, tl_list.ORIGINAL_BALANCE)) AS SETTLED_BALANCE_TOTAL
        , sum(tl_list.SETTLEMENT_AMOUNT) AS SETTLEMENT_AMOUNT_TOTAL
        , case when SETTLED_BALANCE_TOTAL > 0 then (SETTLEMENT_AMOUNT_TOTAL / SETTLED_BALANCE_TOTAL) else null end AS HISTORICAL_SETTLEMENT_PERCENT
    from tl_list
    where TRADELINE_SETTLEMENT_STATUS = 'SETTLED'
    and TRADELINE_SETTLEMENT_SUB_STATUS NOT ILIKE 'BUSTED%'
    group by 1
  ),
  deferral as (
    select t.*,
          ((datediff(month, min_dt, max_dt) + 1) - ct) deferral_ct
    from (
--          select
--            program.id program_id,
--            min(last_day(payment.NU_DSE_SCHEDULE_DATE_C)) min_dt,
--            max(last_day(payment.NU_DSE_SCHEDULE_DATE_C)) max_dt,
--            count(distinct last_day(payment.NU_DSE_SCHEDULE_DATE_C)) ct
--          from {{ params.refined_database_name }}.SALESFORCE.NU_DSE_PROGRAM_C program
--          join {{ params.refined_database_name }}.SALESFORCE.NU_DSE_PAYMENT_C payment on payment.NU_DSE_PROGRAM_C = program.id
--          where payment.NU_DSE_PAYMENT_TYPE_C in ('Deposit', 'Draft')
--          and payment.NU_DSE_TRANSACTION_STATUS_C = 'Completed'
--          group by program.id
        Select
            program_id, PROGRAM_NAME,
            min(last_day(SCHEDULED_DATE_CST)) min_dt,
            max(last_day(SCHEDULED_DATE_CST)) max_dt,
            count(distinct last_day(SCHEDULED_DATE_CST)) ct
          from {{ params.curated_database_name }}.CRM.SCHEDULED_TRANSACTION S
          where S.TRANSACTION_TYPE in ('Deposit', 'Draft')
          and S.TRANSACTION_STATUS = 'Completed' and S.IS_CURRENT_RECORD_FLAG=TRUE
          group by program_id,PROGRAM_NAME

        ) t
  ),
  payments as (
    select * from (
        select
          program.PROGRAM_ID program_id,
          program.PROGRAM_NAME,
          program.PAYMENT_FREQUENCY,
          payment.SCHEDULED_DATE_CST payment_date,
          row_number() over (partition by payment.program_id order by payment_date desc) seq,
          count(payment.SCHEDULED_DATE_CST)
             over (partition by payment.program_id, last_day(payment.SCHEDULED_DATE_CST)
             order by payment.SCHEDULED_DATE_CST desc) payments_count
        from {{ params.curated_database_name }}.CRM.PROGRAM program
        join {{ params.curated_database_name }}.CRM.SCHEDULED_TRANSACTION payment on payment.PROGRAM_NAME = program.PROGRAM_NAME and payment.IS_CURRENT_RECORD_FLAG=TRUE
        where payment.TRANSACTION_TYPE in ('Deposit', 'Draft')
        and payment.TRANSACTION_STATUS = 'Completed' and program.IS_CURRENT_RECORD_FLAG=TRUE)
    where seq > case when PAYMENT_FREQUENCY = 'Monthly' then 6 else 12 end
    order by program_id
  ),
  recent_payments as (
    select * from (
        select
          program.program_id,
          program.PROGRAM_NAME,
          program.PAYMENT_FREQUENCY,
          payment.SCHEDULED_DATE_CST payment_date,
          datediff(month,payment.SCHEDULED_DATE_CST,$V_CALENDAR_DATE_CST),
          row_number() over (partition by program.program_id order by payment_date desc) seq,
          count(payment.SCHEDULED_DATE_CST)
             over (partition by program.program_id, last_day(payment.SCHEDULED_DATE_CST)
             order by payment.SCHEDULED_DATE_CST desc) payments_count
        from {{ params.curated_database_name }}.CRM.PROGRAM program
        join {{ params.curated_database_name }}.CRM.SCHEDULED_TRANSACTION payment on payment.PROGRAM_NAME = program.PROGRAM_NAME and payment.IS_CURRENT_RECORD_FLAG=TRUE
        where payment.TRANSACTION_TYPE in ('Deposit', 'Draft')
        and payment.TRANSACTION_STATUS in ('Completed','In Progress','In Process') and program.IS_CURRENT_RECORD_FLAG=TRUE)
     where seq = case when PAYMENT_FREQUENCY = 'Monthly' then 3 else 6 end
     and datediff(month,payment_date,$V_CALENDAR_DATE_CST) < 4
  ),
  payment_dates as (
    select * from
      (select * from (
          select
          program.program_id,
          program.PROGRAM_NAME,
            program.PAYMENT_FREQUENCY,
            payment.SCHEDULED_DATE_CST,
            row_number() over (partition by program.PROGRAM_NAME order by payment.SCHEDULED_DATE_CST desc) seq
        from {{ params.curated_database_name }}.CRM.PROGRAM program
        join {{ params.curated_database_name }}.CRM.SCHEDULED_TRANSACTION payment on payment.PROGRAM_NAME = program.PROGRAM_NAME and payment.IS_CURRENT_RECORD_FLAG=TRUE
          where payment.TRANSACTION_TYPE in ('Deposit', 'Draft')
          and payment.TRANSACTION_STATUS = 'Completed' and program.IS_CURRENT_RECORD_FLAG=TRUE)
       where 1 = 1
       and seq <= case when PAYMENT_FREQUENCY = 'Monthly' then 1 else 2 end) tt
       pivot (max(SCHEDULED_DATE_CST) for seq in (1,2)) as p (program_id, program_name, payment_frequency, Payment_date1, payment_date2)
  ),
  schedule_adherence as (
    select
      *
      , case when total_payments <> 0 then cast(successful_payments as float)/(total_payments) end as schedule_adherence
    from (
      select distinct program.PROGRAM_ID, program.Program_name,
           sum(case when payment.SCHEDULED_DATE_CST < $V_CALENDAR_DATE_CST and TRANSACTION_STATUS not in ('Failed', 'Cancelled', 'Returned','Scheduled')
               then 1 else 0 end) over(partition by program.PROGRAM_ID) as successful_payments,
           sum(case when payment.SCHEDULED_DATE_CST < $V_CALENDAR_DATE_CST and TRANSACTION_STATUS in ('Failed', 'Returned','Cancelled')
               then 1 else 0 end) over(partition by program.program_id) as incomplete_payments,
           sum(case when payment.SCHEDULED_DATE_CST < $V_CALENDAR_DATE_CST
               then 1 else null end) over(partition by program.program_id) as total_payments
        from {{ params.curated_database_name }}.CRM.PROGRAM program
        join {{ params.curated_database_name }}.CRM.SCHEDULED_TRANSACTION payment on payment.PROGRAM_NAME = program.PROGRAM_NAME and payment.IS_CURRENT_RECORD_FLAG=TRUE
      where payment.TRANSACTION_TYPE in ('Deposit', 'Draft') and program.IS_CURRENT_RECORD_FLAG=TRUE) a
  ),
  creditor_settlements as (
    select
        original_creditor
        , current_creditor
        , sum(settlement_amount)    "Settlement Amount"
        , sum(tradeline_original_amount)    "Tradeline Original Amount"
        , avg(settlement_pct_original)      settlement_pct_original
        , round(avg(settlement_amount)) average_settlement_amt
        , count(tradeline_name)       tradeline_count
        , avg(settlement_pct_original) average_settlement_pct
        , percentile_cont(0.25) within group (order by settlement_pct_original) bottom_25_percent_settlement_pct
        , percentile_cont(0.50) within group (order by settlement_pct_original) middle_50_percent_settlement_pct
        , percentile_cont(0.75) within group (order by settlement_pct_original) top_25_percent_settlement_pct
        , percentile_cont(0.9) within group (order by settlement_pct_original) top_90_percent_settlement_pct
    from (
          select distinct --ifnull(t.original_creditor_parent_name,t.original_creditor_name) original_creditor
                coalesce(c_orig.creditor_name,ca_orig.creditor_alias_name) original_creditor
                -- , coalesce(t.collection_agency_parent_name,t.collection_agency_name, t.current_creditor_parent_name,t.current_creditor_name,t.original_creditor_parent_name,t.original_creditor_name) current_creditor
                , coalesce(t.collection_agency_parent_name,t.collection_agency_name, c_curr.creditor_name,ca_curr.creditor_alias_name,c_orig.creditor_name,ca_orig.creditor_alias_name) current_creditor
                , o.tradeline_original_amount
                , o.settlement_amount
                , case when coalesce(t.NEGOTIATION_BALANCE, t.FEE_BASIS_BALANCE, t.ENROLLED_BALANCE) <> 0 then (cast(o.settlement_amount as float)/ coalesce(t.NEGOTIATION_BALANCE, t.FEE_BASIS_BALANCE, t.ENROLLED_BALANCE)) end as settlement_pct_original
                , case when o.tradeline_original_amount  <> 0 then (cast(o.settlement_service_fee as float)/ o.tradeline_original_amount) end as settlement_fee_pct_original
                , o.settlement_service_fee
                , o.tradeline_name
                , o.offer_accepted_date_cst
          from {{ params.curated_database_name }}.crm.offer o
          left join {{ params.curated_database_name }}.crm.tradeline t on o.tradeline_name = t.tradeline_name
          left join {{ params.curated_database_name }}.crm.creditor c_orig on t.original_creditor_id = c_orig.creditor_id
              and c_orig.is_current_record_flag
          left join {{ params.curated_database_name }}.crm.creditor_alias ca_orig on t.original_creditor_alias_id = ca_orig.creditor_alias_id
              and ca_orig.is_current_record_flag
          left join {{ params.curated_database_name }}.crm.creditor c_curr on t.current_creditor_id = c_curr.creditor_id
              and c_curr.is_current_record_flag
          left join {{ params.curated_database_name }}.crm.creditor_alias ca_curr on t.current_creditor_alias_id = ca_curr.creditor_alias_id
              and ca_curr.is_current_record_flag
          where datediff(day,o.offer_accepted_date_cst,$V_CALENDAR_DATE_CST) <= 90
          and o.is_current_record_flag = 'TRUE'
          and o.is_current_offer = 'TRUE'
          and t.is_current_record_flag = 'TRUE'
          and o.CFT_PAYMENT_SCHEDULED_CREATED_DATE_CST is not null) a
    where settlement_pct_original < 1 and settlement_pct_original > 0.05
    group by original_creditor
            , current_creditor
  ),
  /*fee_template as (
      select
          distinct NU_DSE_PROGRAM_C.ID program_id
          , NU_DSE_PROGRAM_C.NAME
          , nu_dse_settlement_fee_percentage_c settlement_fee_pct
      from  {{ params.refined_database_name }}.SALESFORCE.NU_DSE_PROGRAM_C
      inner join {{ params.refined_database_name }}.SALESFORCE.NU_DSE_FEE_TEMPLATE_C_VW on NU_DSE_PROGRAM_C.NU_DSE_FEE_TEMPLATE_C = NU_DSE_FEE_TEMPLATE_C_VW.ID
      where NU_DSE_PROGRAM_C.is_deleted_flag = FALSE
      and NU_DSE_FEE_TEMPLATE_C_VW.is_deleted_flag = FALSE
   ),*/
  termination_requested as (
      select distinct coalesce(SALESFORCE_PROGRAM_ID, NU_DSE_PROGRAM_C_ID) AS program_id
      from {{ params.refined_database_name }}.FIVE9.CALL
      where DISPOSITION ILIKE '%term%'
  ),
  dnc as (
      select distinct cast(dnc_number as nvarchar) dnc_number
      from {{ params.refined_database_name }}.FIVE9.DNC
  ),
  prior_loan_applicant as (
    Select distinct
            p.Program_ID_C,
            p.LENDER_STATUS_C,
            p.LOAN_APPLICATION_INTEREST_C,
            p.LOAN_APPLICATION_STATUS_C,
            p.CREATED_DATE_CST,
            p.LAST_MODIFIED_DATE_CST,
            dsmr.current_status,
            dsmr.app_submit_date
    From {{ params.refined_database_name }}.BEDROCK.PROGRAM_LOAN_C as p
    left join {{ params.curated_database_name }}.CRM.PROGRAM CP
         on p.PROGRAM_ID_C = CP.PROGRAM_ID AND CP.IS_CURRENT_RECORD_FLAG = TRUE
    LEFT JOIN (
               SELECT *
               FROM {{ params.refined_database_name }}.ABOVE_LENDING.AGL_COMBINED_DETAIL
               QUALIFY rank() OVER (PARTITION BY PROGRAM_NAME ORDER BY APP_SUBMIT_DATE DESC) = 1
              ) DSMR ON DSMR.PROGRAM_NAME = CP.PROGRAM_NAME
    WHERE p.IS_DELETED=FALSE
          and (DSMR.CURRENT_STATUS IN ('ONBOARDED')
               OR (DSMR.CURRENT_STATUS IN ('BACK_END_DECLINED') AND datediff(DAY, DSMR.APP_SUBMIT_DATE, current_date) <= 90)
               OR (DSMR.CURRENT_STATUS IN ('FRONT_END_DECLINED') AND datediff(DAY, DSMR.APP_SUBMIT_DATE, current_date) <= 90)
               )
  ),
  cft_monthly_fees as (
      select p.PROGRAM_ID as program_id, P.PROGRAM_NAME
          , SUM(ACT.TRANSACTION_AMOUNT) as cft_monthly_fees
      FROM {{ params.curated_database_name }}.CRM.PROGRAM P
      left join {{ params.curated_database_name }}.CFT.ACTUAL_TRANSACTION ACT on ACT.PROGRAM_NAME=P.PROGRAM_NAME and ACT.IS_CURRENT_RECORD_FLAG=TRUE
      where ACT.TRANSACTION_STATUS='Completed'
      AND ACT.transaction_group='Fee'
      AND ACT.transaction_type='Monthly Service Fees'
      and datediff(month,TRANSACTION_DATE_CST,$V_CALENDAR_DATE_CST) = 1 and P.IS_CURRENT_RECORD_FLAG=TRUE
      GROUP BY p.PROGRAM_ID, p.PROGRAM_NAME
  ),
  blp_monthly_fee as (
      select p.PROGRAM_ID as program_id, P.PROGRAM_NAME
          , SUM(ACT.TRANSACTION_AMOUNT) as cft_monthly_fees
      FROM {{ params.curated_database_name }}.CRM.PROGRAM P
      left join {{ params.curated_database_name }}.CFT.ACTUAL_TRANSACTION ACT on ACT.PROGRAM_NAME=P.PROGRAM_NAME and ACT.IS_CURRENT_RECORD_FLAG=TRUE
      where ACT.TRANSACTION_STATUS='Completed'
      AND ACT.transaction_group='Fee'
      AND ACT.transaction_type='Monthly Legal Service Fee'
      and datediff(month,TRANSACTION_DATE_CST,$V_CALENDAR_DATE_CST) = 1 and P.IS_CURRENT_RECORD_FLAG=TRUE
      GROUP BY p.PROGRAM_ID, p.PROGRAM_NAME
  ),
  cft_account_balance as (
      select distinct P.PROGRAM_id as program_id, p.PROGRAM_NAME, DA.PROCESSOR_CLIENT_ID
              , DA.CURRENT_BALANCE
              , DA.AVAILABLE_BALANCE
      FROM {{ params.curated_database_name }}.CRM.PROGRAM P
      LEFT JOIN {{ params.curated_database_name }}.CFT.DEPOSIT_ACCOUNT_BALANCE DA ON P.PROGRAM_ID=DA.program_id
      where AS_OF_DATE_CST = $V_CALENDAR_DATE_CST and P.IS_CURRENT_RECORD_FLAG=TRUE
  ),
cft_prior_month_payment as (
      select p.PROGRAM_ID as program_id, P.PROGRAM_NAME
          , SUM(ACT.TRANSACTION_AMOUNT) as cft_monthly_fees
      FROM {{ params.curated_database_name }}.CRM.PROGRAM P
      left join {{ params.curated_database_name }}.CFT.ACTUAL_TRANSACTION ACT on ACT.PROGRAM_NAME=P.PROGRAM_NAME and ACT.IS_CURRENT_RECORD_FLAG=TRUE
      where ACT.TRANSACTION_STATUS='Completed'
      AND ACT.transaction_group='Deposit'
      AND ACT.transaction_type='Deposit'
      and datediff(month,TRANSACTION_DATE_CST,$V_CALENDAR_DATE_CST) = 1 and P.IS_CURRENT_RECORD_FLAG=TRUE
      GROUP BY p.PROGRAM_ID, p.PROGRAM_NAME
),
  fico_score as (
    select * from (
        select distinct
            program.PROGRAM_ID as program_id,
            program.PROGRAM_NAME,
            c.CREDIT_SCORE as credit_score,
            c.CREDIT_SCORE_DATE_CST,
            row_number() over (PARTITION BY program.program_name ORDER BY c.CREDIT_SCORE_DATE_CST asc) as rank
        from {{ params.curated_database_name }}.CRM.PROGRAM program
        left join {{ params.curated_database_name }}.CRM.CLIENT C
            on C.CLIENT_ID = program.CLIENT_ID
        where 1=1
            and C.CREDIT_SCORE  is not null
            and program.IS_CURRENT_RECORD_FLAG=TRUE
        )
    where rank = 1
  ),
  new_or_aged_book as (
    select distinct program.program_id as program_id, program.PROGRAM_NAME
         ,case when datediff(month,program.ENROLLED_DATE_CST,$V_CALENDAR_DATE_CST) >= 6 then 'Aged Book'
                when datediff(month,program.ENROLLED_DATE_CST,$V_CALENDAR_DATE_CST) < 6 then 'New Book'
         end as Program_Age_Bucket
    ,program.ENROLLED_DATE_CST
    from {{ params.curated_database_name }}.CRM.PROGRAM program
      where program.IS_CURRENT_RECORD_FLAG=TRUE
  ),
  beyond_fees as (
    select distinct program_id
        , sum(case when transaction_status = 'Scheduled' then TOTAL_AMOUNT * -1 else 0 end) as remaining_beyond_fees
    from {{ params.curated_database_name }}.crm.scheduled_transaction
    where transaction_type = 'Fee Withdrawal'
    and is_current_record_flag = 'TRUE'
    group by program_id
  ),
fees_outstanding as (
    select program_id
          , sum(TOTAL_AMOUNT * -1) as fees_outstanding
    from {{ params.curated_database_name }}.crm.scheduled_transaction
    where is_current_record_flag = 'TRUE'
    and transaction_status = 'Scheduled'
    and transaction_type = 'Fee'
    group by program_id
  ),
  CCPA_phone as  (
    select distinct REPLACE(regexp_replace(contact.phone,'[.,\/#!$%\^&\*:{}=\_`~()-]'), ' ', '') as CCPA_Phone
    from {{ params.refined_database_name }}.salesforce.compliance_request_c_vw compliance
    left join {{ params.refined_database_name }}.salesforce.contact on compliance.contact_c = contact.id
    and contact.phone is not null
    union
    select REPLACE(regexp_replace(contact.mobile_phone,'[.,\/#!$%\^&\*:{}=\_`~()-]'), ' ', '') as CCPA_Phone
    from {{ params.refined_database_name }}.salesforce.compliance_request_c_vw compliance
    left join {{ params.refined_database_name }}.salesforce.contact on compliance.contact_c = contact.id
    and contact.mobile_phone is not null
    union
    select REPLACE(regexp_replace(contact.home_phone,'[.,\/#!$%\^&\*:{}=\_`~()-]'), ' ', '') as CCPA_Phone
    from {{ params.refined_database_name }}.salesforce.compliance_request_c_vw compliance
    left join {{ params.refined_database_name }}.salesforce.contact on compliance.contact_c = contact.id
    and contact.home_phone is not null
  ),
  CCPA_email as  (
    select distinct compliance.from_address_c as CCPA_Email
    from {{ params.refined_database_name }}.salesforce.compliance_request_c_vw compliance
    left join {{ params.refined_database_name }}.salesforce.contact on compliance.contact_c = contact.id
    and compliance.from_address_c is not null
    union
    select contact.email as CCPA_Email
    from {{ params.refined_database_name }}.salesforce.compliance_request_c_vw compliance
    left join {{ params.refined_database_name }}.salesforce.contact on compliance.contact_c = contact.id
    and contact.email is not null
    UNION
    --Include all Sendgrid and Maripost OptOuts
    Select Distinct Native_ID as email FROM {{ params.refined_database_name }}.SIMON_DATA.SENDGRID_UNSUBSCRIBES
    --UNION
    --Select Distinct EMAIL
    --FROM {{ params.refined_database_name }}.MAROPOST.UNSUBSCRIBES U
    --LEFT JOIN {{ params.refined_database_name }}.MAROPOST.CONTACTS  C on U.CONTACT_ID=C.CONTACT_ID
  ),
  Full_payments as  (
    select program_id,
           program_name,
           count(*)
    from (
        select *,
              row_number() over (partition by program_id order by pay_month desc) as seqnum1
        from (
            select distinct scheduled_transaction.program_id,
                   scheduled_transaction.program_name,
                   last_day(scheduled_transaction.scheduled_date_cst) pay_month,
                    program.monthly_principal_amount_including_fees as Planned_Deposits,
                    sum(case when scheduled_transaction.transaction_status in ('Completed', 'Cleared') then scheduled_transaction.Total_Amount else 0 end) as Completed_Deposits
            from {{ params.curated_database_name }}.crm.scheduled_transaction
            inner join {{ params.curated_database_name }}.crm.program
            on scheduled_transaction.program_id = program.program_id
            and scheduled_transaction.is_current_record_flag = TRUE
            and program.is_current_record_flag = TRUE
            where scheduled_transaction.transaction_type = 'Deposit'
            and scheduled_transaction.transaction_status in ('Completed', 'Cleared')
            and last_day(scheduled_transaction.scheduled_date_cst) < last_day($V_CALENDAR_DATE_CST)
            group by scheduled_transaction.program_id
                    , scheduled_transaction.program_name
                    , last_day(scheduled_transaction.scheduled_date_cst)
                    , program.monthly_principal_amount_including_fees
            having completed_deposits > 0
          ) tt
        where completed_deposits >= planned_deposits
        qualify seqnum1 <= 3
      ) ttt
  where datediff(month,pay_month,$V_CALENDAR_DATE_CST) <= 3
  group by program_id
          , program_name
  having count(*) = 3
  ),
    Deposit_Adherence as (
        With

        Dates as (
            Select
            90 Term, DATEADD('day',-90,CURRENT_DATE()) as StartDate, CURRENT_DATE() as EndDate
            Union All
            Select
            120 Term, DATEADD('day',-120,CURRENT_DATE()) as StartDate, CURRENT_DATE() as EndDate
            Union All
            Select
            180 Term,  DATEADD('day',-180,CURRENT_DATE()) as StartDate, CURRENT_DATE() as EndDate
        ),


        Prg as (
            Select P.PROGRAM_NAME, Last_Day(P.ENROLLED_DATE_CST) as Vintage, P.ENROLLED_DATE_CST, P.PROGRAM_STATUS, E.EFFECTIVE_DATE
            FROM {{ params.curated_database_name }}.CRM.PROGRAM P
            LEFT JOIN (
                Select S.PROGRAM_NAME, min(S.RECORD_EFFECTIVE_START_DATE_TIME_CST) as EFFECTIVE_DATE
                From {{ params.curated_database_name }}.CRM.SCHEDULED_TRANSACTION S
                LEFT Join (
                    Select PROGRAM_NAME, ENROLLED_DATE_CST FROM {{ params.curated_database_name }}.CRM.PROGRAM WHERE IS_CURRENT_RECORD_FLAG = TRUE) P
                    On S.PROGRAM_NAME = P.PROGRAM_NAME
                Where TRANSACTION_TYPE = 'Deposit' AND SCHEDULED_DATE_CST is not NULL AND TRANSACTION_NUMBER is not NULL
                  AND S.SCHEDULED_DATE_CST>= P.ENROLLED_DATE_CST
                GROUP BY S.PROGRAM_NAME) E on P.PROGRAM_NAME = E.PROGRAM_NAME
            WHERE P.IS_CURRENT_RECORD_FLAG = TRUE and P.SOURCE_SYSTEM='BEDROCK'),

        Schedules as (
            Select Term,StartDate,EndDate,PROGRAM_NAME, TRANSACTION_TYPE, ENROLLED_DATE_CST,
                       count(TOTAL_AMOUNT) SCHEDULED_COUNT,Sum(TOTAL_AMOUNT) as SCHEDULED_AMOUNT
                       from (


                        Select Dates.Term,Dates.StartDate,Dates.EndDate,s.PROGRAM_NAME, s.TRANSACTION_TYPE, s.TOTAL_AMOUNT, s.TRANSACTION_STATUS, s.SCHEDULED_DATE_CST, P.ENROLLED_DATE_CST,
                               row_number() over (PARTITION BY TRANSACTION_NUMBER,Dates.Term ORDER BY RECORD_EFFECTIVE_START_DATE_TIME_CST DESC) rnk
                            From {{ params.curated_database_name }}.CRM.SCHEDULED_TRANSACTION S
                        Inner Join Prg P On S.PROGRAM_NAME = P.PROGRAM_NAME
                        CROSS JOIN Dates
                        WHERE 1=1
                          AND S.RECORD_EFFECTIVE_START_DATE_TIME_CST<=greatest(Dates.StartDate,P.EFFECTIVE_DATE)
                        Qualify rnk = 1)
            Where TRANSACTION_STATUS  in ('Scheduled', 'In Progress', 'Pending', 'Tentative','Completed','Failed','Processing Failed','Returned','In_Transit','Suspended')
            AND TRANSACTION_TYPE = 'Deposit' and SCHEDULED_DATE_CST>= StartDate AND SCHEDULED_DATE_CST<= EndDate
            Group BY Term,PROGRAM_NAME, TRANSACTION_TYPE, ENROLLED_DATE_CST, StartDate, EndDate
            ),

        Actuals as (
            Select Term,StartDate, EndDate, PROGRAM_NAME, ENROLLED_DATE_CST,
                   count(TRANSACTION_AMOUNT) ACTUAL_COUNT,Sum(TRANSACTION_AMOUNT) as ACTUAL_AMOUNT
                   from (
                       Select Dates.Term, Dates.StartDate,Dates.EndDate,A.PROGRAM_NAME, A.TRANSACTION_TYPE, A.TRANSACTION_AMOUNT, A.TRANSACTION_STATUS, S.SCHEDULED_DATE_CST As TRANSACTION_DATE_CST, P.ENROLLED_DATE_CST
                       From {{ params.curated_database_name }}.CFT.ACTUAL_TRANSACTION A
                       CROSS JOIN Dates
                       LEFT JOIN {{ params.curated_database_name }}.CRM.SCHEDULED_TRANSACTION S ON S.TRANSACTION_NUMBER=A.TRANSACTION_NUMBER AND s.IS_CURRENT_RECORD_FLAG = TRUE
                       Inner Join Prg P On A.PROGRAM_NAME = P.PROGRAM_NAME
                       WHERE S.SCHEDULED_DATE_CST>= Dates.StartDate AND S.SCHEDULED_DATE_CST<= Dates.EndDate
                         AND A.IS_CURRENT_RECORD_FLAG = TRUE
                         )
            Where upper(TRANSACTION_TYPE) in ('DEPOSIT') And upper(TRANSACTION_STATUS) in ('COMPLETED','IN_TRANSIT','SCHEDULED','IN PROGRESS')
            Group BY Term, PROGRAM_NAME, ENROLLED_DATE_CST, StartDate,EndDate
            )

        Select P.PROGRAM_NAME,Dates.Term, any_value(Dates.StartDate), any_value(Dates.EndDate),any_value(Vintage), nvl(sum(A.ACTUAL_AMOUNT),0) as ACTUAL_AMOUNT ,sum(S.SCHEDULED_AMOUNT) as SCHEDULED_AMOUNT,
            case when sum(S.SCHEDULED_AMOUNT)>0 then  nvl(sum(A.ACTUAL_AMOUNT),0) / sum(S.SCHEDULED_AMOUNT) end as DepAdherence
        From Prg P
        Inner JOIN Schedules S on P.PROGRAM_NAME = S.PROGRAM_NAME
        INNER JOIN Dates On S.Term = Dates.Term
        LEFT JOIN Actuals A on P.PROGRAM_NAME = A.PROGRAM_NAME AND S.StartDate=A.StartDate AND A.Term = S.Term and A.Term = Dates.Term
        WHERE P.PROGRAM_STATUS not in ('Closed','Sold to 3rd Party')
        GROUP BY Dates.Term, P.PROGRAM_NAME
    ),

  bank_account as (
    select
      CT.Name HOLDER_S_NAME_C,
      BANK_NAME_C,
      ACCOUNT_NUMBER_C,
      ROUTING_NUMBER_C,
      TYPE_C,
      P.Name as NU_DSE_PROGRAM_C, STATUS_C,STATUS_DETAIL_C,VERIFICATION_STATUS_C, row_number() over (PARTITION BY P.NAME ORDER BY BANK_ACCOUNT.LAST_MODIFIED_DATE_CST DESC) rnk
    from {{ params.refined_database_name }}.BEDROCK.BANK_ACCOUNT_C bank_account
    left join {{ params.refined_database_name }}.BEDROCK.PROGRAM_C P on P.ACCOUNT_ID_C=bank_account.ACCOUNT_ID_C
    left join {{ params.refined_database_name }}.BEDROCK.CONTACT CT on CT.ACCOUNT_ID=bank_account.ACCOUNT_ID_C
    left join {{ params.refined_database_name }}.BEDROCK.ACCOUNT_CONTACT_RELATION ACR on Acr.CONTACT_ID=CT.ID
    where lower(STATUS_C) = 'active'
    and lower(STATUS_DETAIL_C) = 'verified'
    and lower(bank_account.OFAC_STATUS_C) = 'verified'
    and acr.RELATIONSHIP_C='Client' and CT.IS_DELETED=FALSE and bank_account.IS_DELETED=FALSE
      qualify rnk=1
  ),
  remaining_deposits as (
    select
        T.PROGRAM_NAME,
        sum(TOTAL_AMOUNT) AS BF_REMAINING_DEPOSITS
    from {{ params.curated_database_name }}.CRM.SCHEDULED_TRANSACTION T
    join {{ params.curated_database_name }}.CRM.PROGRAM P ON T.PROGRAM_NAME = P.PROGRAM_NAME AND P.IS_CURRENT_RECORD_FLAG
    where T.IS_CURRENT_RECORD_FLAG
    and TRANSACTION_TYPE = 'Deposit'
    and SCHEDULED_DATE_CST::DATE > $V_CALENDAR_DATE_CST
    and TRANSACTION_STATUS IS DISTINCT FROM 'Cancelled'
    Group by 1
    ),
  coclients as (
  Select P.PROGRAM_NAME, count(CoClient.CONTACT_ID) ct
  FROM {{ params.curated_database_name }}.CRM.PROGRAM p
  join {{ params.refined_database_name }}.BEDROCK.PROGRAM_C PC on PC.name=P.Program_name and PC.IS_DELETED=FALSE
  left join {{ params.refined_database_name }}.BEDROCK.ACCOUNT act on act.id = pc.ACCOUNT_ID_C and act.IS_DELETED=FALSE
  left join {{ params.refined_database_name }}.BEDROCK.ACCOUNT_CONTACT_RELATION ACR on ACR.ACCOUNT_ID=act.ID and ACR.RELATIONSHIP_C='Client' and ACR.IS_DELETED=FALSE
  left join {{ params.refined_database_name }}.BEDROCK.CONTACT CT on acr.CONTACT_ID=ct.id and ct.IS_DELETED=FALSE
  --left join {{ params.refined_database_name }}.BEDROCK.ACCOUNT_CONTACT_RELATION ACR on ACR.CONTACT_ID=CT.ID and ACR.RELATIONSHIP_C='Client' and ACR.IS_DELETED=FALSE
  left join {{ params.refined_database_name }}.BEDROCK.ACCOUNT_CONTACT_RELATION CoClient on CoClient.ACCOUNT_ID=PC.ACCOUNT_ID_C and CoClient.RELATIONSHIP_C='Co-Client' and CoClient.IS_DELETED=FALSE
  WHERE IS_CURRENT_RECORD_FLAG=TRUE
  GROUP BY 1
  ),
DQ as (
    Select PROGRAM_NAME, min(DQ_DELTA_BY_DAY75) ProgDQb75
  FROM (
           Select P.SOURCE_SYSTEM,
                  P.Program_name,
                  P.ENROLLED_DATE_CST,
                  TL.TRADELINE_NAME,
                  coalesce(case
                               when btl.LAST_PAYMENT_DATE_C < '1990-01-01' then crl.last_activity_date_c
                               else btl.LAST_PAYMENT_DATE_C end, crl.last_activity_date_c)           as last_payment_date,
                  datediff(day, coalesce(last_payment_date, p.ENROLLED_DATE_CST), $V_CALENDAR_DATE_CST) - 30 as DQ,

                  coalesce(iff(cc.id = 'a0y3h0000015BqsAAE', 0, ct.DAYS_DELINQUENT_MIN_C), 180)         DQ_MIN,
                  DQ + 75 - DQ_MIN                                                                      DQ_DELTA_BY_DAY75

           FROM {{ params.curated_database_name }}.CRM.TRADELINE tl
                    Left Join {{ params.curated_database_name }}.CRM.PROGRAM P
                              on P.PROGRAM_NAME = TL.PROGRAM_NAME and P.IS_CURRENT_RECORD_FLAG = TRUE
                    left join {{ params.refined_database_name }}.bedrock.program_tradeline_c as btl
                              on btl.NAME = tl.TRADELINE_NAME and btl.is_deleted = 'FALSE'
                    left join {{ params.refined_database_name }}.BEDROCK.opportunity_tradeline_c ot
                              on btl.opportunity_tradeline_id_c = ot.id and ot.is_deleted = FALSE
                    left join {{ params.refined_database_name }}.BEDROCK.cr_liability_c crl
                              on crl.id = ot.cr_liability_id_c and crl.is_deleted = FALSE
                    left join {{ params.refined_database_name }}.bedrock.creditor_terms_c ct on ct.is_deleted = false and ct.creditor_id_c =
                                                                                                    coalesce(
                                                                                                            btl.NEGOTIATING_CREDITOR_ID_C,
                                                                                                            btl.ORIGINATING_CREDITOR_ID_C)
                    left join {{ params.refined_database_name }}.BEDROCK.CREDITOR_C as cc
                              on cc.id = coalesce(btl.NEGOTIATING_CREDITOR_ID_C, btl.ORIGINATING_CREDITOR_ID_C) and
                                 cc.is_deleted = False
           Where tl.TRADELINE_SETTLEMENT_STATUS in ('ENROLLED')
             and TL.IS_CURRENT_RECORD_FLAG = TRUE
             and P.SOURCE_SYSTEM = 'BEDROCK'
             and P.PROGRAM_STATUS in ('Active', 'New', 'Enrolled')
               qualify row_number() over (PARTITION BY tl.TRADELINE_ID ORDER BY ct.DAYS_DELINQUENT_MIN_C ASC) = 1
       )
    GROUP BY PROGRAM_NAME
),
    creditor_matrix_settlement_logic as (
        --Getting DQ when creditors will first offer settlement
        with FIRST_ELIGIBLE_INFO as (

        select distinct
            CREDITOR_BUCKET_NAME,
            ORIGINAL_CREDITOR_ALIAS_ID,
            min(DAYS_DELINQUENT_MIN) as DAYS_DELINQUENT_MIN,
            NEGOTIATION_BALANCE_MIN,
            NEGOTIATION_BALANCE_MAX
        from {{ params.refined_database_name }}.CONFIGURATION.CREDITOR_TERMS
        where IPL_USE_FLAG = TRUE
        group by 1,2,4,5
        order by 1

        ),

        --Grabbing term, offer rate and min payment for intial offers
        FIRST_ELIGIBLE_INFO_2 as (

        select distinct
            a.*,
            coalesce(ct_alias_lump.current_creditor_bucketed,ct_no_alias_lump.current_creditor_bucketed) as current_creditor_bucketed,
            coalesce(ct_alias_lump.OFFER_PERCENT,ct_no_alias_lump.OFFER_PERCENT) as OFFER_PERCENT_NOT_LEGAL,
            coalesce(ct_alias_lump.OFFER_PERCENT,ct_no_alias_lump.OFFER_PERCENT) + coalesce(ct_alias_lump.LEGAL_RATE_INCREASE,ct_no_alias_lump.LEGAL_RATE_INCREASE,0) as OFFER_PERCENT_IS_LEGAL,
            coalesce(ct_alias_lump.AVG_OFFER_TERM,ct_no_alias_lump.AVG_OFFER_TERM) as AVG_OFFER_TERM,
            coalesce(ct_alias_lump.OFFER_MINIMUM_PAYMENT,ct_no_alias_lump.OFFER_MINIMUM_PAYMENT) as OFFER_MINIMUM_PAYMENT,
            coalesce(ct_alias_lump.CREDITOR_BUCKET_NAME,ct_no_alias_lump.CREDITOR_BUCKET_NAME) as CREDITOR_BUCKET_NAME_2

        from FIRST_ELIGIBLE_INFO as a

        --Only matching in IPL
        --when matching on original alias required - When lump sum offer expected (For IPL)
        left join {{ params.refined_database_name }}.CONFIGURATION.CREDITOR_TERMS as ct_alias_lump on ct_alias_lump.CREDITOR_BUCKET_NAME = a.CREDITOR_BUCKET_NAME
            and a.DAYS_DELINQUENT_MIN = ct_alias_lump.DAYS_DELINQUENT_MIN
            and a.NEGOTIATION_BALANCE_MIN = ct_alias_lump.NEGOTIATION_BALANCE_MIN
            and a.ORIGINAL_CREDITOR_ALIAS_ID = ct_alias_lump.ORIGINAL_CREDITOR_ALIAS_ID and ct_alias_lump.IPL_USE_FLAG = TRUE
        --when matching on original alias is not required - When lump sum offer expected (For IPL)
        left join {{ params.refined_database_name }}.CONFIGURATION.CREDITOR_TERMS as ct_no_alias_lump on ct_no_alias_lump.CREDITOR_BUCKET_NAME = a.CREDITOR_BUCKET_NAME
            and a.DAYS_DELINQUENT_MIN = ct_no_alias_lump.DAYS_DELINQUENT_MIN
            and a.NEGOTIATION_BALANCE_MIN = ct_no_alias_lump.NEGOTIATION_BALANCE_MIN
            and ct_no_alias_lump.ORIGINAL_CREDITOR_ALIAS_ID is null and ct_no_alias_lump.IPL_USE_FLAG = TRUE

        ),

        Tradelines as (
            Select P.SOURCE_SYSTEM,
                   P.Program_name,
                   P.ENROLLED_DATE_CST,
                   TL.TRADELINE_NAME,
                   tl.is_legal_flag,
                   coalesce(case
                                when btl.LAST_PAYMENT_DATE_C < '1990-01-01' then crl.last_activity_date_c
                                else btl.LAST_PAYMENT_DATE_C end, crl.last_activity_date_c,tl.current_last_payment_date_cst)           as last_payment_date,
                   datediff(day, coalesce(last_payment_date, p.ENROLLED_DATE_CST), current_date) - 15 as DQ,
                   coalesce(tl.CURRENT_CREDITOR_ID, tl.CURRENT_CREDITOR_ALIAS_ID, tl.ORIGINAL_CREDITOR_ID, tl.ORIGINAL_CREDITOR_ALIAS_ID) as CURRENT_CREDITOR_ID,
                   tl.ORIGINAL_CREDITOR_ALIAS_ID,
                   coalesce(tl.NEGOTIATION_BALANCE, tl.FEE_BASIS_BALANCE, tl.ENROLLED_BALANCE) as negotiation_balance
            FROM {{ params.curated_database_name }}.CRM.TRADELINE tl
                     Left Join {{ params.curated_database_name }}.CRM.PROGRAM P
                               on P.PROGRAM_NAME = TL.PROGRAM_NAME and P.IS_CURRENT_RECORD_FLAG = TRUE
                     left join {{ params.refined_database_name }}.bedrock.program_tradeline_c as btl
                               on btl.NAME = tl.TRADELINE_NAME and btl.is_deleted = 'FALSE'
                     left join {{ params.refined_database_name }}.BEDROCK.opportunity_tradeline_c ot
                               on btl.opportunity_tradeline_id_c = ot.id and ot.is_deleted = FALSE
                     left join {{ params.refined_database_name }}.BEDROCK.cr_liability_c crl
                               on crl.id = ot.cr_liability_id_c and crl.is_deleted = FALSE
            Where tl.TRADELINE_SETTLEMENT_STATUS not in ('SETTLED','ATTRITED','NOT ENROLLED')
                 and TL.IS_CURRENT_RECORD_FLAG = TRUE
                 and (TL.INCLUDE_IN_PROGRAM_FLAG=TRUE or (TL.CONDITIONAL_DEBT_STATUS in ('Pending') and TL.tradeline_settlement_status='ENROLLED'))
                 and P.SOURCE_SYSTEM = 'BEDROCK'
                 and P.PROGRAM_STATUS in ('Active','New','Enrolled') and tl.INCLUDE_IN_PROGRAM_FLAG=TRUE
        ),


        TRADELINES_W_EST_OFFER as (

          select distinct
              t.program_name,
              t.tradeline_name,
              t.negotiation_balance,
              t.CURRENT_CREDITOR_ID,
              c.creditor_name,
              t.DQ as CURRENT_DQ,
              case when cb.CREDITOR_BUCKET_NAME is null then 'N' else 'Y' end as TOP_50_CREDITOR,
              case when coalesce(ct_alias_lump.OFFER_PERCENT,ct_no_alias_lump.OFFER_PERCENT) is null then 'N' else 'Y' end as SETTLEMENT_ELIGIBLE_NOW, --i.e. meeting matrix eligibility criteria
              case
                  when SETTLEMENT_ELIGIBLE_NOW = 'Y' and t.IS_LEGAL_FLAG = FALSE then coalesce(ct_alias_lump.OFFER_PERCENT,ct_no_alias_lump.OFFER_PERCENT)
                  when SETTLEMENT_ELIGIBLE_NOW = 'Y' and t.IS_LEGAL_FLAG = TRUE then (coalesce(ct_alias_lump.OFFER_PERCENT,ct_no_alias_lump.OFFER_PERCENT) + coalesce(ct_alias_lump.LEGAL_RATE_INCREASE,ct_no_alias_lump.LEGAL_RATE_INCREASE,0))
                  when SETTLEMENT_ELIGIBLE_NOW = 'N' and t.IS_LEGAL_FLAG = FALSE then coalesce(ctf_alias_lump.OFFER_PERCENT_NOT_LEGAL,ctf_no_alias_lump.OFFER_PERCENT_NOT_LEGAL)
                  when SETTLEMENT_ELIGIBLE_NOW = 'N' and t.IS_LEGAL_FLAG = TRUE then coalesce(ctf_alias_lump.OFFER_PERCENT_IS_LEGAL,ctf_no_alias_lump.OFFER_PERCENT_IS_LEGAL)
              else null end as EST_OFFER_PERCENT,
              case
                  when SETTLEMENT_ELIGIBLE_NOW = 'Y' then coalesce(ct_alias_lump.CREDITOR_BUCKET_NAME,ct_no_alias_lump.CREDITOR_BUCKET_NAME)
                  when SETTLEMENT_ELIGIBLE_NOW = 'N' then coalesce(ctf_alias_lump.CREDITOR_BUCKET_NAME,ctf_no_alias_lump.CREDITOR_BUCKET_NAME)
              else null end as CREDITOR_BUCKET_NAME

          from TRADELINES as t
          LEFT JOIN {{ params.curated_database_name }}.CRM.CREDITOR C
              ON T.CURRENT_CREDITOR_ID = C.CREDITOR_ID AND C.IS_CURRENT_RECORD_FLAG
          left join {{ params.refined_database_name }}.CONFIGURATION.CREDITOR_BUCKETS as cb on t.CURRENT_CREDITOR_ID = cb.CURRENT_CREDITOR_ID

          --currently eligibile tradelines
          --when matching on original alias required - When lump sum offer expected (For IPL)
          left join {{ params.refined_database_name }}.CONFIGURATION.CREDITOR_TERMS as ct_alias_lump on ct_alias_lump.CREDITOR_BUCKET_NAME = cb.CREDITOR_BUCKET_NAME
              and t.DQ >= ct_alias_lump.DAYS_DELINQUENT_MIN and (ct_alias_lump.DAYS_DELINQUENT_MAX is null or t.DQ < ct_alias_lump.DAYS_DELINQUENT_MAX)
              and t.negotiation_balance >= ct_alias_lump.NEGOTIATION_BALANCE_MIN and (ct_alias_lump.NEGOTIATION_BALANCE_MAX is null or t.negotiation_balance < ct_alias_lump.NEGOTIATION_BALANCE_MAX)
              and t.ORIGINAL_CREDITOR_ALIAS_ID = ct_alias_lump.ORIGINAL_CREDITOR_ALIAS_ID and ct_alias_lump.IPL_USE_FLAG = TRUE
          --when matching on original alias is not required - When lump sum offer expected (For IPL)
          left join {{ params.refined_database_name }}.CONFIGURATION.CREDITOR_TERMS as ct_no_alias_lump on ct_no_alias_lump.CREDITOR_BUCKET_NAME = cb.CREDITOR_BUCKET_NAME
              and t.DQ >= ct_no_alias_lump.DAYS_DELINQUENT_MIN and (ct_no_alias_lump.DAYS_DELINQUENT_MAX is null or t.DQ < ct_no_alias_lump.DAYS_DELINQUENT_MAX)
              and t.negotiation_balance >= ct_no_alias_lump.NEGOTIATION_BALANCE_MIN and (ct_no_alias_lump.NEGOTIATION_BALANCE_MAX is null or t.negotiation_balance < ct_no_alias_lump.NEGOTIATION_BALANCE_MAX)
              and ct_no_alias_lump.ORIGINAL_CREDITOR_ALIAS_ID is null and ct_no_alias_lump.IPL_USE_FLAG = TRUE

          --For matching info with future eligibile programs
          --when matching on original alias required - (IPL)
          left join FIRST_ELIGIBLE_INFO_2 as ctf_alias_lump on ctf_alias_lump.CREDITOR_BUCKET_NAME = cb.CREDITOR_BUCKET_NAME
              and t.negotiation_balance >= ctf_alias_lump.NEGOTIATION_BALANCE_MIN and (ctf_alias_lump.NEGOTIATION_BALANCE_MAX is null or t.negotiation_balance < ctf_alias_lump.NEGOTIATION_BALANCE_MAX)
              and t.ORIGINAL_CREDITOR_ALIAS_ID = ctf_alias_lump.ORIGINAL_CREDITOR_ALIAS_ID
          --when no matching on original alias required - (IPL)
          left join FIRST_ELIGIBLE_INFO_2 as ctf_no_alias_lump on ctf_no_alias_lump.CREDITOR_BUCKET_NAME = cb.CREDITOR_BUCKET_NAME
              and t.negotiation_balance >= ctf_no_alias_lump.NEGOTIATION_BALANCE_MIN and (ctf_no_alias_lump.NEGOTIATION_BALANCE_MAX is null or t.negotiation_balance < ctf_no_alias_lump.NEGOTIATION_BALANCE_MAX)
              and ctf_no_alias_lump.ORIGINAL_CREDITOR_ALIAS_ID is null

        )

        select PROGRAM_NAME, TRADELINE_NAME, creditor_name,
        --add case when to limit settlement rate to 100% max
        case when ((EST_OFFER_PERCENT + 3) / 100) > 1 then 1 else ((EST_OFFER_PERCENT + 3) / 100) end as OFFER_PERCENT --adding 3% buffer
        from TRADELINES_W_EST_OFFER
        where EST_OFFER_PERCENT is not null

      )

--new book bedrock

  select distinct
      P.PROGRAM_id client_id,
      right(left(P.PROGRAM_ID, 15), 3) || right(left(P.CLIENT_ID, 15), 3) as activation_code,
      CT.FIRST_NAME,
      Ct.LAST_NAME,
      ct.MAILING_STREET Mailing_Address,
      ct.MAILING_CITY CITY,
      ct.MAILING_STATE STATE,
      left(ct.MAILING_POSTAL_CODE,5) ZIP_Code,
      ct.EMAIL Email_Address,
      REPLACE(regexp_replace(nvl(ct.MOBILE_PHONE, nvl(ct.HOME_PHONE, nvl(ct.PHONE,ct.OTHER_PHONE))),'[.,\/#!$%\^&\*:{}=\_`~()-]'), ' ', '') Telephone_Number,
      ct.BIRTHDATE,
      ct.SSN_C SSN,
      nvl(current_draft_amt.Per_freq_amount, 0) DRAFT_AMOUNT,
      Case when p.PAYMENT_FREQUENCY='Semi-Monthly' then 'Twice Monthly' else p.PAYMENT_FREQUENCY end PAYMENT_FREQUENCY,
      pd.payment_date1                 last_payment_date,
      pd.payment_date2                 last_payment_date2,
      cast(sum(case when upper(tl_list.tradeline_settlement_status) not in ('SETTLED','ATTRITED')
                  then (coalesce(tl_list.NEGOTIATION_BALANCE, tl_list.FEE_BASIS_BALANCE, tl_list.original_balance)*1.00) * coalesce(matrix.OFFER_PERCENT,c.top_90_percent_settlement_pct,.70)
              when upper(tl_list.tradeline_settlement_status) in ('SETTLED') and tl_list.tradeline_settlement_sub_status not like 'BUSTED%' and tl_list.tradeline_settlement_sub_status != 'PAID OFF'
                  then tl_list.CREDITOR_PAYMENTS_OUTSTANDING_AMOUNT
              when upper(tl_list.tradeline_settlement_status) in ('SETTLED') and tl_list.tradeline_settlement_sub_status like 'BUSTED%'
                  then (coalesce(tl_list.NEGOTIATION_BALANCE, tl_list.FEE_BASIS_BALANCE, tl_list.original_balance)*1.00) * coalesce(matrix.OFFER_PERCENT,c.top_90_percent_settlement_pct,.70)
              else 0 end) over (partition by p.program_id)
          +
          sum(case when upper(tl_list.tradeline_settlement_status) not in ('SETTLED','ATTRITED')
                      then tl_list.estimated_fees
                  when upper(tl_list.tradeline_settlement_status) in ('SETTLED') and tl_list.tradeline_settlement_sub_status != 'PAID OFF'
                      then tl_list.fees_outstanding_amount
                  else 0 end) over (partition by p.program_id)
          -
          cft_account_balance.available_balance as decimal(18,2))
          + 6*(nvl(P.MONTHLY_LEGAL_SERVICE_FEE,0)+nvl(P.MONTHLY_TRUST_ACCOUNT_SERVICE_FEE,0))
          as Amount_Financed,

      sum(case when upper(tl_list.tradeline_settlement_status) not in ('SETTLED','ATTRITED')
                  then tl_list.estimated_fees
              when upper(tl_list.tradeline_settlement_status) in ('SETTLED') and tl_list.tradeline_settlement_sub_status != 'PAID OFF'
                  then tl_list.fees_outstanding_amount
              else 0 end) over (partition by p.program_id)
      + 6*(nvl(P.MONTHLY_LEGAL_SERVICE_FEE,0)+nvl(P.MONTHLY_TRUST_ACCOUNT_SERVICE_FEE,0))  as estimated_beyond_program_fees,

      cft_account_balance.available_balance total_deposits,

      coalesce(cast(sum(case when upper(tl_list.tradeline_settlement_status) not in ('SETTLED','ATTRITED')
                  then (coalesce(tl_list.NEGOTIATION_BALANCE, tl_list.FEE_BASIS_BALANCE, tl_list.original_balance)*1.00) * coalesce(matrix.OFFER_PERCENT,c.top_90_percent_settlement_pct,.70)
              when upper(tl_list.tradeline_settlement_status) in ('SETTLED') and tl_list.tradeline_settlement_sub_status not like 'BUSTED%' and tl_list.tradeline_settlement_sub_status != 'PAID OFF'
                  then tl_list.CREDITOR_PAYMENTS_OUTSTANDING_AMOUNT
              when upper(tl_list.tradeline_settlement_status) in ('SETTLED') and tl_list.tradeline_settlement_sub_status like 'BUSTED%'
                  then (coalesce(tl_list.NEGOTIATION_BALANCE, tl_list.FEE_BASIS_BALANCE, tl_list.original_balance)*1.00) * coalesce(matrix.OFFER_PERCENT,c.top_90_percent_settlement_pct,.70)
              else 0 end) over (partition by p.program_id, tl_list.tradeline_name, tl_list.current_creditor) as decimal(18,2)),0) as settlement_amount,

      tl_list.current_creditor tradeline_name,
      '' tradeline_account_number,
      ta.ROUTING_NUMBER_C cft_routing_number,
      ta.ACCOUNT_NUMBER_C cft_account_number,
      case when ta.ROUTING_NUMBER_C = 053101561 then 'WELLS FARGO BANK'
              when ta.ROUTING_NUMBER_C = 053112505 then 'AXOS BANK'
              else null
              end as cft_bank_name,
      concat(CT.FIRST_NAME,' ',CT.LAST_NAME) cft_account_holder_name,
      cft_account_balance.PROCESSOR_CLIENT_ID :: varchar    external_id,
      CASE WHEN coclients.ct>0 THEN TRUE ELSE FALSE END as CO_CLIENT,
      p.program_name  program_id,
      datediff(month,p.ENROLLED_DATE_CST,$V_CALENDAR_DATE_CST) months_since_enrollment,
      next_draft_date.next_draft_date next_payment_date
      ,nvl(active_debts.Unsettled_Debt,0) + nvl(num_of_settlements.term_pay_balance,0) as total_amount_enrolled_debt
      ,p.ENROLLED_DATE_CST as beyond_enrollment_date
      ,p.PROGRAM_STATUS as beyond_enrollment_status
      ,coalesce(last_nsf.nsf_3_mos,0) as nsfs_3_months
      ,tl_list.original_creditor
      ,coalesce(matrix.OFFER_PERCENT,c.top_90_percent_settlement_pct,.70) as settlement_percent
      ,tl_list.tradeline_settlement_status || ' - ' || tl_list.tradeline_settlement_sub_status as settled_tradelined_flag
      ,coalesce(da90.DepAdherence,0) as payment_adherence_ratio_3_months
      ,coalesce(da120.DepAdherence,0) as payment_adherence_ratio_4_months
      ,coalesce(da180.DepAdherence,0) as payment_adherence_ratio_6_months
      ,bank_account.HOLDER_S_NAME_C
      ,bank_account.BANK_NAME_C
      ,bank_account.ACCOUNT_NUMBER_C
      ,bank_account.ROUTING_NUMBER_C
      ,bank_account.TYPE_C
      ,historical_settlement_percent.HISTORICAL_SETTLEMENT_PERCENT
      ,remaining_deposits.BF_REMAINING_DEPOSITS
      ,current_draft_amt.amount MonthlyAmt
      ,coalesce(dq.ProgDQb75,0) DQ_Check
      ,tl_list.TRADELINE_NAME TL_NAME

  FROM {{ params.curated_database_name }}.CRM.PROGRAM p
  join {{ params.refined_database_name }}.BEDROCK.PROGRAM_C PC on PC.name=P.Program_name and PC.IS_DELETED=FALSE
  left join {{ params.refined_database_name }}.BEDROCK.ACCOUNT act on act.id = pc.ACCOUNT_ID_C and act.IS_DELETED=FALSE
  left join {{ params.refined_database_name }}.BEDROCK.ACCOUNT_CONTACT_RELATION ACR on ACR.ACCOUNT_ID=act.ID and ACR.RELATIONSHIP_C='Client' and ACR.IS_DELETED=FALSE
  left join {{ params.refined_database_name }}.BEDROCK.CONTACT CT on acr.CONTACT_ID=ct.id and ct.IS_DELETED=FALSE
  left join {{ params.refined_database_name }}.BEDROCK.TRUST_ACCOUNT_C ta on p.program_id = TA.PROGRAM_ID_C AND TA.IS_DELETED = FALSE 
  left join coclients on coclients.PROGRAM_NAME=p.PROGRAM_NAME
  --left join {{ params.refined_database_name }}.BEDROCK.ACCOUNT_CONTACT_RELATION CoClient on CoClient.ACCOUNT_ID=PC.ACCOUNT_ID_C and CoClient.RELATIONSHIP_C='Co-Client'
  left join last_nsf on last_nsf.program_id = P.program_id
  left join current_draft_amt on current_draft_amt.program_id = P.program_id
  left join num_of_settlements on num_of_settlements.program_id = P.program_id
  left join active_debts on active_debts.program_id = P.program_id
  left join payment_dates pd on pd.program_id = P.program_id
  left join tl_list on tl_list.PROGRAM_NAME = P.program_name and (tl_list.INCLUDE_IN_PROGRAM_FLAG=TRUE or (tl_list.CONDITIONAL_DEBT_STATUS in ('Pending') and tl_list.tradeline_settlement_status='ENROLLED'))
  left join deferral on deferral.program_id = P.program_id
--  left join {{ params.refined_database_name }}.SALESFORCE.NU_DSE_FEE_TEMPLATE_C ft ON ft.id = program.NU_DSE_FEE_TEMPLATE_C
  left join next_draft_date ndd on ndd.program_name = p.program_name
  left join creditor_settlements c on c.original_creditor = tl_list.original_creditor
      and c.current_creditor = tl_list.current_creditor
  left join termination_requested on termination_requested.program_id = p.program_id
  left join dnc dnc1 on dnc1.dnc_number = REPLACE(regexp_replace(ct.MOBILE_PHONE,'[.,\/#!$%\^&\*:{}=\_`~()-]'), ' ', '')
  left join dnc dnc2 on dnc2.dnc_number = REPLACE(regexp_replace(ct.PHONE,'[.,\/#!$%\^&\*:{}=\_`~()-]'), ' ', '')
  left join dnc dnc3 on dnc3.dnc_number = REPLACE(regexp_replace(ct.HOME_PHONE,'[.,\/#!$%\^&\*:{}=\_`~()-]'), ' ', '')
  left join dnc dnc4 on dnc4.dnc_number = REPLACE(regexp_replace(ct.OTHER_PHONE,'[.,\/#!$%\^&\*:{}=\_`~()-]'), ' ', '')
  left join prior_loan_applicant on prior_loan_applicant.Program_ID_C = p.PROGRAM_ID
  left join cft_monthly_fees on cft_monthly_fees.program_id = P.program_id
  left join blp_monthly_fee on blp_monthly_fee.program_id = P.program_id
  left join cft_account_balance on cft_account_balance.program_id = P.program_id
  left join cft_prior_month_payment on cft_prior_month_payment.program_id = P.program_id
  left join fico_score on fico_score.program_id = P.program_id
  left join new_or_aged_book on new_or_aged_book.program_id = P.program_id
  left join recent_payments on recent_payments.program_id = P.program_id
  left join schedule_adherence on schedule_adherence.program_id = P.program_id
  left join beyond_fees on beyond_fees.program_id = P.program_id
  left join fees_outstanding on fees_outstanding.program_id = P.program_id
  --left join fee_template on fee_template.program_id = program.id
  left join ccpa_phone ccpa1 on REPLACE(regexp_replace(CT.MOBILE_PHONE,'[.,\/#!$%\^&\*:{}=\_`~()-]'), ' ', '') = ccpa1.ccpa_phone
  left join ccpa_phone ccpa2 on REPLACE(regexp_replace(ct.HOME_PHONE,'[.,\/#!$%\^&\*:{}=\_`~()-]'), ' ', '') = ccpa2.ccpa_phone
  left join ccpa_phone ccpa3 on REPLACE(regexp_replace(ct.OTHER_PHONE,'[.,\/#!$%\^&\*:{}=\_`~()-]'), ' ', '') = ccpa3.ccpa_phone
  left join ccpa_phone ccpa4 on REPLACE(regexp_replace(ct.PHONE,'[.,\/#!$%\^&\*:{}=\_`~()-]'), ' ', '') = ccpa4.ccpa_phone
  left join ccpa_email ccpa5 on ct.EMAIL = ccpa5.ccpa_email
  left join full_payments on full_payments.program_id = p.Program_id
  left join next_draft_date on next_draft_date.program_id = P.Program_id
  left join deposit_adherence DA90 on DA90.program_name = P.program_name and DA90.Term=90
  left join deposit_adherence DA120 on DA120.program_name = P.program_name and DA120.Term=120
  left join deposit_adherence DA180 on DA180.program_name = P.program_name and DA180.Term=180
  left join bank_account on bank_account.NU_DSE_PROGRAM_C = P.Program_name
  left join historical_settlement_percent on historical_settlement_percent.program_name = P.Program_name
  left join remaining_deposits on remaining_deposits.program_name = P.Program_name
  left join dq on P.PROGRAM_NAME=DQ.PROGRAM_NAME
  left join program_no_cred on p.program_name = program_no_cred.program_name
  left join creditor_matrix_settlement_logic as matrix on tl_list.tradeline_name = matrix.tradeline_name
  where 1 = 1
  and P.IS_CURRENT_RECORD_FLAG=TRUE
  and new_or_aged_book.program_age_bucket = 'New Book'
  and P.PROGRAM_STATUS IN ('Active', 'New Client','Enrolled')
  and state IN ('CA','MI','TX','IN','NC','MO','AL','NM','TN','MS','MT','KY','FL','SD','AK','DC','OK','WI','NY','PA','VA','AZ','AR','UT','ID','LA','MD','NE','MA','GA','SC','OH')
  and (last_nsf.last_nsf_dt is null or last_nsf.last_nsf_dt < dateadd(month, -3, cast($V_CALENDAR_DATE_CST - 1 as date)))
  and coalesce(dnc1.dnc_number,dnc2.dnc_number,dnc3.dnc_number,dnc4.dnc_number) is null
  and prior_loan_applicant.Program_ID_C is null
  and (tl_list.tradeline_settlement_status not in ('ATTRITED', 'NOT ENROLLED')
       or (tl_list.tradeline_settlement_status != 'SETTLED' and tl_list.tradeline_settlement_sub_status != 'PAID OFF')
       or tl_list.tradeline_settlement_status is null)
  and p.CREATED_DATE_CST < dateadd(month, -3, cast($V_CALENDAR_DATE_CST - 1 as date))
  and recent_payments.program_id is not null
  and coalesce(DA90.DepAdherence,0) >= 0.95
  and termination_requested.program_id is null
  and fico_score.credit_score >= 540
  and ccpa1.ccpa_phone is null
  and ccpa2.ccpa_phone is null
  and ccpa3.ccpa_phone is null
  and ccpa4.ccpa_phone is null
  and ccpa5.ccpa_email is null
  and program_no_cred.program_name is null
  and not (state = 'MI' and p.CREATED_DATE_CST >= dateadd(month, -5, cast($V_CALENDAR_DATE_CST - 1 as date)))
  and DQ_Check>=0
  and ct.EMAIL is not null
  and coalesce(ct.MOBILE_PHONE,ct.HOME_PHONE,ct.PHONE,ct.OTHER_PHONE) is not null
  and ct.SSN_C is not null
  -- and full_payments.program_id is not null
  qualify (1- (MonthlyAmt - ((Amount_financed/.95) / current_draft_amt.discount_factor))
          / (case when MonthlyAmt > 0 then MonthlyAmt end)) <= 1.30

  union

  select distinct
      P.PROGRAM_id client_id,
      right(left(P.PROGRAM_ID, 15), 3) || right(left(P.CLIENT_ID, 15), 3) as activation_code,
      CT.FIRST_NAME,
      Ct.LAST_NAME,
      ct.MAILING_STREET Mailing_Address,
      ct.MAILING_CITY CITY,
      ct.MAILING_STATE STATE,
      left(ct.MAILING_POSTAL_CODE,5) ZIP_Code,
      ct.EMAIL Email_Address,
      REPLACE(regexp_replace(nvl(ct.MOBILE_PHONE, nvl(ct.HOME_PHONE, nvl(ct.PHONE,ct.OTHER_PHONE))),'[.,\/#!$%\^&\*:{}=\_`~()-]'), ' ', '') Telephone_Number,
      ct.BIRTHDATE,
      ct.SSN_C SSN,
      nvl(current_draft_amt.Per_freq_amount, 0) DRAFT_AMOUNT,
      Case when p.PAYMENT_FREQUENCY='Semi-Monthly' then 'Twice Monthly' else p.PAYMENT_FREQUENCY end PAYMENT_FREQUENCY,
      pd.payment_date1                 last_payment_date,
      pd.payment_date2                 last_payment_date2,
      cast(sum(case when upper(tl_list.tradeline_settlement_status) not in ('SETTLED','ATTRITED')
                  then (coalesce(tl_list.NEGOTIATION_BALANCE, tl_list.FEE_BASIS_BALANCE, tl_list.original_balance)*1.00) * coalesce(matrix.OFFER_PERCENT,c.top_90_percent_settlement_pct,.70)
              when upper(tl_list.tradeline_settlement_status) in ('SETTLED') and tl_list.tradeline_settlement_sub_status not like 'BUSTED%' and tl_list.tradeline_settlement_sub_status != 'PAID OFF'
                  then tl_list.CREDITOR_PAYMENTS_OUTSTANDING_AMOUNT
              when upper(tl_list.tradeline_settlement_status) in ('SETTLED') and tl_list.tradeline_settlement_sub_status like 'BUSTED%'
                  then (coalesce(tl_list.NEGOTIATION_BALANCE, tl_list.FEE_BASIS_BALANCE, tl_list.original_balance)*1.00) * coalesce(matrix.OFFER_PERCENT,c.top_90_percent_settlement_pct,.70)
              else 0 end) over (partition by p.program_id)
          +
          sum(case when upper(tl_list.tradeline_settlement_status) not in ('SETTLED','ATTRITED')
                  then tl_list.estimated_fees
              when upper(tl_list.tradeline_settlement_status) in ('SETTLED') and tl_list.tradeline_settlement_sub_status != 'PAID OFF'
                  then tl_list.fees_outstanding_amount
              else 0 end) over (partition by p.program_id)
          -
          cft_account_balance.available_balance as decimal(18,2))
          + 6*(nvl(P.MONTHLY_LEGAL_SERVICE_FEE,0)+nvl(P.MONTHLY_TRUST_ACCOUNT_SERVICE_FEE,0))
          as Amount_Financed,

      sum(case when upper(tl_list.tradeline_settlement_status) not in ('SETTLED','ATTRITED')
                  then tl_list.estimated_fees
              when upper(tl_list.tradeline_settlement_status) in ('SETTLED') and tl_list.tradeline_settlement_sub_status != 'PAID OFF'
                  then tl_list.fees_outstanding_amount
              else 0 end) over (partition by p.program_id)
      + 6*(nvl(P.MONTHLY_LEGAL_SERVICE_FEE,0)+nvl(P.MONTHLY_TRUST_ACCOUNT_SERVICE_FEE,0))  as estimated_beyond_program_fees,
      cft_account_balance.available_balance                             total_deposits,

      coalesce(cast(sum(case when upper(tl_list.tradeline_settlement_status) not in ('SETTLED','ATTRITED')
                  then (coalesce(tl_list.NEGOTIATION_BALANCE, tl_list.FEE_BASIS_BALANCE, tl_list.original_balance)*1.00) * coalesce(matrix.OFFER_PERCENT,c.top_90_percent_settlement_pct,.70)
              when upper(tl_list.tradeline_settlement_status) in ('SETTLED') and tl_list.tradeline_settlement_sub_status not like 'BUSTED%' and tl_list.tradeline_settlement_sub_status != 'PAID OFF'
                  then tl_list.CREDITOR_PAYMENTS_OUTSTANDING_AMOUNT
              when upper(tl_list.tradeline_settlement_status) in ('SETTLED') and tl_list.tradeline_settlement_sub_status like 'BUSTED%'
                  then (coalesce(tl_list.NEGOTIATION_BALANCE, tl_list.FEE_BASIS_BALANCE, tl_list.original_balance)*1.00) * coalesce(matrix.OFFER_PERCENT,c.top_90_percent_settlement_pct,.70)
              else 0 end) over (partition by p.program_id, tl_list.tradeline_name, tl_list.current_creditor) as decimal(18,2)),0) as settlement_amount,

      tl_list.current_creditor tradeline_name,
      '' tradeline_account_number,
      ta.ROUTING_NUMBER_C cft_routing_number,
      ta.ACCOUNT_NUMBER_C cft_account_number,
      case when ta.ROUTING_NUMBER_C = 053101561 then 'WELLS FARGO BANK'
              when ta.ROUTING_NUMBER_C = 053112505 then 'AXOS BANK'
              else null
              end as cft_bank_name,
     concat(CT.FIRST_NAME,' ',CT.LAST_NAME) cft_account_holder_name,
      cft_account_balance.PROCESSOR_CLIENT_ID ::varchar external_id,
      CASE WHEN CoClients.ct>0 THEN TRUE ELSE FALSE END as CO_CLIENT,
      p.PROGRAM_NAME  program_id,
      datediff(month,P.ENROLLED_DATE_CST,$V_CALENDAR_DATE_CST) months_since_enrollment,
      next_draft_date.next_draft_date next_payment_date
      ,nvl(active_debts.Unsettled_Debt,0) + nvl(num_of_settlements.term_pay_balance,0) as total_amount_enrolled_debt
      ,p.ENROLLED_DATE_CST as beyond_enrollment_date
      ,P.PROGRAM_STATUS as beyond_enrollment_status
      ,coalesce(last_nsf.nsf_3_mos,0) as nsfs_3_months
      ,tl_list.original_creditor
      ,coalesce(matrix.OFFER_PERCENT,c.top_90_percent_settlement_pct,.70) as settlement_percent
      ,tl_list.tradeline_settlement_status || ' - ' || tl_list.tradeline_settlement_sub_status as settled_tradelined_flag
      ,coalesce(da90.DepAdherence,0) as payment_adherence_ratio_3_months
      ,coalesce(da120.DepAdherence,0) as payment_adherence_ratio_4_months
      ,coalesce(da180.DepAdherence,0) as payment_adherence_ratio_6_months
      ,bank_account.HOLDER_S_NAME_C
      ,bank_account.BANK_NAME_C
      ,bank_account.ACCOUNT_NUMBER_C
      ,bank_account.ROUTING_NUMBER_C
      ,bank_account.TYPE_C
      ,historical_settlement_percent.HISTORICAL_SETTLEMENT_PERCENT
      ,remaining_deposits.BF_REMAINING_DEPOSITS
      ,current_draft_amt.amount MonthlyAmt
      ,null as DQ_Check
      ,tl_list.TRADELINE_NAME as TL_NAME

  FROM {{ params.curated_database_name }}.CRM.PROGRAM p
  join {{ params.refined_database_name }}.BEDROCK.PROGRAM_C PC on PC.name=P.Program_name and PC.IS_DELETED=FALSE
  left join {{ params.refined_database_name }}.BEDROCK.ACCOUNT act on act.id = pc.ACCOUNT_ID_C and act.IS_DELETED=FALSE
  left join {{ params.refined_database_name }}.BEDROCK.ACCOUNT_CONTACT_RELATION ACR on ACR.ACCOUNT_ID=act.ID and ACR.RELATIONSHIP_C='Client' and ACR.IS_DELETED=FALSE
  left join {{ params.refined_database_name }}.BEDROCK.CONTACT CT on acr.CONTACT_ID=ct.id and ct.IS_DELETED=FALSE
  left join {{ params.refined_database_name }}.BEDROCK.TRUST_ACCOUNT_C ta on p.program_id = TA.PROGRAM_ID_C AND TA.IS_DELETED = FALSE 
  left join CoClients on coclients.PROGRAM_NAME=P.PROGRAM_NAME
--  left join {{ params.refined_database_name }}.BEDROCK.ACCOUNT_CONTACT_RELATION CoClient on CoClient.ACCOUNT_ID=PC.ACCOUNT_ID_C and CoClient.RELATIONSHIP_C='Co-Client'
  left join last_nsf on last_nsf.program_id = P.program_id
  left join current_draft_amt on current_draft_amt.program_id = P.program_id
  left join num_of_settlements on num_of_settlements.program_id = P.program_id
  left join active_debts on active_debts.program_id = P.program_id
  left join payment_dates pd on pd.program_id = P.program_id
  left join tl_list on tl_list.PROGRAM_NAME = P.program_name and (tl_list.INCLUDE_IN_PROGRAM_FLAG=TRUE or (tl_list.CONDITIONAL_DEBT_STATUS in ('Pending') and tl_list.tradeline_settlement_status='ENROLLED'))
  left join deferral on deferral.program_id = P.program_id
--  left join {{ params.refined_database_name }}.SALESFORCE.NU_DSE_FEE_TEMPLATE_C ft ON ft.id = program.NU_DSE_FEE_TEMPLATE_C
  left join next_draft_date ndd on ndd.program_name = p.program_name
  left join creditor_settlements c on c.original_creditor = tl_list.original_creditor
      and c.current_creditor = tl_list.current_creditor
  left join termination_requested on termination_requested.program_id = p.program_id
  left join dnc on dnc.dnc_number = REPLACE(regexp_replace(nvl(ct.MOBILE_PHONE, nvl(ct.HOME_PHONE, nvl(ct.PHONE,ct.OTHER_PHONE))),'[.,\/#!$%\^&\*:{}=\_`~()-]'), ' ', '')
  left join prior_loan_applicant on prior_loan_applicant.Program_ID_C = p.PROGRAM_ID
  left join cft_monthly_fees on cft_monthly_fees.program_id = P.program_id
  left join blp_monthly_fee on blp_monthly_fee.program_id = P.program_id
  left join cft_account_balance on cft_account_balance.program_id = P.program_id
  left join cft_prior_month_payment on cft_prior_month_payment.program_id = P.program_id
  left join fico_score on fico_score.program_id = P.program_id
  left join new_or_aged_book on new_or_aged_book.program_id = P.program_id
  left join recent_payments on recent_payments.program_id = P.program_id
  left join schedule_adherence on schedule_adherence.program_id = P.program_id
  left join beyond_fees on beyond_fees.program_id = P.program_id
  left join fees_outstanding on fees_outstanding.program_id = P.program_id
  --left join fee_template on fee_template.program_id = program.id
  left join ccpa_phone ccpa1 on REPLACE(regexp_replace(CT.MOBILE_PHONE,'[.,\/#!$%\^&\*:{}=\_`~()-]'), ' ', '') = ccpa1.ccpa_phone
  left join ccpa_phone ccpa2 on REPLACE(regexp_replace(ct.HOME_PHONE,'[.,\/#!$%\^&\*:{}=\_`~()-]'), ' ', '') = ccpa2.ccpa_phone
  left join ccpa_phone ccpa3 on REPLACE(regexp_replace(ct.OTHER_PHONE,'[.,\/#!$%\^&\*:{}=\_`~()-]'), ' ', '') = ccpa3.ccpa_phone
  left join ccpa_phone ccpa4 on REPLACE(regexp_replace(ct.PHONE,'[.,\/#!$%\^&\*:{}=\_`~()-]'), ' ', '') = ccpa4.ccpa_phone
  left join ccpa_email ccpa5 on ct.EMAIL = ccpa5.ccpa_email
  left join full_payments on full_payments.program_id = p.Program_id
  left join next_draft_date on next_draft_date.program_id = P.Program_id
  left join deposit_adherence DA90 on DA90.program_name = P.program_name and DA90.Term=90
  left join deposit_adherence DA120 on DA120.program_name = P.program_name and DA120.Term=120
  left join deposit_adherence DA180 on DA180.program_name = P.program_name and DA180.Term=180
  left join bank_account on bank_account.NU_DSE_PROGRAM_C = P.Program_name
  left join historical_settlement_percent on historical_settlement_percent.program_name = P.Program_name
  left join remaining_deposits on remaining_deposits.program_name = P.Program_name
  left join program_no_cred on p.program_name = program_no_cred.program_name
  left join creditor_matrix_settlement_logic as matrix on tl_list.tradeline_name = matrix.tradeline_name
  where 1 = 1
  and P.IS_CURRENT_RECORD_FLAG=TRUE
  and new_or_aged_book.program_age_bucket = 'Aged Book'
  and P.PROGRAM_STATUS IN ('Active', 'New Client','Enrolled')
  and state IN ('CA','MI','TX','IN','NC','MO','AL','NM','TN','MS','MT','KY','FL','SD','AK','DC','OK','WI','NY','PA','VA','AZ','AR','UT','ID','LA','MD','NE','MA','GA','SC','OH')
  and (last_nsf.last_nsf_dt is null or last_nsf.last_nsf_dt < dateadd(month, -3, cast($V_CALENDAR_DATE_CST - 1 as date)))
  -- and act.NU_DSE_CFT_CO_CLIENT_ID_C is null
  and dnc.dnc_number is null
  and prior_loan_applicant.Program_ID_C is null
  and (tl_list.tradeline_settlement_status not in ('ATTRITED', 'NOT ENROLLED')
       or (tl_list.tradeline_settlement_status != 'SETTLED' and tl_list.tradeline_settlement_sub_status != 'PAID OFF')
       or tl_list.tradeline_settlement_status is null)
  and p.CREATED_DATE_CST < dateadd(month, -3, cast($V_CALENDAR_DATE_CST - 1 as date))
  and recent_payments.program_id is not null
  and coalesce(DA180.DepAdherence,0) >= 0.80
  and termination_requested.program_id is null
  --and fico_score.credit_score >= 600
  and ccpa1.ccpa_phone is null
  and ccpa2.ccpa_phone is null
  and ccpa3.ccpa_phone is null
  and ccpa4.ccpa_phone is null
  and ccpa5.ccpa_email is null
  and program_no_cred.program_name is null
  and ct.EMAIL is not null
  and coalesce(ct.MOBILE_PHONE,ct.HOME_PHONE,ct.PHONE,ct.OTHER_PHONE) is not null
  and ct.SSN_C is not null
  -- and full_payments.program_id is not null
  qualify (1- (MonthlyAmt - ((Amount_financed/.95) / current_draft_amt.discount_factor))
          / (case when MonthlyAmt > 0 then MonthlyAmt end)) <= 1.48

  ) b
where b.amount_financed >= (case when state = 'CA' then 5000 
                                 when state = 'MA' then 6500 
                                 else 1000 end) and b.amount_financed <= 71250
) ipl

LEFT JOIN {{ params.curated_database_name }}.CRM.PROGRAM p ON p.PROGRAM_NAME = ipl.program_id AND p.IS_CURRENT_RECORD_FLAG
where p.service_entity_name = 'Beyond Finance';

COMMIT;
