WITH alpha_donations AS (
  SELECT
    16 as "round_num",
    ad."title" AS "title",
    ad."round_name" AS "round_name",
    ad."usd_amount" AS "usd_amount",
    ad."block_time" AS "block_time",
    CONCAT('0', LOWER(SUBSTRING(ad."donor", 2))) AS voter,
    ag.address as "grantAddress",
    'unknown' as "project_id"
FROM
    "public"."AlphaRoundsAllDonations" ad
LEFT JOIN "public"."AlphaRoundGranteeAddresses" ag ON ag."title" = ad."title"),
cc as (
  SELECT
    LOWER(cc."voter_address") AS "voter_address",
    cc."grant_id" AS "project_id",
    CAST(cc."created_on" AS timestamp) AS "created_on",
    cc."amountUSD" AS "amountUSD",
    cc."checkout_type" AS "checkout_type",
    CASE 
      WHEN checkout_type = 'eth_std' THEN 1 
      WHEN checkout_type = 'eth_polygon' THEN 137
      WHEN checkout_type = 'eth_zksync' THEN 324
      ELSE 0
    END AS chain_id
FROM
    "public"."cgrantsContributions" cc
),
rt as (
  SELECT
    rt."round_num" AS "round_num",
    rt."start_date" AS "start_date",
    rt."end_date" AS "end_date"
  FROM
    "public"."cgrantsRoundTimings" rt
),
cg as (
  SELECT
    cg."grantid" AS "project_id",
    cg."name" AS "name",
    cg."payoutaddress" AS "payoutaddress"
  FROM
    "public"."cgrantsGrants" cg
),
cgrants_donations as (
  SELECT
    rt."round_num",
    cc."voter_address",
    cc."project_id",
    cg."name",
    cg."payoutaddress",
    cc."created_on",
    cc."amountUSD",
    cc."chain_id"
  FROM
    cc
  LEFT JOIN rt on cc.created_on > rt.start_date
    and cc.created_on < rt.end_date
    LEFT JOIN cg on cg."project_id" = cc."project_id"
)

SELECT 
  round_num::int,
  CONCAT('Gitcoin Grants ', round_num) as round_name,
  "voter_address" as "donor_address",
  "amountUSD" as "amount_in_usd",
  "payoutaddress" as "recipient_address", 
  "created_on" as "timestamp",
  "name" as "project_name",
  project_id::text,
  CAST(100000 + round_num::int AS TEXT) as round_id,
  chain_id,
  'CGrants' as "source"
FROM 
  cgrants_donations
UNION
SELECT 
  round_num::int,
  round_name,
  "voter" as "donor_address",
  "usd_amount" as "amount_in_usd",
  "grantAddress" as "recipient_address",
  "block_time" as "timestamp",
  "title" as "project_name",
  project_id,
  '' as round_id,
  1 as chain_id,
  'Alpha' as "source"
FROM 
  alpha_donations

