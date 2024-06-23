WITH alpha_matching as (SELECT
  am."round_num" AS "round_num",
  am."title" AS "title",
  am."match_amount_dai" AS "match_amount_usd",
  am."sub_round_slug" AS "sub_round_slug",
  addr."address" AS "payoutaddress",
  'unknown' as "project_id"
FROM
  "public"."AlphaMatching" am
LEFT JOIN "public"."AlphaRoundGranteeAddresses" addr ON addr."title" = am."title"),
cgrants as (
  SELECT
    cg."grantid" AS "grant_id",
    cg."name" AS "name",
    cg."payoutaddress" AS "payoutaddress"
  FROM
    "public"."cgrantsGrants" cg
),
cgrants_matching as (

SELECT
  cgm."round_num" AS "round_num",
  cg."name" as "title",
  cgm."sub_round_slug" AS "sub_round_slug",
  cgm."match_amount" AS "match_amount_usd",
  cg."payoutaddress" AS "payoutaddress",
  cg."grant_id" AS "project_id"
FROM
  "public"."cgrantsMatches" cgm 
  LEFT JOIN cgrants cg ON cg."grant_id" = cgm."grant_id"),
gg_rounds AS (SELECT
    CASE 
      WHEN "program" = 'GG19' THEN '19'
      WHEN "program" = 'GG18' THEN '18'
      WHEN "program" = 'Beta' THEN '17'
      ELSE NULL 
    END AS "round_num",
  gg."type" AS "type",
  gg."chain_name" AS "chain_name",
  gg."chain_id" AS "chain_id",
  gg."round_name" AS "round_name",
  gg."round_id" AS "round_id"
FROM
  "public"."GrantsProgramRoundsOnGrantsStack" gg),
  grants_stack_matching as (
SELECT
  1
),
static_matching as (SELECT 
    "round_num",
    "title",
    "match_amount_usd",
    "payoutaddress",
    project_id,
    "sub_round_slug" as round_id,
    1 as chain_id
FROM alpha_matching
UNION 
SELECT 
    "round_num",
    "title",
    "match_amount_usd", 
    "payoutaddress",
    project_id::text,
    "sub_round_slug" as round_id,
    1 as chain_id
FROM cgrants_matching
)
SELECT * from static_matching 
