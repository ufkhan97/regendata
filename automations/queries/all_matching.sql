WITH gg_rounds AS (
    SELECT
      gg."round_number" AS "round_num",
      gg."type" AS "type",
      gg."chain_name" AS "chain_name",
      gg."chain_id" AS "chain_id",
      gg."round_name" AS "round_name",
      gg."round_id" AS "round_id"
    FROM
       "experimental_views"."all_rounds_20240810172652" gg),
 grants_stack_matching as ( 
  SELECT 
     round_num,
     project_name as title,
     match_amount_in_usd,
     project_payout_address as recipient_address,
     project_id,
     im.round_id,
     im.chain_id
  FROM 
  indexer_matching im
  LEFT JOIN gg_rounds gg 
    ON gg.chain_id = im.chain_id 
    AND LOWER(gg.round_id) = im.round_id
  )
SELECT 
    round_num,
    title,
    match_amount_in_usd,
    recipient_address,
    project_id,
    round_id,
    chain_id
FROM 
    grants_stack_matching
UNION
SELECT
    round_num,
    title,
    match_amount_usd as match_amount_in_usd,
    payoutaddress as recipient_address,
    project_id,
    round_id,
    chain_id
FROM 
    static_matching