SELECT 
    r.id AS round_id,
    r.chain_id,
    (r."round_metadata" #>> array['name'])::text AS "round_name",
    md.value->>'projectId' AS project_id,
    md.value->>'projectName' AS project_name,
    md.value->>'applicationId' AS application_id,
    md.value->>'contributionsCount' AS contributions_count,
    md.value->>'matchAmountInToken' AS match_amount_in_token,
    md.value->>'matchPoolPercentage' AS match_pool_percentage,
    md.value->>'projectPayoutAddress' AS project_payout_address,
    md.value->>'originalMatchAmountInToken' AS original_match_amount_in_token,
    (CAST(md.value->>'matchPoolPercentage' AS NUMERIC) * r.match_amount_in_usd) AS match_amount_in_usd
FROM 
    rounds r,
    jsonb_array_elements(r.matching_distribution->'matchingDistribution') AS md(value)