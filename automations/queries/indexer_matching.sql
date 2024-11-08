SELECT 
    r.id AS round_id,
    r.chain_id,
    (r.round_metadata #>> '{name}')::TEXT AS round_name,
    TO_TIMESTAMP(r.matching_distribution->>'blockTimestamp', 'YYYY-MM-DD"T"HH24:MI:SS.MSZ') AS timestamp,
    md.value->>'projectId' AS project_id,
    md.value->>'projectName' AS project_name,
    md.value->>'applicationId' AS application_id,
    md.value->>'contributionsCount' AS contributions_count,
    md.value->>'matchAmountInToken' AS match_amount_in_token,
    md.value->>'matchPoolPercentage' AS match_pool_percentage,
    md.value->>'projectPayoutAddress' AS project_payout_address,
    md.value->>'originalMatchAmountInToken' AS original_match_amount_in_token,
    CASE 
        WHEN r.id = '0xa1d52f9b5339792651861329a046dd912761e9a9' THEN (CAST(md.value->>'matchPoolPercentage' AS NUMERIC) * r.match_amount_in_usd)/1000000000000
        ELSE (CAST(md.value->>'matchPoolPercentage' AS NUMERIC) * r.match_amount_in_usd)
    END AS match_amount_in_usd
FROM rounds r
CROSS JOIN LATERAL
    jsonb_array_elements(r.matching_distribution->'matchingDistribution') AS md(value)
WHERE 
    r.chain_id != 11155111
