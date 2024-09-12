WITH program_data AS (
    SELECT 
        program, chain_id, LOWER(round_id) as round_id
    FROM 
        "experimental_views"."all_rounds_20240810172652"
    WHERE 
        "round_number" IS NOT NULL
        AND LOWER("round_id") NOT IN ('0x911ae126be7d88155aa9254c91a49f4d85b83688', '0x40511f88b87b69496a3471cdbe1d3d25ac68e408', '0xc08008d47e3deb10b27fc1a75a96d97d11d58cf8', '0xb5c0939a9bb0c404b028d402493b86d9998af55e')
),

maci_round_stats AS (
    SELECT
        round_id,
        chain_id,
        SUM(voice_credit_balance) / 100000 * 3000 as USD,
        COUNT(DISTINCT contributor_address) as unique_donors,
        COUNT(DISTINCT transaction_hash) as transactions,
        MAX(timestamp) as latest_contribution
    FROM
        maci."contributions"
    GROUP BY
        round_id, chain_id
),

direct_grants AS (
    SELECT
        chain_id,
        round_id,
        SUM(amount_in_usd) as direct_grants_payout,
        MAX(timestamp) as last_payout_time
    FROM
        "public"."applications_payouts"
    WHERE 
        chain_id != 11155111
    GROUP BY    
        chain_id,
        round_id
),

direct_allocation_stats AS (
    SELECT 
        d.round_id, 
        d.chain_id, 
        MAX(d.timestamp) as donations_end_time, 
        SUM(d.amount_in_usd) as total_donated
    FROM 
        donations d
    JOIN 
        (SELECT id, chain_id FROM rounds WHERE strategy_name = 'allov2.DirectAllocationStrategy') r 
    ON 
        r.id = d.round_id AND r.chain_id = d.chain_id
    GROUP BY 
        d.round_id, d.chain_id
),

chain_names AS (
    SELECT * FROM (VALUES
        (1, 'Ethereum'), (10, 'Optimism'), (137, 'Polygon'), (250, 'Fantom'),
        (324, 'zkSync'), (424, 'PGN'), (8453, 'Base'), (42161, 'Arbitrum One'),
        (43114, 'Avalanche'), (534352, 'Scroll'), (1329, 'SEI'), (11155111, 'Sepolia')
    ) AS t(chain_id, chain_name)
),

round_totals AS (
    SELECT
        "desc" AS "Name",
        "amount_in_usd" AS "Total USD",
        0 AS "Crowdfunding USD",
        "amount_in_usd" AS "Matching USD",
        0 AS "Donations",
        0 AS "Donors",
        0 AS "Avg Donor Contribution",
        "network_name" AS "Chain",
        "timestamp" AS "Indexed Time",
        "strategy_name" AS "Strategy",
        '0' AS "Chain ID",
        '0' AS "Round ID"
    FROM
        "public"."AlloRoundsOutsideIndexer"
    WHERE
        timestamp <= CURRENT_TIMESTAMP

    UNION ALL

    SELECT
        CASE 
            WHEN r.strategy_name = 'allov2.DirectAllocationStrategy' THEN 'Direct Donations Round'
            ELSE (r."round_metadata" #>> array['name'])::text 
        END AS "Name",
        COALESCE(r."total_amount_donated_in_usd", 0) + 
        COALESCE(
            CASE 
                WHEN r."matching_distribution" IS NOT NULL THEN r."match_amount_in_usd"
                WHEN dg."direct_grants_payout" IS NOT NULL THEN dg."direct_grants_payout"
                ELSE 0 
            END, 0
        ) AS "Total USD",
        COALESCE(r."total_amount_donated_in_usd", 0) AS "Crowdfunding USD",
        COALESCE(
            CASE 
                WHEN r."matching_distribution" IS NOT NULL THEN r."match_amount_in_usd"
                WHEN dg."direct_grants_payout" IS NOT NULL THEN dg."direct_grants_payout"
                ELSE 0 
            END, 0
        ) AS "Matching USD",
        COALESCE(r."total_donations_count", 0) AS "Donations",
        COALESCE(r."unique_donors_count", 0) AS "Donors",
        CASE 
            WHEN COALESCE(r."unique_donors_count", 0) = 0 THEN 0
            ELSE COALESCE(r."total_amount_donated_in_usd", 0) / r."unique_donors_count"
        END AS "Avg Donor Contribution",
        cn.chain_name AS "Chain",
        COALESCE(
            CASE 
                WHEN r.strategy_name = 'allov2.DirectAllocationStrategy' THEN das.donations_end_time
                WHEN r."donations_end_time" IS NOT NULL THEN r."donations_end_time"
                WHEN dg.last_payout_time IS NOT NULL THEN dg."last_payout_time"
                ELSE r.applications_start_time 
            END,
            r.applications_start_time
        ) AS "Indexed Time",
        CASE 
            WHEN r.strategy_name IN ('allov1.QF', 'allov2.DonationVotingMerkleDistributionDirectTransferStrategy') THEN 'Allo QF'
            WHEN r.strategy_name IN ('allov1.Direct', 'allov2.DirectGrantsLiteStrategy') THEN 'Direct Grants'
            WHEN r.strategy_name = 'allov2.DirectAllocationStrategy' THEN 'Direct Donations'
            ELSE r.strategy_name
        END AS "Strategy",
        r."chain_id"::text AS "Chain ID",
        r."id" AS "Round ID"
    FROM
        "rounds" AS r
    LEFT JOIN direct_grants dg ON dg.chain_id = r.chain_id AND dg.round_id = r.id
    LEFT JOIN direct_allocation_stats das ON das.chain_id = r.chain_id AND das.round_id = r.id
    LEFT JOIN chain_names cn ON cn.chain_id = r.chain_id
    WHERE 
        (r."donations_end_time" >= '2024-01-01' OR r."applications_start_time" >= '2024-01-01' OR r.strategy_name = 'allov2.DirectAllocationStrategy')
        AND r."chain_id" != 11155111

    UNION ALL

    SELECT
        (mr."round_metadata" #>> array['name'])::text AS "Name",
        COALESCE(mrs.USD, 0)  AS "Total USD",
        COALESCE(mrs.USD, 0) AS "Crowdfunding USD",
        0 AS "Matching USD",
        COALESCE(mrs.transactions, 0) AS "Donations",
        COALESCE(mrs.unique_donors, 0) AS "Donors",
        CASE 
            WHEN COALESCE(mrs.unique_donors, 0) = 0 THEN 0
            ELSE COALESCE(mrs.USD, 0) / mrs.unique_donors
        END AS "Avg Donor Contribution",
        cn.chain_name AS "Chain",
        mr."donations_end_time" AS "Indexed Time",
        CASE 
            WHEN mr.strategy_name = 'allov2.MACIQF' THEN 'MACI QF'
            ELSE mr.strategy_name
        END AS "Strategy",
        mr."chain_id"::text AS "Chain ID",
        mr."id" AS "Round ID"
    FROM
        maci."rounds" AS mr
    LEFT JOIN maci_round_stats AS mrs ON mrs.round_id = mr.id AND mrs.chain_id = mr.chain_id
    LEFT JOIN chain_names cn ON cn.chain_id = mr.chain_id
    WHERE 
        mr."chain_id" != 11155111
)

SELECT
    p.program,
    r.*
FROM 
    round_totals r
LEFT JOIN program_data p ON p.chain_id = CAST(r."Chain ID" AS int) AND p.round_id = r."Round ID"
ORDER BY "Total USD" DESC;