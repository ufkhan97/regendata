-- Common base mapping for all streams
WITH chain_mapping AS (
    SELECT *
    FROM (VALUES
        (1, 'ethereum'),
        (10, 'optimism'),
        (137, 'polygon'),
        (250, 'fantom'),
        (324, 'zksync'),
        (424, 'pgn'),
        (8453, 'base'),
        (42161, 'arbitrum'),
        (43114, 'avalanche'),
        (534352, 'scroll'),
        (1329, 'sei'),
        (11155111, 'sepolia'),
        (42220, 'celo'),
        (56, 'binance'),
        (100, 'gnosis'),
        (1284, 'moonbeam'),
        (1285, 'moonriver'),
        (25, 'cronos'),
        (128, 'huobi'),
        (1666600000, 'harmony'),
        (2222, 'kava'),
        (30, 'rsk'),
        (288, 'boba network'),
        (1313161554, 'aurora'),
        (42262, 'oasis emerald'),
        (1088, 'metis andromeda'),
        (66, 'okexchain'),
        (1101, 'polygon zkevm'),
        (592, 'astar'),
        (42, 'lukso')
    ) AS t(chain_id, chain_name)
),

-- Common round operators structure
round_operators AS (
    SELECT 
        r.chain_id,
        r.round_id,
        r.address,
        'round_operator' AS role,
        COUNT(r.address) OVER (PARTITION BY r.chain_id, r.round_id) AS count_addresses
    FROM indexer.round_roles r
    GROUP BY r.chain_id, r.round_id, r.address
),

-- STREAM 1: DONATIONS --
donations_summary AS (
    SELECT 
        d.chain_id,
        cm.chain_name AS blockchain,
        d.round_id,
        CASE 
            WHEN strategy_name = '' THEN 'allov1.QF'
            ELSE strategy_name 
        END as strategy_name,
        (r.round_metadata #>> '{name}')::TEXT AS round_name,
        d.transaction_hash as tx_hash,
        d.timestamp::timestamp with time zone as tx_timestamp,
        SUM(d.amount_in_usd) AS total_amount_in_usd
    FROM donations d
    LEFT JOIN chain_mapping cm ON cm.chain_id = d.chain_id
    LEFT JOIN rounds r on r.id = d.round_id AND r.chain_id = d.chain_id
    GROUP BY 1,2,3,4,5,6,7
),

donation_contract_devs AS (
    SELECT 
        strategy_name,
        blockchain,
        dev_address,
        COUNT(*) OVER (PARTITION BY strategy_name, blockchain) as address_count
    FROM (
        SELECT 
            strategy_name,
            blockchain,
            CASE 
                WHEN strategy_name IN ('allov2.DonationVotingMerkleDistributionDirectTransferStrategy', 'allov2.DirectAllocationStrategy')
                    THEN '0x8c180840fcbb90ce8464b4ecd12ab0f840c6647c'
                WHEN strategy_name IN ('allov1.QF') AND blockchain = 'optimism' 
                    THEN '0xb8cef765721a6da910f14be93e7684e9a3714123'
                ELSE '0x1fd06f088c720ba3b7a3634a8f021fdd485dca42'
            END as dev_address
        FROM (SELECT DISTINCT strategy_name, blockchain FROM donations_summary) base
        
        UNION ALL
        
        SELECT 
            COALESCE(strategy_name, 'allov1.QF') as strategy_name,
            blockchain,
            CASE 
                WHEN strategy_name = 'allov2.DonationVotingMerkleDistributionDirectTransferStrategy'
                    THEN '0x79427367e9be16353336d230de3031d489b1b3c3'
                ELSE '0xb8cef765721a6da910f14be93e7684e9a3714123'
            END as dev_address
        FROM (
            SELECT DISTINCT strategy_name, blockchain 
            FROM donations_summary 
            WHERE 
                (strategy_name = 'allov1.QF' AND blockchain = 'ethereum')
                OR (strategy_name = 'allov2.DonationVotingMerkleDistributionDirectTransferStrategy')
        ) eth_strategies
    ) all_addresses
    WHERE blockchain IS NOT NULL AND strategy_name != ''
),

donation_round_operator_gmv AS (
    SELECT 
        d.blockchain,
        d.chain_id,
        d.round_name as pool_name,
        d.round_id,
        d.tx_timestamp,
        d.tx_hash,
        ro.address,
        d.strategy_name,
        'round_operator' AS role,
        d.total_amount_in_usd / ro.count_addresses AS gmv
    FROM donations_summary d
    LEFT JOIN round_operators ro 
        ON ro.chain_id = d.chain_id
        AND ro.round_id = d.round_id
),

donation_contract_dev_gmv AS (
    SELECT 
        d.blockchain,
        d.chain_id,
        round_name as pool_name,
        d.round_id,
        d.tx_timestamp,
        d.tx_hash,
        ca.dev_address as address,
        d.strategy_name,
        'contract_dev' AS role,
        SUM(d.total_amount_in_usd / NULLIF(ca.address_count, 0)) AS gmv
    FROM donations_summary d
    LEFT JOIN donation_contract_devs ca 
        ON ca.strategy_name = d.strategy_name 
        AND ca.blockchain = d.blockchain
    GROUP BY 1,2,3,4,5,6,7,8,9
),

donation_donor_grantee_gmv AS (
    SELECT 
        cm.chain_name AS blockchain,
        d.chain_id,
        (r.round_metadata #>> '{name}')::TEXT AS pool_name,
        d.round_id,
        d.timestamp::timestamp with time zone AS tx_timestamp,
        d.transaction_hash AS tx_hash,
        d.address,
        CASE 
            WHEN r.strategy_name = '' THEN 'allov1.QF'
            ELSE r.strategy_name 
        END as strategy_name,
        d.role,
        d.gmv
    FROM (
        -- Donor GMV
        SELECT 
            chain_id,
            round_id,
            timestamp,
            transaction_hash,
            donor_address AS address,
            'donor' AS role,
            SUM(amount_in_usd) AS gmv
        FROM donations
        GROUP BY 1, 2, 3, 4, 5, 6
        
        UNION ALL
        
        -- Grantee GMV
        SELECT 
            chain_id,
            round_id,
            timestamp,
            transaction_hash,
            recipient_address AS address,
            'grantee' AS role,
            SUM(amount_in_usd) AS gmv
        FROM donations
        GROUP BY 1, 2, 3, 4, 5, 6
    ) d
    LEFT JOIN chain_mapping cm ON cm.chain_id = d.chain_id
    LEFT JOIN rounds r on r.id = d.round_id AND r.chain_id = d.chain_id
),

-- STREAM 2: DISTRIBUTIONS --
distribution_base AS (
    SELECT 
        chain_id,
        round_id,
        round_name,
        timestamp::timestamp with time zone as timestamp,
        project_name,
        recipient_address,
        transaction_hash,
        amount_in_usd,
        strategy_id,
        strategy_name,
        'matching' as source_type
    FROM (
        SELECT 
            r.id AS round_id,
            r.chain_id,
            (r.round_metadata #>> '{name}')::TEXT AS round_name,
            TO_TIMESTAMP(r.matching_distribution->>'blockTimestamp', 'YYYY-MM-DD"T"HH24:MI:SS.MSZ') AS timestamp,
            md.value->>'projectName' AS project_name,
            md.value->>'projectPayoutAddress' AS recipient_address,
            a.distribution_transaction as transaction_hash,
            CASE 
                WHEN r.id = '0xa1d52f9b5339792651861329a046dd912761e9a9' 
                THEN (CAST(md.value->>'matchPoolPercentage' AS NUMERIC) * r.match_amount_in_usd)/1000000000000 
                ELSE (CAST(md.value->>'matchPoolPercentage' AS NUMERIC) * r.match_amount_in_usd)
            END AS amount_in_usd,
            strategy_id,
            strategy_name
        FROM rounds r
        CROSS JOIN LATERAL jsonb_array_elements(r.matching_distribution->'matchingDistribution') AS md(value)
        LEFT JOIN applications a 
            ON a.chain_id = r.chain_id 
            AND a.round_id = r.id 
            AND a.id = (md.value->>'applicationId')
        WHERE r.chain_id != 11155111
    ) matching
    
    UNION ALL
    
    SELECT 
        ap.chain_id,
        ap.round_id,
        (r.round_metadata #>> '{name}')::TEXT AS round_name,
        timestamp::timestamp with time zone,
        (a."metadata" #>> '{application, project, title}')::text AS project_name,
        (a."metadata" #>> '{application, recipient}')::text AS recipient_address,
        transaction_hash,
        amount_in_usd,
        strategy_id,
        strategy_name,
        'direct' as source_type
    FROM applications_payouts ap 
    LEFT JOIN applications a 
        ON a.chain_id = ap.chain_id 
        AND a.round_id = ap.round_id 
        AND a.id = ap.application_id
    LEFT JOIN rounds r 
        ON ap.chain_id = r.chain_id 
        AND r.id = ap.round_id
    WHERE amount_in_usd > 0
),

distribution_contract_devs AS (
    SELECT 
        strategy_name,
        blockchain,
        dev_address,
        COUNT(*) OVER (PARTITION BY strategy_name, blockchain) as address_count
    FROM (
        SELECT 
            strategy_name,
            blockchain,
            CASE 
                WHEN strategy_name IN ('allov2.DonationVotingMerkleDistributionDirectTransferStrategy', 'allov2.DirectGrantsLiteStrategy')
                    THEN '0x8c180840fcbb90ce8464b4ecd12ab0f840c6647c'
                WHEN strategy_name IN ('allov1.QF', 'allov1.Direct') AND blockchain = 'optimism' 
                    THEN '0xb8cef765721a6da910f14be93e7684e9a3714123'
                ELSE '0x1fd06f088c720ba3b7a3634a8f021fdd485dca42'
            END as dev_address
        FROM (
            SELECT DISTINCT strategy_name, cm.chain_name as blockchain 
            FROM distribution_base d
            LEFT JOIN chain_mapping cm ON cm.chain_id = d.chain_id
        ) base
        
        UNION ALL
        
        SELECT 
            strategy_name,
            'ethereum' as blockchain,
            '0xb8cef765721a6da910f14be93e7684e9a3714123' as dev_address
        FROM (
            SELECT DISTINCT strategy_name 
            FROM distribution_base d
            LEFT JOIN chain_mapping cm ON cm.chain_id = d.chain_id
            WHERE strategy_name IN ('allov1.QF', 'allov1.Direct')
            AND cm.chain_name = 'ethereum'
        ) eth_strategies
    ) all_addresses
),

distribution_grantee_gmv AS (
    SELECT 
        cm.chain_name as blockchain,
        d.chain_id,
        round_name as pool_name,
        round_id,
        timestamp::timestamp with time zone as tx_timestamp,
        transaction_hash as tx_hash,
        recipient_address as address,
        strategy_name,
        'grantee' as role,
        amount_in_usd as gmv
    FROM distribution_base d
    LEFT JOIN chain_mapping cm ON cm.chain_id = d.chain_id
),

distribution_round_operator_gmv AS (
    SELECT 
        g.blockchain,
        g.chain_id,
        pool_name,
        g.round_id,
        g.tx_timestamp,
        tx_hash,
        ro.address,
        g.strategy_name,
        'round_operator' AS role,
        SUM(g.gmv / ro.count_addresses) AS gmv
    FROM distribution_grantee_gmv g
    LEFT JOIN round_operators ro 
        ON ro.chain_id = g.chain_id
        AND ro.round_id = g.round_id
    GROUP BY 1,2,3,4,5,6,7,8,9
),

distribution_contract_dev_gmv AS (
    SELECT 
        g.blockchain,
        g.chain_id,
        pool_name,
        g.round_id,
        g.tx_timestamp,
        tx_hash,
        ca.dev_address as address,
        g.strategy_name,
        'contract_dev' AS role,
        SUM(g.gmv / NULLIF(ca.address_count, 0)) AS gmv
    FROM distribution_grantee_gmv g
    LEFT JOIN distribution_contract_devs ca 
        ON ca.strategy_name = g.strategy_name 
        AND ca.blockchain = g.blockchain
    GROUP BY 1,2,3,4,5,6,7,8,9
),
-- STREAM 3: DUNE --
dune_gmv AS (
    SELECT
        blockchain,
        cm.chain_id AS chain_id,
        '' AS pool_name,
        '' AS round_id,
        tx_timestamp::timestamp WITH TIME ZONE AS tx_timestamp,
        tx_hash,
        address,
        strategy_name,
        role,
        gmv
    FROM "experimental_views"."allov2_distribution_events_for_leaderboard" dune
    LEFT JOIN chain_mapping cm 
        ON cm.chain_name = dune.blockchain
    WHERE strategy_name != 'DonationVotingMerkleDistributionDirectTransferStrategy'
)

-- Final combination of all streams
, all_gmv AS (
SELECT *, 'donation' as data_source FROM donation_round_operator_gmv
UNION ALL
SELECT *, 'donation' as data_source FROM donation_contract_dev_gmv
UNION ALL
SELECT *, 'donation' as data_source FROM donation_donor_grantee_gmv
UNION ALL
SELECT *, 'distribution' as data_source FROM distribution_round_operator_gmv
UNION ALL
SELECT *, 'distribution' as data_source FROM distribution_contract_dev_gmv
UNION ALL
SELECT *, 'distribution' as data_source FROM distribution_grantee_gmv
UNION ALL
SELECT *, 'dune' as data_source FROM dune_gmv)

SELECT 
    ag.blockchain,
    ag.round_id,
    ag.strategy_name,
    ag.pool_name,
    ag.tx_timestamp,
    ag.tx_hash,
    ag.address,
    ag.role,
    ag.gmv,
    ag.data_source,
    ens.name
FROM
    all_gmv ag
LEFT JOIN 
    "experimental_views"."ens_names_allo_donors_20241022231136" ens 
    ON ag.address = ens.address
WHERE 
    ag.address IS NOT NULL
    AND ag.chain_id != 11155111
    AND ag.gmv >= 0.5;