/*
 * Query Purpose: Aggregates GMV (Gross Merchandise Value) data across multiple sources 
 * (managers, donors, grantees, and Dune) with blockchain mapping and ENS name resolution.
 * 
 * Output: Returns consolidated GMV allocations with blockchain details, timestamps,
 * transaction information, and resolved ENS names where available.
 */

-- Chain ID to blockchain name mapping
WITH chain_mapping AS (
    -- Using VALUES clause for efficient static mapping
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

-- Calculate round manager statistics
round_managers AS (
    SELECT 
        r.chain_id,
        cm.chain_name AS blockchain,
        r.round_id,
        r.address,
        'manager' AS role,
        COUNT(r.address) OVER (PARTITION BY r.chain_id, r.round_id) AS count_addresses
    FROM indexer.round_roles r
    LEFT JOIN chain_mapping cm ON cm.chain_id = r.chain_id
),

-- Aggregate donation summaries by round
donations_summary AS (
    SELECT 
        MAX(d.timestamp)::timestamp with time zone AS tx_timestamp,
        d.chain_id,
        cm.chain_name AS blockchain,
        d.round_id,
        SUM(d.amount_in_usd) AS total_amount_in_usd
    FROM donations d
    LEFT JOIN chain_mapping cm ON cm.chain_id = d.chain_id
    GROUP BY d.chain_id, cm.chain_name, d.round_id
),

-- Calculate GMV for managers
manager_points AS (
    SELECT 
        d.blockchain,
        d.round_id,
        d.tx_timestamp,
        '' AS tx_hash,
        rm.address,
        'manager' AS role,
        d.total_amount_in_usd / rm.count_addresses AS gmv,
        NULL AS name
    FROM donations_summary d
    LEFT JOIN round_managers rm 
        ON rm.chain_id = d.chain_id
        AND rm.round_id = d.round_id
),

-- Calculate GMV for donors and grantees
donor_grantee_points AS (
    SELECT 
        cm.chain_name AS blockchain,
        d.round_id,
        d.timestamp::timestamp with time zone AS tx_timestamp,
        d.transaction_hash AS tx_hash,
        d.address,
        d.role,
        d.gmv,
        NULL AS name
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
),

-- Import Dune points data
dune_points AS (
    SELECT
        blockchain,
        '' AS round_id,
        tx_timestamp::timestamp with time zone AS tx_timestamp,
        tx_hash,
        address,
        role,
        number_of_points AS gmv,
        ens AS name
    FROM "experimental_views"."dune_allo_gmv_distirbutions_20241028210437"
),

-- Combine all GMV sources
all_points AS (
    SELECT * FROM manager_points
    UNION ALL
    SELECT * FROM donor_grantee_points
    UNION ALL 
    SELECT * FROM dune_points
)

-- Final result with ENS name resolution
SELECT 
    ap.blockchain,
    ap.round_id,
    ap.tx_timestamp,
    ap.tx_hash,
    ap.address,
    ap.role,
    ap.gmv,
    COALESCE(ap.name, ens.name) AS name
FROM all_points ap
LEFT JOIN "experimental_views"."ens_names_allo_donors_20241022231136" ens 
    ON ap.address = ens.address
WHERE ap.address IS NOT NULL;