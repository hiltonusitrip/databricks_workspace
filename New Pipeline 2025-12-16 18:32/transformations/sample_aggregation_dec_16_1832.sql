-- This file defines a sample transformation.
-- Edit the sample below or add new transformations
-- using "+ Add" in the file browser.

CREATE MATERIALIZED VIEW sample_aggregation_dec_16_1832 AS
SELECT
    user_type,
    COUNT(user_type) AS total_count
FROM sample_users_dec_16_1832
GROUP BY user_type;
