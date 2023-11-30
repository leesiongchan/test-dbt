WITH
issues AS (
    SELECT
        group_id AS id,
        COUNT(report_type) AS total_errors
    FROM {{ ref('events') }}
    GROUP BY group_id
)

SELECT *
FROM issues
