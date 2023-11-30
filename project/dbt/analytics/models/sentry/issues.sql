WITH
issues AS (
    SELECT
        group_id AS issue_id,
        COUNT(report_type) AS total_errors
    FROM {{ ref('events') }}
    GROUP BY issue_id
)

SELECT *
FROM issues
