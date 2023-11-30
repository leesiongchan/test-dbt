WITH
issues AS (
    SELECT
        group_id AS id,
        COUNT(report_type) AS total_errors
    FROM {{ ref('events_view') }}
    GROUP BY group_id
)

SELECT *
FROM issues
