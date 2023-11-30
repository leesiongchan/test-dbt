WITH
stg_events AS (
    SELECT
        _airbyte_data.dateCreated::TEXT AS date_created,
        _airbyte_data.id::INTEGER,
        _airbyte_data.groupID::INTEGER AS group_id,
        _airbyte_data.title::TEXT AS culprit,
        _airbyte_data.tags
    FROM {{ source('airbyte', '_airbyte_raw_events')}}
),
events AS (
    SELECT
        e.date_created,
        e.id,
        e.group_id,
        e.culprit,
        et.value::TEXT AS report_type
    FROM stg_events e, e.tags AS et
    WHERE et.key = 'ReportType'
)

SELECT *
FROM events
