WITH
stg_events AS (
    SELECT
        {{ json_extract_scalar('_airbyte_data', ['dateCreated']) }}::TEXT AS {{ adapter.quote('date_created') }},
        {{ json_extract_scalar('_airbyte_data', ['id']) }}::INTEGER,
        {{ json_extract_scalar('_airbyte_data', ['groupID']) }}::INTEGER AS {{ adapter.quote('group_id') }},
        {{ json_extract_scalar('_airbyte_data', ['culprit']) }}::TEXT,
        {{ json_extract_scalar('_airbyte_data', ['tags']) }}
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
