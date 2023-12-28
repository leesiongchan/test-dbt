WITH sentry_events AS (
    SELECT
        datecreated AS date_created,
        id,
        culprit,
        groupid AS group_id,
        projectid AS project_id,
        tags,
        title
    FROM {{ source('airbyte', 'airbyte_events') }}
    {% if is_incremental() %}
    WHERE _airbyte_emitted_at > (SELECT MAX(this._airbyte_emitted_at) FROM {{ this }} this)
    {% endif %}

),
cet_sentry_events AS (
    SELECT
        e.*,
        et.value::TEXT AS report_type
    FROM sentry_events e, e.tags AS et
    WHERE et.key = 'ReportType'
)

SELECT * FROM cet_sentry_events
