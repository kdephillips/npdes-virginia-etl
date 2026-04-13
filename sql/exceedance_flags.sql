-- ============================================================
-- Virginia NPDES Exceedance Flags
-- Purpose: Identify permit limit exceedances by comparing
--          actual DMR values against permitted limits
-- Author: [Your Name]
-- Last Updated: [Date]
-- ============================================================

SELECT
    d.EXTERNAL_PERMIT_NMBR          AS permit_id,
    d.PERM_FEATURE_NMBR             AS outfall_number,
    d.PARAMETER_CODE                AS parameter_code,
    d.PARAMETER_DESC                AS pollutant,
    d.MONITORING_PERIOD_END_DATE    AS monitoring_period,
    d.DMR_VALUE_NMBR                AS actual_value,
    d.DMR_VALUE_STANDARD_UNITS      AS actual_units,
    l.LIMIT_VALUE_NMBR              AS permit_limit,
    l.LIMIT_VALUE_STANDARD_UNITS    AS limit_units,
    l.STATISTICAL_BASE_CODE         AS limit_type,

    -- Compliance status flag
    CASE
        WHEN d.DMR_VALUE_NMBR IS NULL THEN 'No Data Reported'
        WHEN l.LIMIT_VALUE_NMBR IS NULL THEN 'Monitor Only'
        WHEN CAST(d.DMR_VALUE_NMBR AS FLOAT) > CAST(l.LIMIT_VALUE_NMBR AS FLOAT)
            THEN 'Exceedance'
        ELSE 'Compliant'
    END AS compliance_status,

    -- Magnitude of exceedance (negative = under limit)
    CASE
        WHEN d.DMR_VALUE_NMBR IS NOT NULL AND l.LIMIT_VALUE_NMBR IS NOT NULL
            THEN CAST(d.DMR_VALUE_NMBR AS FLOAT) - CAST(l.LIMIT_VALUE_NMBR AS FLOAT)
        ELSE NULL
    END AS exceedance_magnitude,

    -- Percent over limit
    CASE
        WHEN d.DMR_VALUE_NMBR IS NOT NULL
            AND l.LIMIT_VALUE_NMBR IS NOT NULL
            AND CAST(l.LIMIT_VALUE_NMBR AS FLOAT) > 0
            THEN ROUND(
                (CAST(d.DMR_VALUE_NMBR AS FLOAT) - CAST(l.LIMIT_VALUE_NMBR AS FLOAT))
                / CAST(l.LIMIT_VALUE_NMBR AS FLOAT) * 100, 2)
        ELSE NULL
    END AS exceedance_pct

FROM dmr_data d
LEFT JOIN limits_data l
    ON  d.EXTERNAL_PERMIT_NMBR  = l.EXTERNAL_PERMIT_NMBR
    AND d.PERM_FEATURE_NMBR     = l.PERM_FEATURE_NMBR
    AND d.PARAMETER_CODE        = l.PARAMETER_CODE

WHERE d.EXTERNAL_PERMIT_NMBR LIKE 'VA0%'

ORDER BY exceedance_magnitude DESC NULLS LAST;