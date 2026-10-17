{% macro load_ohlcv_staging(manifest_uri) %}

    {% set sql %}
        BEGIN;

        DELETE FROM staging.ohlcv_1m;

        COPY staging.ohlcv_1m
        FROM '{{ manifest_uri }}'
        IAM_ROLE 'arn:aws:iam::850041402358:role/CryptoMarketRedshiftRead'
        MANIFEST
        FORMAT AS PARQUET;

        COMMIT;
    {% endset %}

    {% do run_query(sql) %}

{% endmacro %}