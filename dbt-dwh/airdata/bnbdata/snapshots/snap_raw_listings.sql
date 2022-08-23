{% snapshot snap_raw_listings %}
    {{ config(
        target_schema = 'dev',
        unique_key = 'id',
        strategy = 'timestamp',
        updated_at = 'updated_at',
        invalidate_hard_deletes = True
    ) }}

    SELECT * FROM {{ source('airbnb', 'listings') }}
{% endsnapshot %}

/* Snapshot data 
A snapshot is a copy of the table at a certain time AND 
additional snapshot fields to keep track of editing in subsequent rows


dbt snapshot (initial snapshot)

SELECT * FROM AIRBNB.DEV.SNAP_RAW_LISTINGS WHERE ID=3176; -- 1 raw with RAW_LISTINGS cols + sanpshot cols

UPDATE AIRBNB.RAW.RAW_LISTINGS SET MINIMUM_NIGHTS=30,
updated_at=CURRENT_TIMESTAMP() WHERE ID=3176;

dbt snapshot

SELECT * FROM AIRBNB.DEV.SNAP_RAW_LISTINGS WHERE ID=3176; -- 2 rows with DBT_VALID_FROM and DBT_VALID_TO
*/