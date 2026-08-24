from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="smart_city_traffic_etl",
    start_date=datetime(2026, 8, 24),
    schedule_interval=None,
    catchup=False,
) as dag:

    check_raw_file = BashOperator(
        task_id="check_raw_file",
        bash_command="hdfs dfs -test -e /data/traffic/raw/traffic_events.csv"
    )

    create_staging_table = BashOperator(
        task_id="create_staging_table",
        bash_command="""
        beeline -u jdbc:hive2://hive-server:10000 -e "
        CREATE EXTERNAL TABLE IF NOT EXISTS traffic.stg_traffic_events (
            event_id INT,
            road_id STRING,
            city STRING,
            event_type STRING,
            speed_kmh INT,
            vehicle_count INT,
            severity INT,
            event_time STRING
        )
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
        LOCATION '/data/traffic/raw'
        TBLPROPERTIES ('skip.header.line.count'='1');
        "
        """
    )

    create_clean_events = BashOperator(
        task_id="create_clean_events",
        bash_command="""
        beeline -u jdbc:hive2://hive-server:10000 -e "
        DROP TABLE IF EXISTS traffic.clean_traffic_events;

        CREATE TABLE traffic.clean_traffic_events
        STORED AS PARQUET
        AS
        SELECT
            event_id,
            road_id,
            city,
            event_type,
            speed_kmh,
            vehicle_count,
            severity,
            event_time
        FROM traffic.stg_traffic_events
        WHERE speed_kmh >= 0
          AND vehicle_count >= 0
          AND severity BETWEEN 1 AND 5
          AND event_type IN ('ACCIDENT','CONGESTION','ROAD_CLOSED','NORMAL');
        "
        """
    )

    create_city_summary = BashOperator(
        task_id="create_city_summary",
        bash_command="""
        beeline -u jdbc:hive2://hive-server:10000 -e "
        DROP TABLE IF EXISTS traffic.city_traffic_summary;

        CREATE TABLE traffic.city_traffic_summary
        STORED AS PARQUET
        AS
        SELECT
            city,
            COUNT(*) AS total_events,
            SUM(CASE WHEN event_type = 'ACCIDENT' THEN 1 ELSE 0 END) AS accident_count,
            AVG(speed_kmh) AS avg_speed,
            SUM(vehicle_count) AS total_vehicles
        FROM traffic.clean_traffic_events
        GROUP BY city;
        "
        """
    )

    create_high_risk_roads = BashOperator(
        task_id="create_high_risk_roads",
        bash_command="""
        beeline -u jdbc:hive2://hive-server:10000 -e "
        DROP TABLE IF EXISTS traffic.high_risk_roads;

        CREATE TABLE traffic.high_risk_roads
        STORED AS PARQUET
        AS
        SELECT
            road_id,
            city,
            COUNT(*) AS high_risk_events,
            MAX(severity) AS max_severity
        FROM traffic.clean_traffic_events
        WHERE severity >= 4
           OR event_type = 'ACCIDENT'
        GROUP BY road_id, city;
        "
        """
    )

    check_raw_file \
        >> create_staging_table \
        >> create_clean_events \
        >> create_city_summary \
        >> create_high_risk_roads

