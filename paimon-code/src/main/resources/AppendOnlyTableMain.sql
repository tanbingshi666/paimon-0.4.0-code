CREATE
TEMPORARY TABLE IF NOT EXISTS kafka_source_hp_series (
  `factoryNum` STRING,
  `equType` STRING,
  `equNum` STRING,
  `workSta` STRING,
  `deptName` STRING,
  `roomNo` STRING,
  `bedNo` STRING,
  `state` STRING,
  `drugName` STRING,
  `injectMode` STRING,
  `presetValue` FLOAT,
  `speed` FLOAT,
  `alreadyInjectTime` INTEGER,
  `remainTime` INTEGER,
  `alreadyInjectValue` FLOAT,
  `residual` FLOAT,
  `alarm1` STRING,
  `alarm2` STRING,
  `alarm3` STRING,
  `alarm4` STRING,
  `pressureValue` FLOAT,
  `pressureUint` STRING,
  `ts` BIGINT,
  `ts_ltz` AS TO_TIMESTAMP_LTZ(ts, 3),
  WATERMARK FOR ts_ltz AS ts_ltz - INTERVAL '1' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'temp_06_12',
  'properties.bootstrap.servers' = 'hadoop101:9092',
  'properties.group.id' = 'MKT_INFUSION_PUMP_HP_SERIES_JSON_06_22',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);


CREATE TABLE IF NOT EXISTS ods_kafka_source_hp_series
(
    `factory_num`
    STRING,
    `equ_type`
    STRING,
    `equ_num`
    STRING,
    `work_sta`
    STRING,
    `dept_name`
    STRING,
    `room_no`
    STRING,
    `bed_no`
    STRING,
    `state`
    STRING,
    `drug_name`
    STRING,
    `inject_mode`
    STRING,
    `preset_value`
    FLOAT,
    `speed`
    FLOAT,
    `already_inject_time`
    INTEGER,
    `remain_time`
    INTEGER,
    `already_inject_value`
    FLOAT,
    `residual`
    FLOAT,
    `alarm1`
    STRING,
    `alarm2`
    STRING,
    `alarm3`
    STRING,
    `alarm4`
    STRING,
    `pressure_value`
    FLOAT,
    `pressure_uint`
    STRING,
    `ts`
    BIGINT,

    `dt`
    STRING
) PARTITIONED BY
(
    dt
) WITH (
      'bucket' = '3',
      'bucket-key' = 'factory_num',
      'write-mode' = 'append-only'
      );


INSERT INTO ods_kafka_source_hp_series
SELECT `factoryNum`                      AS `factory_num`,
       `equType`                         AS `equ_type`,
       `equNum`                          AS `equ_num`,
       `workSta`                         AS `work_sta`,
       `deptName`                        AS `dept_name`,
       `roomNo`                          AS `room_no`,
       `bedNo`                           AS `bed_no`,
       `state`                           AS `state`,
       `drugName`                        AS `drug_name`,
       `injectMode`                      AS `inject_mode`,
       `presetValue`                     AS `preset_value`,
       `speed`                           AS `speed`,
       `alreadyInjectTime`               AS `already_inject_time`,
       `remainTime`                      AS `remain_time`,
       `alreadyInjectValue`              AS `already_inject_value`,
       `residual`                        AS `residual`,
       `alarm1`                          AS `alarm1`,
       `alarm2`                          AS `alarm2`,
       `alarm3`                          AS `alarm3`,
       `alarm4`                          AS `alarm4`,
       `pressureValue`                   AS `pressure_value`,
       `pressureUint`                    AS `pressure_uint`,
       `ts`,
       DATE_FORMAT(ts_ltz, 'yyyy-MM-dd') AS `dt`
FROM kafka_source_hp_series;