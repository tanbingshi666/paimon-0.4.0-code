CREATE
CATALOG meiotds WITH (
  'type' = 'paimon',
  'metastore' = 'hive',
  'uri' = 'thrift://hadoop102:9083',
  'warehouse' = 'hdfs://hadoop102:8020/user/hive/warehouse/meiotds'
);

CREATE
DATABASE IF NOT EXISTS USE equ_mkt_infusion_pump;

USE
equ_mkt_infusion_pump;

CREATE
TEMPORARY TABLE IF NOT EXISTS kafka_source_hp_series (
  `host` STRING,
  `port` BIGINT,
  `code` BIGINT,
  `header` MAP<STRING, STRING>,
  `data` MAP<STRING, STRING>,
  `ts` BIGINT,
  `ts_ltz` AS TO_TIMESTAMP_LTZ(ts, 3)
) WITH (
  'connector' = 'kafka',
  'topic' = 'MKT_INFUSION_PUMP_HP_SERIES_JSON',
  'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092',
  'properties.group.id' = 'MKT_INFUSION_PUMP_HP_SERIES_JSON_06_22',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);

CREATE TABLE IF NOT EXISTS ods_equ_mkt_infusion_pump_hp_series
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

    `host`
    STRING,
    `port`
    BIGINT,
    `code`
    BIGINT,
    `header`
    MAP<
    STRING,
    STRING>,
    `ts`
    BIGINT,

    `dt`
    STRING,
    `hh`
    STRING,

    PRIMARY
    KEY
(
    dt,
    hh,
    factory_num,
    ts
) NOT ENFORCED
    ) PARTITIONED BY
(
    dt,
    hh
) WITH (
      'bucket' = '3',
      'bucket-key' = 'factory_num, ts',
      'write-mode' = 'change-log'
      );

INSERT INTO ods_equ_mkt_infusion_pump_hp_series
SELECT `data`['factoryNum']                         as `factory_num`,
       `data`['equType']                            as `equ_type`,
       `data`['equNum']                             as `equ_num`,
       `data`['workSta']                            as `work_sta`,
       `data`['deptName']                           as `dept_name`,
       `data`['roomNo']                             as `room_no`,
       `data`['bedNo']                              as `bed_no`,
       `data`['state']                              as `state`,
       `data`['drugName']                           as `drug_name`,
       `data`['injectMode']                         as `inject_mode`,
       CAST(`data`['presetValue'] AS FLOAT)         as `preset_value`,
       CAST(`data`['speed'] AS FLOAT)               as `speed`,
       CAST(`data`['alreadyInjectTime'] AS INTEGER) as `already_inject_time`,
       CAST(`data`['remainTime'] AS INTEGER)        as `remain_time`,
       CAST(`data`['alreadyInjectValue'] AS FLOAT)  as `already_inject_value`,
       CAST(`data`['residual'] AS FLOAT)            as `residual`,
       `data`['alarm1']                             as `alarm1`,
       `data`['alarm2']                             as `alarm2`,
       `data`['alarm3']                             as `alarm3`,
       `data`['alarm4']                             as `alarm4`,
       CAST(`data`['pressureValue'] AS FLOAT)       as `pressure_value`,
       `data`['pressureUint']                       as `pressure_uint`,

       `host`,
       `port`,
       `code`,
       `header`,
       `ts`,

       DATE_FORMAT(`ts_ltz`, 'yyyy-MM-dd')          as dt,
       CAST(HOUR (`ts_ltz`) AS STRING)              as hh

FROM kafka_source_hp_series;