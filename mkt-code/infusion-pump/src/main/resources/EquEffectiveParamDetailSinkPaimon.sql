CREATE
TEMPORARY TABLE IF NOT EXISTS `mkt_infusion_pump_equ_effective_param_detail` (
  `factoryNum` STRING,
  `equType` STRING,
  `equNum` STRING,

  `presetValue` FLOAT,
  `speed` FLOAT,
  `alreadyInjectTime` INTEGER,
  `remainTime` INTEGER,
  `alreadyInjectValue` FLOAT,
  `residual` FLOAT,
  `pressureValue` FLOAT,

  `workSta` STRING,
  `drugName` STRING,
  `injectMode` STRING,

  `deptName` STRING,
  `roomNo` STRING,
  `bedNo` STRING,

  `ts` BIGINT,
  `ts_ltz` AS TO_TIMESTAMP_LTZ(ts, 3),
  WATERMARK FOR ts_ltz AS ts_ltz
) WITH (
  'connector' = 'kafka',
  'topic' = 'MKT_INFUSION_PUMP_EQU_EFFECTIVE_PARAM_DETAIL',
  'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092',
  'properties.group.id' = 'MKT_INFUSION_PUMP_EQU_EFFECTIVE_PARAM_DETAIL_06_22',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);

CREATE TABLE IF NOT EXISTS `equ_mkt_infusion_pump`.`dwd_mkt_infusion_pump_equ_effective_param_detail`
(
    `factory_num`
    STRING,
    `equ_type`
    STRING,
    `equ_num`
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
    `pressure_value`
    FLOAT,

    `work_sta`
    STRING,
    `drug_name`
    STRING,
    `inject_mode`
    STRING,

    `dept_name`
    STRING,
    `room_no`
    STRING,
    `bed_no`
    STRING,

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
      'bucket' = '1',
      'bucket-key' = 'factory_num,ts',
      'write-mode' = 'change-log'
      );


INSERT INTO `equ_mkt_infusion_pump`.`dwd_mkt_infusion_pump_equ_effective_param_detail`
SELECT `factoryNum`                        AS `factory_num`,
       `equType`                           AS `equ_type`,
       `equNum`                            AS `equ_num`,

       `presetValue`                       AS `preset_value`,
       `speed`,
       `alreadyInjectTime`                 AS `already_inject_time`,
       `remainTime`                        AS `remain_time`,
       `alreadyInjectValue`                AS `already_inject_value`,
       `residual`,
       `pressureValue`                     AS `pressure_value`,

       `workSta`                           AS `work_sta`,
       `drugName`                          AS `drug_name`,
       `injectMode`                        AS `inject_mode`,

       `deptName`                          AS `dept_name`,
       `roomNo`                            AS `room_no`,
       `bedNo`                             AS `bed_no`,

       `ts`,
       DATE_FORMAT(`ts_ltz`, 'yyyy-MM-dd') as `dt`,
       CAST(HOUR (`ts_ltz`) AS STRING)     as `hh`
FROM mkt_infusion_pump_equ_effective_param_detail;