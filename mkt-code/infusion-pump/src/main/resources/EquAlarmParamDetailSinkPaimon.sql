CREATE
TEMPORARY TABLE IF NOT EXISTS `mkt_infusion_pump_equ_alarm_param_detail` (
  `factoryNum` STRING,
  `injectMode` STRING,

  `alarmType` STRING,
  `alarmValue` STRING,

  `deptName` STRING,
  `roomNo` STRING,
  `bedNo` STRING,

  `alarmTs` BIGINT,
  `ts_ltz` AS TO_TIMESTAMP_LTZ(alarmTs, 3),
  WATERMARK FOR ts_ltz AS ts_ltz
) WITH (
  'connector' = 'kafka',
  'topic' = 'MKT_INFUSION_PUMP_EQU_ALARM_PARAM_DETAIL',
  'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092',
  'properties.group.id' = 'MKT_INFUSION_PUMP_EQU_ALARM_PARAM_DETAIL_06_22',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);


CREATE TABLE IF NOT EXISTS `equ_mkt_infusion_pump`.`dwd_mkt_infusion_pump_equ_alarm_param_detail`
(
    `factory_num`
    STRING,
    `inject_mode`
    STRING,

    `alarm_type`
    STRING,

    `alarm1`
    TINYINT,
    `alarm2`
    TINYINT,
    `alarm3`
    TINYINT,
    `alarm4`
    TINYINT,
    `alarm5`
    TINYINT,
    `alarm6`
    TINYINT,
    `alarm7`
    TINYINT,
    `alarm8`
    TINYINT,

    `alarm9`
    TINYINT,
    `alarm10`
    TINYINT,
    `alarm11`
    TINYINT,
    `alarm12`
    TINYINT,
    `alarm13`
    TINYINT,
    `alarm14`
    TINYINT,
    `alarm15`
    TINYINT,
    `alarm16`
    TINYINT,

    `alarm17`
    TINYINT,
    `alarm18`
    TINYINT,
    `alarm19`
    TINYINT,
    `alarm20`
    TINYINT,
    `alarm21`
    TINYINT,
    `alarm22`
    TINYINT,
    `alarm23`
    TINYINT,
    `alarm24`
    TINYINT,

    `alarm25`
    TINYINT,
    `alarm26`
    TINYINT,
    `alarm27`
    TINYINT,
    `alarm28`
    TINYINT,
    `alarm29`
    TINYINT,
    `alarm30`
    TINYINT,
    `alarm31`
    TINYINT,
    `alarm32`
    TINYINT,

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
    alarm_type,
    ts
) NOT ENFORCED
    ) PARTITIONED BY
(
    dt,
    hh
) WITH (
      'bucket' = '1',
      'bucket-key' = 'factory_num,alarm_type,ts',
      'write-mode' = 'change-log'
      );

INSERT INTO `equ_mkt_infusion_pump`.`dwd_mkt_infusion_pump_equ_alarm_param_detail`
SELECT `factoryNum`                                           AS `factory_num`,
       `injectMode`                                           AS `inject_mode`,

       `alarmType`                                            AS `alarm_type`,

       CAST(SUBSTRING(`alarmValue` FROM 1 FOR 1) AS TINYINT)  AS `alarm1`,
       CAST(SUBSTRING(`alarmValue` FROM 2 FOR 1) AS TINYINT)  AS `alarm2`,
       CAST(SUBSTRING(`alarmValue` FROM 3 FOR 1) AS TINYINT)  AS `alarm3`,
       CAST(SUBSTRING(`alarmValue` FROM 4 FOR 1) AS TINYINT)  AS `alarm4`,
       CAST(SUBSTRING(`alarmValue` FROM 5 FOR 1) AS TINYINT)  AS `alarm5`,
       CAST(SUBSTRING(`alarmValue` FROM 6 FOR 1) AS TINYINT)  AS `alarm6`,
       CAST(SUBSTRING(`alarmValue` FROM 7 FOR 1) AS TINYINT)  AS `alarm7`,
       CAST(SUBSTRING(`alarmValue` FROM 8 FOR 1) AS TINYINT)  AS `alarm8`,

       CAST(SUBSTRING(`alarmValue` FROM 9 FOR 1) AS TINYINT)  AS `alarm9`,
       CAST(SUBSTRING(`alarmValue` FROM 10 FOR 1) AS TINYINT) AS `alarm10`,
       CAST(SUBSTRING(`alarmValue` FROM 11 FOR 1) AS TINYINT) AS `alarm11`,
       CAST(SUBSTRING(`alarmValue` FROM 12 FOR 1) AS TINYINT) AS `alarm12`,
       CAST(SUBSTRING(`alarmValue` FROM 13 FOR 1) AS TINYINT) AS `alarm13`,
       CAST(SUBSTRING(`alarmValue` FROM 14 FOR 1) AS TINYINT) AS `alarm14`,
       CAST(SUBSTRING(`alarmValue` FROM 15 FOR 1) AS TINYINT) AS `alarm15`,
       CAST(SUBSTRING(`alarmValue` FROM 16 FOR 1) AS TINYINT) AS `alarm16`,

       CAST(SUBSTRING(`alarmValue` FROM 17 FOR 1) AS TINYINT) AS `alarm17`,
       CAST(SUBSTRING(`alarmValue` FROM 18 FOR 1) AS TINYINT) AS `alarm18`,
       CAST(SUBSTRING(`alarmValue` FROM 19 FOR 1) AS TINYINT) AS `alarm19`,
       CAST(SUBSTRING(`alarmValue` FROM 20 FOR 1) AS TINYINT) AS `alarm20`,
       CAST(SUBSTRING(`alarmValue` FROM 21 FOR 1) AS TINYINT) AS `alarm21`,
       CAST(SUBSTRING(`alarmValue` FROM 22 FOR 1) AS TINYINT) AS `alarm22`,
       CAST(SUBSTRING(`alarmValue` FROM 23 FOR 1) AS TINYINT) AS `alarm23`,
       CAST(SUBSTRING(`alarmValue` FROM 24 FOR 1) AS TINYINT) AS `alarm24`,

       CAST(SUBSTRING(`alarmValue` FROM 25 FOR 1) AS TINYINT) AS `alarm25`,
       CAST(SUBSTRING(`alarmValue` FROM 26 FOR 1) AS TINYINT) AS `alarm26`,
       CAST(SUBSTRING(`alarmValue` FROM 27 FOR 1) AS TINYINT) AS `alarm27`,
       CAST(SUBSTRING(`alarmValue` FROM 28 FOR 1) AS TINYINT) AS `alarm28`,
       CAST(SUBSTRING(`alarmValue` FROM 29 FOR 1) AS TINYINT) AS `alarm29`,
       CAST(SUBSTRING(`alarmValue` FROM 30 FOR 1) AS TINYINT) AS `alarm30`,
       CAST(SUBSTRING(`alarmValue` FROM 31 FOR 1) AS TINYINT) AS `alarm31`,
       CAST(SUBSTRING(`alarmValue` FROM 32 FOR 1) AS TINYINT) AS `alarm32`,

       `deptName`                                             AS `dept_name`,
       `roomNo`                                               AS `room_no`,
       `bedNo`                                                AS `bed_no`,

       `alarmTs`                                              AS `ts`,
       DATE_FORMAT(`ts_ltz`, 'yyyy-MM-dd')                    as `dt`,
       CAST(HOUR (`ts_ltz`) AS STRING)                        as `hh`
FROM mkt_infusion_pump_equ_alarm_param_detail;