CREATE TABLE IF NOT EXISTS `default`.`ods_patient_inpatient_order_records`
(
    -- 住院号
    `inpati_id`
    STRING,
    -- 本人姓名
    `p_name`
    STRING,
    -- 性别名称
    `sex_name`
    STRING,
    -- 出生日期
    `birthday`
    STRING,
    -- 医嘱号
    `advice_num`
    STRING,
    -- 医嘱核对者姓名
    `reviewer_name`
    STRING,
    -- 科室代码
    `depart_code`
    STRING,
    -- 科室名称
    `depart_name`
    STRING,
    -- 病房号
    `inpward_room`
    STRING,
    -- 病床号
    `inpward_bedno`
    STRING,
    -- 创建时间 yyyy-MM-dd HH:mm:ss
    `create_time`
    STRING,
    -- 更新时间 yyyy-MM-dd HH:mm:ss
    `update_time`
    STRING,

    `ts`
    BIGINT,
    `dt`
    STRING,
    PRIMARY
    KEY
(
    dt,
    inpati_id,
    ts
) NOT ENFORCED
    ) PARTITIONED BY
(
    dt
) WITH (
      'bucket' = '1',
      'bucket-key' = 'inpati_id,ts',
      'write-mode' = 'change-log'
      );


INSERT INTO `default`.`ods_patient_inpatient_order_records`
VALUES ('A-1001',
        'tan',
        'man',
        '1998-08-13',
        '666666',
        '张三',
        '手术室:ICU',
        '手术室:ICU',
        'ICU-601',
        '601-01',
        '2023-06-22 11-26-02',
        '2023-06-22 11-26-02',
        1687404362505,
        '2023-06-22'),
       ('A-1002',
        'cheng',
        'sex',
        '1998-08-14',
        '777777',
        '李四',
        '手术室:ICU2',
        '手术室:ICU2',
        'ICU-602',
        '602-02',
        '2023-06-23 16-47-09',
        '2023-06-23 16-47-09',
        1687404281309,
        '2023-06-23'),
       ('A-1001',
        'tan2',
        'man',
        '1998-08-13',
        '666666',
        '张三',
        '手术室:ICU',
        '手术室:ICU',
        'ICU-601',
        '601-01',
        '2023-06-22 11-26-03',
        '2023-06-22 11-26-03',
        1687404363505,
        '2023-06-22');


CREATE TABLE IF NOT EXISTS `default`.`dim_patient_inpatient_order_records`
(
    -- 科室代码 + 病房号 + 病床号
    `location_id`
    BIGINT,

    -- 住院号
    `inpati_id`
    STRING,
    -- 本人姓名
    `p_name`
    STRING,
    -- 性别名称
    `sex_name`
    STRING,
    -- 出生日期
    `birthday`
    STRING,
    -- 医嘱号
    `advice_num`
    STRING,
    -- 医嘱核对者姓名
    `reviewer_name`
    STRING,
    -- 科室代码
    `depart_code`
    STRING,
    -- 科室名称
    `depart_name`
    STRING,
    -- 病房号
    `inpward_room`
    STRING,
    -- 病床号
    `inpward_bedno`
    STRING,
    -- 创建时间 yyyy-MM-dd HH:mm:ss
    `create_time`
    STRING,
    -- 更新时间 yyyy-MM-dd HH:mm:ss
    `update_time`
    STRING,

    PRIMARY
    KEY
(
    location_id
) NOT ENFORCED
    ) WITH (
          'bucket' = '1',
          'bucket-key' = 'location_id',
          'merge-engine' = 'deduplicate'
          );


INSERT INTO `default`.`dim_patient_inpatient_order_records`
SELECT CAL_LOCATION(`depart_code`, `inpward_room`, `inpward_bedno`) AS `location_id`,

       -- 住院号
       `inpati_id`,
       -- 本人姓名
       `p_name`,
       -- 性别名称
       `sex_name`,
       -- 出生日期
       `birthday`,
       -- 医嘱号
       `advice_num`,
       -- 医嘱核对者姓名
       `reviewer_name`,
       -- 科室代码
       `depart_code`,
       -- 科室名称
       `depart_name`,
       -- 病房号
       `inpward_room`,
       -- 病床号
       `inpward_bedno`,
       -- 创建时间 yyyy-MM-dd HH:mm:ss
       `create_time`,
       -- 更新时间 yyyy-MM-dd HH:mm:ss
       `update_time`

FROM `default`.`ods_patient_inpatient_order_records`;