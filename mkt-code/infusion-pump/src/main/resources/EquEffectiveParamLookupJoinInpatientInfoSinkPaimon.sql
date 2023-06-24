CREATE TABLE IF NOT EXISTS `equ_mkt_infusion_pump`.`dws_mkt_infusion_pump_equ_effective_param_detail_with_inpatient_info`
(
    `location_id`
    BIGINT,

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

    `proc_time`
    STRING,

    `inpati_id`
    STRING,
    `p_name`
    STRING,
    `sex_name`
    STRING,
    `birthday`
    STRING,
    `advice_num`
    STRING,
    `reviewer_name`
    STRING,
    `depart_code`
    STRING,
    `depart_name`
    STRING,
    `inpward_room`
    STRING,
    `inpward_bedno`
    STRING,
    `create_time`
    STRING,
    `update_time`
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
      'merge-engine' = 'deduplicate'
      );


INSERT INTO `equ_mkt_infusion_pump`.`dws_mkt_infusion_pump_equ_effective_param_detail_with_inpatient_info`
SELECT temp_tbl_1.`location_id`               AS `location_id`,

       temp_tbl_1.`factory_num`               AS `factory_num`,
       temp_tbl_1.`equ_type`                  AS `equ_type`,
       temp_tbl_1.`equ_num`                   AS `equ_num`,

       temp_tbl_1.`preset_value`              AS `preset_value`,
       temp_tbl_1.`speed`                     AS `speed`,
       temp_tbl_1.`already_inject_time`       AS `already_inject_time`,
       temp_tbl_1.`remain_time`               AS `remain_time`,
       temp_tbl_1.`already_inject_value`      AS `already_inject_value`,
       temp_tbl_1.`residual`                  AS `residual`,
       temp_tbl_1.`pressure_value`            AS `pressure_value`,

       temp_tbl_1.`work_sta`                  AS `work_sta`,
       temp_tbl_1.`drug_name`                 AS `drug_name`,
       temp_tbl_1.`inject_mode`               AS `inject_mode`,

       temp_tbl_1.`dept_name`                 AS `dept_name`,
       temp_tbl_1.`room_no`                   AS `room_no`,
       temp_tbl_1.`bed_no`                    AS `bed_no`,

       temp_tbl_1.`ts`                        AS `ts`,

       temp_tbl_1.`dt`                        AS `dt`,
       temp_tbl_1.`hh`                        AS `hh`,

       CAST(temp_tbl_1.`proc_time` AS STRING) AS `proc_time`,

       temp_tbl_2.`inpati_id`                 AS `inpati_id`,
       temp_tbl_2.`p_name`                    AS `p_name`,
       temp_tbl_2.`sex_name`                  AS `sex_name`,
       temp_tbl_2.`birthday`                  AS `birthday`,
       temp_tbl_2.`advice_num`                AS `advice_num`,
       temp_tbl_2.`reviewer_name`             AS `reviewer_name`,
       temp_tbl_2.`depart_code`               AS `depart_code`,
       temp_tbl_2.`depart_name`               AS `depart_name`,
       temp_tbl_2.`inpward_room`              AS `inpward_room`,
       temp_tbl_2.`inpward_bedno`             AS `inpward_bedno`,
       temp_tbl_2.`create_time`               AS `create_time`,
       temp_tbl_2.`create_time`               AS `create_time`

FROM (SELECT CAL_LOCATION(`dept_name`, `room_no`, `bed_no`) AS `location_id`,

             `factory_num`,
             `equ_type`,
             `equ_num`,

             `preset_value`,
             `speed`,
             `already_inject_time`,
             `remain_time`,
             `already_inject_value`,
             `residual`,
             `pressure_value`,

             `work_sta`,
             `drug_name`,
             `inject_mode`,

             `dept_name`,
             `room_no`,
             `bed_no`,

             `ts`,

             `dt`,
             `hh`,

             PROCTIME()                                     AS proc_time
      FROM `equ_mkt_infusion_pump`.`dwd_mkt_infusion_pump_equ_effective_param_detail`) AS temp_tbl_1
         JOIN `default`.`dim_patient_inpatient_order_records` /*+ OPTIONS('rocksdb.compression.type'='NO_COMPRESSION') */
    FOR SYSTEM_TIME AS OF temp_tbl_1.proc_time AS temp_tbl_2
              ON temp_tbl_1.location_id = temp_tbl_2.location_id;