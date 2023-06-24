package com.tan.paimon.mkt.infusion.pump.hp.dim;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

public class InpatientOrderRecordsSinkPaimon {

    public static void main(String[] args) {

        System.setProperty("HADOOP_USER_NAME", "tanbs");

        Configuration conf = new Configuration();
        // basic setting from https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/deployment/config/#basic-setup
        conf.setInteger("parallelism.default", 1);
        // checkpoint setting from https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/deployment/config/#checkpointing
        conf.setLong("execution.checkpointing.interval", 60 * 1000L);
        conf.setString("execution.checkpointing.mode", "EXACTLY_ONCE");
        conf.setLong("execution.checkpointing.timeout", 60 * 1000L);
        conf.setLong("execution.checkpointing.min-pause", 60 * 1000L);
        conf.setInteger("execution.checkpointing.max-concurrent-checkpoints", 1);
        conf.setInteger("execution.checkpointing.tolerable-failed-checkpoints", 3);
        conf.setString("execution.checkpointing.externalized-checkpoint-retention", "RETAIN_ON_CANCELLATION");
        // state backend setting from https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/deployment/config/#checkpoints-and-state-backends
        conf.setString("state.backend", "hashmap");
        conf.setString("state.checkpoint-storage", "filesystem");
        conf.setString("state.checkpoints.dir", "hdfs://hadoop102:8020/meiotds/chk/mkt/dwd");

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .withConfiguration(conf)
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        tableEnv.createTemporarySystemFunction("CAL_LOCATION", CalLocation.class);

        tableEnv.executeSql("CREATE CATALOG meiotds WITH (\n" +
                "  'type' = 'paimon',\n" +
                "  'metastore' = 'hive',\n" +
                "  'uri' = 'thrift://hadoop102:9083',\n" +
                "  'warehouse' = 'hdfs://hadoop102:8020/user/hive/warehouse/meiotds'\n" +
                ");");

        tableEnv.executeSql("USE CATALOG meiotds;");

        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS `default`.`ods_patient_inpatient_order_records`(\n" +
                "    -- 住院号\n" +
                "    `inpati_id` STRING,\n" +
                "    -- 本人姓名\n" +
                "    `p_name` STRING,\n" +
                "    -- 性别名称\n" +
                "    `sex_name` STRING,\n" +
                "    -- 出生日期\n" +
                "    `birthday` STRING,\n" +
                "    -- 医嘱号\n" +
                "    `advice_num` STRING,\n" +
                "    -- 医嘱核对者姓名\n" +
                "    `reviewer_name` STRING,\n" +
                "    -- 科室代码\n" +
                "    `depart_code` STRING,\n" +
                "    -- 科室名称\n" +
                "    `depart_name` STRING,\n" +
                "    -- 病房号\n" +
                "    `inpward_room` STRING,\n" +
                "    -- 病床号\n" +
                "    `inpward_bedno` STRING,\n" +
                "    -- 创建时间 yyyy-MM-dd HH:mm:ss\n" +
                "    `create_time` STRING,\n" +
                "    -- 更新时间 yyyy-MM-dd HH:mm:ss\n" +
                "    `update_time` STRING,\n" +
                "\n" +
                "    `ts` BIGINT,\n" +
                "    `dt` STRING,\n" +
                "    PRIMARY KEY (dt, inpati_id, ts) NOT ENFORCED\n" +
                ") PARTITIONED BY (dt) WITH (\n" +
                "    'bucket' = '1',\n" +
                "    'bucket-key' = 'inpati_id,ts',\n" +
                "    'write-mode' = 'change-log'\n" +
                ");");


        tableEnv.executeSql("INSERT INTO `default`.`ods_patient_inpatient_order_records` VALUES (\n" +
                "    'A-1001',\n" +
                "    'tan',\n" +
                "    'man',\n" +
                "    '1998-08-13',\n" +
                "    '666666',\n" +
                "    '张三',\n" +
                "    '手术室:ICU',\n" +
                "    '手术室:ICU',\n" +
                "    'ICU-601',\n" +
                "    '601-01',\n" +
                "    '2023-06-22 11-26-02',\n" +
                "    '2023-06-22 11-26-02',\n" +
                "    1687404362505,\n" +
                "    '2023-06-22'\n" +
                "), (\n" +
                "    'A-1002',\n" +
                "    'cheng',\n" +
                "    'sex',\n" +
                "    '1998-08-14',\n" +
                "    '777777',\n" +
                "    '李四',\n" +
                "    '手术室:ICU2',\n" +
                "    '手术室:ICU2',\n" +
                "    'ICU-602',\n" +
                "    '602-02',\n" +
                "    '2023-06-23 16-47-09',\n" +
                "    '2023-06-23 16-47-09',\n" +
                "    1687404281309,\n" +
                "    '2023-06-23'\n" +
                "), (\n" +
                "    'A-1001',\n" +
                "    'tan2',\n" +
                "    'man',\n" +
                "    '1998-08-13',\n" +
                "    '666666',\n" +
                "    '张三',\n" +
                "    '手术室:ICU',\n" +
                "    '手术室:ICU',\n" +
                "    'ICU-601',\n" +
                "    '601-01',\n" +
                "    '2023-06-22 11-26-03',\n" +
                "    '2023-06-22 11-26-03',\n" +
                "    1687404363505,\n" +
                "    '2023-06-22'\n" +
                ");");

        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS `default`.`dim_patient_inpatient_order_records` (\n" +
                "    -- 科室代码 + 病房号 + 病床号\n" +
                "    `location_id` BIGINT,\n" +
                "\n" +
                "    -- 住院号\n" +
                "    `inpati_id` STRING,\n" +
                "    -- 本人姓名\n" +
                "    `p_name` STRING,\n" +
                "    -- 性别名称\n" +
                "    `sex_name` STRING,\n" +
                "    -- 出生日期\n" +
                "    `birthday` STRING,\n" +
                "    -- 医嘱号\n" +
                "    `advice_num` STRING,\n" +
                "    -- 医嘱核对者姓名\n" +
                "    `reviewer_name` STRING,\n" +
                "    -- 科室代码\n" +
                "    `depart_code` STRING,\n" +
                "    -- 科室名称\n" +
                "    `depart_name` STRING,\n" +
                "    -- 病房号\n" +
                "    `inpward_room` STRING,\n" +
                "    -- 病床号\n" +
                "    `inpward_bedno` STRING,\n" +
                "    -- 创建时间 yyyy-MM-dd HH:mm:ss\n" +
                "    `create_time` STRING,\n" +
                "    -- 更新时间 yyyy-MM-dd HH:mm:ss\n" +
                "    `update_time` STRING,\n" +
                "\n" +
                "    PRIMARY KEY (location_id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "    'bucket' = '1',\n" +
                "    'bucket-key' = 'location_id',\n" +
                "    'merge-engine' = 'deduplicate'\n" +
                ");");

        /**
         tableEnv.sqlQuery("SELECT\n" +
         "\n" +
         "    CAL_LOCATION(`depart_code`, `inpward_room`, `inpward_bedno`) AS `location_id`,\n" +
         "\n" +
         "    -- 住院号\n" +
         "    `inpati_id`,\n" +
         "    -- 本人姓名\n" +
         "    `p_name`,\n" +
         "    -- 性别名称\n" +
         "    `sex_name`,\n" +
         "    -- 出生日期\n" +
         "    `birthday`,\n" +
         "    -- 医嘱号\n" +
         "    `advice_num`,\n" +
         "    -- 医嘱核对者姓名\n" +
         "    `reviewer_name`,\n" +
         "    -- 科室代码\n" +
         "    `depart_code`,\n" +
         "    -- 科室名称\n" +
         "    `depart_name`,\n" +
         "    -- 病房号\n" +
         "    `inpward_room`,\n" +
         "    -- 病床号\n" +
         "    `inpward_bedno`,\n" +
         "    -- 创建时间 yyyy-MM-dd HH:mm:ss\n" +
         "    `create_time`,\n" +
         "    -- 更新时间 yyyy-MM-dd HH:mm:ss\n" +
         "    `update_time`\n" +
         "\n" +
         "FROM `default`.`ods_patient_inpatient_order_records`;")
         .execute()
         .print();
         */

        tableEnv.executeSql("INSERT INTO `default`.`dim_patient_inpatient_order_records` SELECT\n" +
                "\n" +
                "    CAL_LOCATION(`depart_code`, `inpward_room`, `inpward_bedno`) AS `location_id`,\n" +
                "\n" +
                "    -- 住院号\n" +
                "    `inpati_id`,\n" +
                "    -- 本人姓名\n" +
                "    `p_name`,\n" +
                "    -- 性别名称\n" +
                "    `sex_name`,\n" +
                "    -- 出生日期\n" +
                "    `birthday`,\n" +
                "    -- 医嘱号\n" +
                "    `advice_num`,\n" +
                "    -- 医嘱核对者姓名\n" +
                "    `reviewer_name`,\n" +
                "    -- 科室代码\n" +
                "    `depart_code`,\n" +
                "    -- 科室名称\n" +
                "    `depart_name`,\n" +
                "    -- 病房号\n" +
                "    `inpward_room`,\n" +
                "    -- 病床号\n" +
                "    `inpward_bedno`,\n" +
                "    -- 创建时间 yyyy-MM-dd HH:mm:ss\n" +
                "    `create_time`,\n" +
                "    -- 更新时间 yyyy-MM-dd HH:mm:ss\n" +
                "    `update_time`\n" +
                "\n" +
                "FROM `default`.`ods_patient_inpatient_order_records`;");

    }

    public static class CalLocation extends ScalarFunction {

        public Integer eval(String var1, String var2, String var3) {
            return hash(var1.trim() + var2.trim() + var3.trim());
        }

        public Integer hash(String var) {
            int h;
            return ((h = var.hashCode()) ^ (h >>> 16)) & Integer.MAX_VALUE;
        }

    }

}
