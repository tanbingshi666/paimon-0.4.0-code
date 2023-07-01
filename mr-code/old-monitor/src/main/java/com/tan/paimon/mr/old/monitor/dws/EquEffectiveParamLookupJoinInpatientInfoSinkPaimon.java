package com.tan.paimon.mr.old.monitor.dws;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

public class EquEffectiveParamLookupJoinInpatientInfoSinkPaimon {

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
        conf.setString("state.checkpoints.dir", "hdfs://hadoop102:8020/meiotds/chk/mkt/dws");

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

        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS `default`.`dws_mr_old_monitor_equ_effective_param_detail_with_inpatient_info` (\n" +
                "  `location_id` BIGINT,\n" +
                "  \n" +
                "  `start_date` STRING,\n" +
                "  `equ_id` STRING,\n" +
                "  `dept_name` STRING,\n" +
                "  `room_no` STRING,\n" +
                "  `bed_no` STRING,\n" +
                "  `hr` STRING,\n" +
                "  `pvcs` STRING,\n" +
                "  `rr` STRING,\n" +
                "  `spo2` STRING,\n" +
                "  `pr` STRING,\n" +
                "\n" +
                "  `ts` BIGINT,\n" +
                "\n" +
                "  `dt` STRING,\n" +
                "\n" +
                "  `proc_time` STRING,\n" +
                "\n" +
                "  `inpati_id` STRING,\n" +
                "  `p_name` STRING,\n" +
                "  `sex_name` STRING,\n" +
                "  `birthday` STRING,\n" +
                "  `advice_num` STRING,\n" +
                "  `reviewer_name` STRING,\n" +
                "  `depart_code` STRING,\n" +
                "  `depart_name` STRING,\n" +
                "  `inpward_room` STRING,\n" +
                "  `inpward_bedno` STRING,\n" +
                "  `create_time` STRING,\n" +
                "  `update_time` STRING,\n" +
                "\n" +
                "   PRIMARY KEY (dt, equ_id, ts) NOT ENFORCED\n" +
                ") PARTITIONED BY (dt) WITH (\n" +
                "    'bucket' = '1',\n" +
                "    'bucket-key' = 'equ_id,ts',\n" +
                "    'merge-engine' = 'deduplicate'\n" +
                ");");

//        tableEnv.sqlQuery("SELECT \n" +
//                        "\n" +
//                        "  temp_tbl_1.`location_id` AS `location_id`,\n" +
//                        "\n" +
//                        "  temp_tbl_1.`equ_id` AS `equ_id`,\n" +
//                        "  temp_tbl_1.`dept_name` AS `dept_name`,\n" +
//                        "  temp_tbl_1.`room_no` AS `room_no`,\n" +
//                        "  temp_tbl_1.`bed_no` AS `bed_no`,\n" +
//                        "\n" +
//                        "  temp_tbl_1.`hr` AS `hr`,\n" +
//                        "  temp_tbl_1.`pvcs` AS `pvcs`,\n" +
//                        "  temp_tbl_1.`rr` AS `rr`,\n" +
//                        "  temp_tbl_1.`spo2` AS `spo2`,\n" +
//                        "  temp_tbl_1.`pr` AS `pr`,\n" +
//                        "\n" +
//                        "  temp_tbl_1.`ts` AS `ts`,\n" +
//                        "\n" +
//                        "  temp_tbl_1.`dt` AS `dt`,\n" +
//                        "\n" +
//                        "  CAST(temp_tbl_1.`proc_time` AS STRING) AS `proc_time`,\n" +
//                        "\n" +
//                        "  temp_tbl_2.`inpati_id` AS `inpati_id`,\n" +
//                        "  temp_tbl_2.`p_name` AS `p_name`,\n" +
//                        "  temp_tbl_2.`sex_name` AS `sex_name`,\n" +
//                        "  temp_tbl_2.`birthday` AS `birthday`,\n" +
//                        "  temp_tbl_2.`advice_num` AS `advice_num`,\n" +
//                        "  temp_tbl_2.`reviewer_name` AS `reviewer_name`,\n" +
//                        "  temp_tbl_2.`depart_code` AS `depart_code`,\n" +
//                        "  temp_tbl_2.`depart_name` AS `depart_name`,\n" +
//                        "  temp_tbl_2.`inpward_room` AS `inpward_room`,\n" +
//                        "  temp_tbl_2.`inpward_bedno` AS `inpward_bedno`,\n" +
//                        "  temp_tbl_2.`create_time` AS `create_time`,\n" +
//                        "  temp_tbl_2.`create_time` AS `create_time`\n" +
//                        "\n" +
//                        "FROM\n" +
//                        "( SELECT\n" +
//                        "  CAL_LOCATION(`dept_name`, `room_no`, `bed_no`) AS `location_id`,\n" +
//                        "  \n" +
//                        "  `equ_id`,\n" +
//                        "  `dept_name`,\n" +
//                        "  `room_no`,\n" +
//                        "  `bed_no`,\n" +
//                        "  `hr`,\n" +
//                        "  `pvcs`,\n" +
//                        "  `rr`,\n" +
//                        "  `spo2`,\n" +
//                        "  `pr`,\n" +
//                        "\n" +
//                        "  `ts`,\n" +
//                        "  `dt`,\n" +
//                        "  \n" +
//                        "  PROCTIME() AS proc_time\n" +
//                        "FROM `default`.`dwd_mr_old_monitor_equ_effective_param_detail`) AS temp_tbl_1\n" +
//                        "JOIN `default`.`dim_patient_inpatient_order_records` /*+ OPTIONS('rocksdb.compression.type'='NO_COMPRESSION') */\n" +
//                        "FOR SYSTEM_TIME AS OF temp_tbl_1.proc_time AS temp_tbl_2\n" +
//                        "ON temp_tbl_1.location_id = temp_tbl_2.location_id;")
//                .execute()
//                .print();

        tableEnv.executeSql("INSERT INTO `default`.`dws_mr_old_monitor_equ_effective_param_detail_with_inpatient_info` SELECT \n" +
                "\n" +
                "  temp_tbl_1.`location_id` AS `location_id`,\n" +
                "\n" +
                "  temp_tbl_1.`start_date` AS `start_date`,\n" +
                "  temp_tbl_1.`equ_id` AS `equ_id`,\n" +
                "  temp_tbl_1.`dept_name` AS `dept_name`,\n" +
                "  temp_tbl_1.`room_no` AS `room_no`,\n" +
                "  temp_tbl_1.`bed_no` AS `bed_no`,\n" +
                "\n" +
                "  temp_tbl_1.`hr` AS `hr`,\n" +
                "  temp_tbl_1.`pvcs` AS `pvcs`,\n" +
                "  temp_tbl_1.`rr` AS `rr`,\n" +
                "  temp_tbl_1.`spo2` AS `spo2`,\n" +
                "  temp_tbl_1.`pr` AS `pr`,\n" +
                "\n" +
                "  temp_tbl_1.`ts` AS `ts`,\n" +
                "\n" +
                "  temp_tbl_1.`dt` AS `dt`,\n" +
                "\n" +
                "  CAST(temp_tbl_1.`proc_time` AS STRING) AS `proc_time`,\n" +
                "\n" +
                "  temp_tbl_2.`inpati_id` AS `inpati_id`,\n" +
                "  temp_tbl_2.`p_name` AS `p_name`,\n" +
                "  temp_tbl_2.`sex_name` AS `sex_name`,\n" +
                "  temp_tbl_2.`birthday` AS `birthday`,\n" +
                "  temp_tbl_2.`advice_num` AS `advice_num`,\n" +
                "  temp_tbl_2.`reviewer_name` AS `reviewer_name`,\n" +
                "  temp_tbl_2.`depart_code` AS `depart_code`,\n" +
                "  temp_tbl_2.`depart_name` AS `depart_name`,\n" +
                "  temp_tbl_2.`inpward_room` AS `inpward_room`,\n" +
                "  temp_tbl_2.`inpward_bedno` AS `inpward_bedno`,\n" +
                "  temp_tbl_2.`create_time` AS `create_time`,\n" +
                "  temp_tbl_2.`create_time` AS `create_time`\n" +
                "\n" +
                "FROM\n" +
                "( SELECT\n" +
                "  CAL_LOCATION(`dept_name`, `room_no`, `bed_no`) AS `location_id`,\n" +
                "  `start_date`,\n" +
                "  `equ_id`,\n" +
                "  `dept_name`,\n" +
                "  `room_no`,\n" +
                "  `bed_no`,\n" +
                "  `hr`,\n" +
                "  `pvcs`,\n" +
                "  `rr`,\n" +
                "  `spo2`,\n" +
                "  `pr`,\n" +
                "\n" +
                "  `ts`,\n" +
                "  `dt`,\n" +
                "  \n" +
                "  PROCTIME() AS proc_time\n" +
                "FROM `default`.`dwd_mr_old_monitor_equ_effective_param_detail`) AS temp_tbl_1\n" +
                "JOIN `default`.`dim_patient_inpatient_order_records` /*+ OPTIONS('rocksdb.compression.type'='NO_COMPRESSION') */\n" +
                "FOR SYSTEM_TIME AS OF temp_tbl_1.proc_time AS temp_tbl_2\n" +
                "ON temp_tbl_1.location_id = temp_tbl_2.location_id;");

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
