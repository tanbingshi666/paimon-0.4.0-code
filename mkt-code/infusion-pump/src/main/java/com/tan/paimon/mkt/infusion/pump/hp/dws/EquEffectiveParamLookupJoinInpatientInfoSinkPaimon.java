package com.tan.paimon.mkt.infusion.pump.hp.dws;

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

        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS `equ_mkt_infusion_pump`.`dws_mkt_infusion_pump_equ_effective_param_detail_with_inpatient_info` (\n" +
                "  `location_id` BIGINT,\n" +
                "  \n" +
                "  `factory_num` STRING,\n" +
                "  `equ_type` STRING,\n" +
                "  `equ_num` STRING,\n" +
                "\n" +
                "  `preset_value` FLOAT,\n" +
                "  `speed` FLOAT,\n" +
                "  `already_inject_time` INTEGER,\n" +
                "  `remain_time` INTEGER,\n" +
                "  `already_inject_value` FLOAT,\n" +
                "  `residual` FLOAT,\n" +
                "  `pressure_value` FLOAT,\n" +
                "\n" +
                "  `work_sta` STRING,\n" +
                "  `drug_name` STRING,\n" +
                "  `inject_mode` STRING,\n" +
                "\n" +
                "  `dept_name` STRING,\n" +
                "  `room_no` STRING,\n" +
                "  `bed_no` STRING,\n" +
                "\n" +
                "  `ts` BIGINT,\n" +
                "\n" +
                "  `dt` STRING,\n" +
                "  `hh` STRING,\n" +
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
                "   PRIMARY KEY (dt, hh, factory_num, ts) NOT ENFORCED\n" +
                ") PARTITIONED BY (dt, hh) WITH (\n" +
                "    'bucket' = '1',\n" +
                "    'bucket-key' = 'factory_num,ts',\n" +
                "    'merge-engine' = 'deduplicate'\n" +
                ");");

        tableEnv.executeSql("INSERT INTO `equ_mkt_infusion_pump`.`dws_mkt_infusion_pump_equ_effective_param_detail_with_inpatient_info` SELECT \n" +
                "\n" +
                "  temp_tbl_1.`location_id` AS `location_id`,\n" +
                "\n" +
                "  temp_tbl_1.`factory_num` AS `factory_num`,\n" +
                "  temp_tbl_1.`equ_type` AS `equ_type`,\n" +
                "  temp_tbl_1.`equ_num` AS `equ_num`,\n" +
                "\n" +
                "  temp_tbl_1.`preset_value` AS `preset_value`,\n" +
                "  temp_tbl_1.`speed` AS `speed`,\n" +
                "  temp_tbl_1.`already_inject_time` AS `already_inject_time`,\n" +
                "  temp_tbl_1.`remain_time` AS `remain_time`,\n" +
                "  temp_tbl_1.`already_inject_value` AS `already_inject_value`,\n" +
                "  temp_tbl_1.`residual` AS `residual`,\n" +
                "  temp_tbl_1.`pressure_value` AS `pressure_value`,\n" +
                "\n" +
                "  temp_tbl_1.`work_sta` AS `work_sta`,\n" +
                "  temp_tbl_1.`drug_name` AS `drug_name`,\n" +
                "  temp_tbl_1.`inject_mode` AS `inject_mode`,\n" +
                "\n" +
                "  temp_tbl_1.`dept_name` AS `dept_name`,\n" +
                "  temp_tbl_1.`room_no` AS `room_no`,\n" +
                "  temp_tbl_1.`bed_no` AS `bed_no`,\n" +
                "\n" +
                "  temp_tbl_1.`ts` AS `ts`,\n" +
                "\n" +
                "  temp_tbl_1.`dt` AS `dt`,\n" +
                "  temp_tbl_1.`hh` AS `hh`,\n" +
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
                "  \n" +
                "  `factory_num`,\n" +
                "  `equ_type`,\n" +
                "  `equ_num`,\n" +
                "\n" +
                "  `preset_value`,\n" +
                "  `speed`,\n" +
                "  `already_inject_time`,\n" +
                "  `remain_time`,\n" +
                "  `already_inject_value`,\n" +
                "  `residual`,\n" +
                "  `pressure_value`,\n" +
                "\n" +
                "  `work_sta`,\n" +
                "  `drug_name`,\n" +
                "  `inject_mode`,\n" +
                "\n" +
                "  `dept_name`,\n" +
                "  `room_no`,\n" +
                "  `bed_no`,\n" +
                "\n" +
                "  `ts`,\n" +
                "\n" +
                "  `dt`,\n" +
                "  `hh`,\n" +
                "  \n" +
                "  PROCTIME() AS proc_time\n" +
                "FROM `equ_mkt_infusion_pump`.`dwd_mkt_infusion_pump_equ_effective_param_detail`) AS temp_tbl_1\n" +
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
