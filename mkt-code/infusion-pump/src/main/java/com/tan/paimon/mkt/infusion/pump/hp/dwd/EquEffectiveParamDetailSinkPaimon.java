package com.tan.paimon.mkt.infusion.pump.hp.dwd;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class EquEffectiveParamDetailSinkPaimon {

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

        tableEnv.executeSql("CREATE CATALOG meiotds WITH (\n" +
                "  'type' = 'paimon',\n" +
                "  'metastore' = 'hive',\n" +
                "  'uri' = 'thrift://hadoop102:9083',\n" +
                "  'warehouse' = 'hdfs://hadoop102:8020/user/hive/warehouse/meiotds'\n" +
                ");");

        tableEnv.executeSql("USE CATALOG meiotds;");

        tableEnv.executeSql("CREATE TEMPORARY TABLE IF NOT EXISTS `mkt_infusion_pump_equ_effective_param_detail` (\n" +
                "  `factoryNum` STRING,\n" +
                "  `equType` STRING,\n" +
                "  `equNum` STRING,\n" +
                "\n" +
                "  `presetValue` FLOAT,\n" +
                "  `speed` FLOAT,\n" +
                "  `alreadyInjectTime` INTEGER,\n" +
                "  `remainTime` INTEGER,\n" +
                "  `alreadyInjectValue` FLOAT,\n" +
                "  `residual` FLOAT,\n" +
                "  `pressureValue` FLOAT,\n" +
                "\n" +
                "  `workSta` STRING,\n" +
                "  `drugName` STRING,\n" +
                "  `injectMode` STRING,\n" +
                "\n" +
                "  `deptName` STRING,\n" +
                "  `roomNo` STRING,\n" +
                "  `bedNo` STRING,\n" +
                "\n" +
                "  `ts` BIGINT,\n" +
                "  `ts_ltz` AS TO_TIMESTAMP_LTZ(ts, 3),\n" +
                "  WATERMARK FOR ts_ltz AS ts_ltz\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'MKT_INFUSION_PUMP_EQU_EFFECTIVE_PARAM_DETAIL',\n" +
                "  'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092',\n" +
                "  'properties.group.id' = 'MKT_INFUSION_PUMP_EQU_EFFECTIVE_PARAM_DETAIL_06_22',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json'\n" +
                ");");

        /**
         * tableEnv.sqlQuery("select * from mkt_infusion_pump_equ_effective_param_detail")
         *                 .execute()
         *                 .print();
         */
        // tableEnv.executeSql("DROP TABLE `equ_mkt_infusion_pump`.`dwd_mkt_infusion_pump_equ_effective_param_detail`");
        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS `equ_mkt_infusion_pump`.`dwd_mkt_infusion_pump_equ_effective_param_detail` (\n" +
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
                "   PRIMARY KEY (dt, hh, factory_num, ts) NOT ENFORCED\n" +
                ") PARTITIONED BY (dt, hh) WITH (\n" +
                "    'bucket' = '1',\n" +
                "    'bucket-key' = 'factory_num,ts',\n" +
                "    'write-mode' = 'change-log'\n" +
                ");");

        /**
         * tableEnv.sqlQuery("SELECT\n" +
         *                         "  `factoryNum` AS `factory_num`,\n" +
         *                         "  `equType` AS `equ_Type`,\n" +
         *                         "  `equNum` AS `equ_num`,\n" +
         *                         "\n" +
         *                         "  `presetValue` AS `preset_value`,\n" +
         *                         "  `speed`,\n" +
         *                         "  `alreadyInjectTime` AS `already_inject_time`,\n" +
         *                         "  `remainTime` AS `remain_time`,\n" +
         *                         "  `alreadyInjectValue` AS `already_inject_value`,\n" +
         *                         "  `residual`,\n" +
         *                         "  `pressureValue` AS `pressure_value`,\n" +
         *                         "\n" +
         *                         "  `workSta` AS `work_sta`,\n" +
         *                         "  `drugName` AS `drug_name`,\n" +
         *                         "  `injectMode` AS `inject_mode`,\n" +
         *                         "\n" +
         *                         "  `deptName` AS `dept_name`,\n" +
         *                         "  `roomNo` AS `room_no`,\n" +
         *                         "  `bedNo` AS `bed_no`,\n" +
         *                         "\n" +
         *                         "  `ts`,\n" +
         *                         "   DATE_FORMAT(`ts_ltz`,'yyyy-MM-dd') as dt,\n" +
         *                         "   CAST(HOUR(`ts_ltz`) AS STRING) as hh\n" +
         *                         "FROM mkt_infusion_pump_equ_effective_param_detail;")
         *                 .execute()
         *                 .print();
         */

        tableEnv.executeSql("INSERT INTO `equ_mkt_infusion_pump`.`dwd_mkt_infusion_pump_equ_effective_param_detail` SELECT\n" +
                "  `factoryNum` AS `factory_num`,\n" +
                "  `equType` AS `equ_type`,\n" +
                "  `equNum` AS `equ_num`,\n" +
                "\n" +
                "  `presetValue` AS `preset_value`,\n" +
                "  `speed`,\n" +
                "  `alreadyInjectTime` AS `already_inject_time`,\n" +
                "  `remainTime` AS `remain_time`,\n" +
                "  `alreadyInjectValue` AS `already_inject_value`,\n" +
                "  `residual`,\n" +
                "  `pressureValue` AS `pressure_value`,\n" +
                "\n" +
                "  `workSta` AS `work_sta`,\n" +
                "  `drugName` AS `drug_name`,\n" +
                "  `injectMode` AS `inject_mode`,\n" +
                "\n" +
                "  `deptName` AS `dept_name`,\n" +
                "  `roomNo` AS `room_no`,\n" +
                "  `bedNo` AS `bed_no`,\n" +
                "\n" +
                "  `ts`,\n" +
                "   DATE_FORMAT(`ts_ltz`,'yyyy-MM-dd') as `dt`,\n" +
                "   CAST(HOUR(`ts_ltz`) AS STRING) as `hh`\n" +
                "FROM mkt_infusion_pump_equ_effective_param_detail;");

    }

}
