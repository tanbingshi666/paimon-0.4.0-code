package com.tan.paimon.mkt.infusion.pump.hp.dwd;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class EquAlarmParamDetailSinkPaimon {

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

        tableEnv.executeSql("CREATE TEMPORARY TABLE IF NOT EXISTS `mkt_infusion_pump_equ_alarm_param_detail` (\n" +
                "  `factoryNum` STRING,\n" +
                "  `injectMode` STRING,\n" +
                "\n" +
                "  `alarmType` STRING,\n" +
                "  `alarmValue` STRING,\n" +
                "\n" +
                "  `deptName` STRING,\n" +
                "  `roomNo` STRING,\n" +
                "  `bedNo` STRING,\n" +
                "\n" +
                "  `alarmTs` BIGINT,\n" +
                "  `ts_ltz` AS TO_TIMESTAMP_LTZ(alarmTs, 3),\n" +
                "  WATERMARK FOR ts_ltz AS ts_ltz\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'MKT_INFUSION_PUMP_EQU_ALARM_PARAM_DETAIL',\n" +
                "  'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092',\n" +
                "  'properties.group.id' = 'MKT_INFUSION_PUMP_EQU_ALARM_PARAM_DETAIL_06_22',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json'\n" +
                ");");

        /**
         * tableEnv.sqlQuery("select * from mkt_infusion_pump_equ_alarm_param_detail;")
         *                 .execute()
         *                 .print();
         */
        tableEnv.executeSql("DROP TABLE `equ_mkt_infusion_pump`.`dwd_mkt_infusion_pump_equ_alarm_param_detail`");
        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS `equ_mkt_infusion_pump`.`dwd_mkt_infusion_pump_equ_alarm_param_detail` (\n" +
                "  `factory_num` STRING,\n" +
                "  `inject_mode` STRING,\n" +
                "\n" +
                "  `alarm_type` STRING,\n" +
                "\n" +
                "  `alarm1` TINYINT,\n" +
                "  `alarm2` TINYINT,\n" +
                "  `alarm3` TINYINT,\n" +
                "  `alarm4` TINYINT,\n" +
                "  `alarm5` TINYINT,\n" +
                "  `alarm6` TINYINT,\n" +
                "  `alarm7` TINYINT,\n" +
                "  `alarm8` TINYINT,\n" +
                "\n" +
                "  `alarm9` TINYINT,\n" +
                "  `alarm10` TINYINT,\n" +
                "  `alarm11` TINYINT,\n" +
                "  `alarm12` TINYINT,\n" +
                "  `alarm13` TINYINT,\n" +
                "  `alarm14` TINYINT,\n" +
                "  `alarm15` TINYINT,\n" +
                "  `alarm16` TINYINT,\n" +
                "\n" +
                "  `alarm17` TINYINT,\n" +
                "  `alarm18` TINYINT,\n" +
                "  `alarm19` TINYINT,\n" +
                "  `alarm20` TINYINT,\n" +
                "  `alarm21` TINYINT,\n" +
                "  `alarm22` TINYINT,\n" +
                "  `alarm23` TINYINT,\n" +
                "  `alarm24` TINYINT,\n" +
                "\n" +
                "  `alarm25` TINYINT,\n" +
                "  `alarm26` TINYINT,\n" +
                "  `alarm27` TINYINT,\n" +
                "  `alarm28` TINYINT,\n" +
                "  `alarm29` TINYINT,\n" +
                "  `alarm30` TINYINT,\n" +
                "  `alarm31` TINYINT,\n" +
                "  `alarm32` TINYINT,\n" +
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
                "   PRIMARY KEY (dt, hh, factory_num, alarm_type, ts) NOT ENFORCED\n" +
                ") PARTITIONED BY (dt, hh) WITH (\n" +
                "    'bucket' = '1',\n" +
                "    'bucket-key' = 'factory_num,alarm_type,ts',\n" +
                "    'write-mode' = 'change-log'\n" +
                ");");

        tableEnv.executeSql("INSERT INTO `equ_mkt_infusion_pump`.`dwd_mkt_infusion_pump_equ_alarm_param_detail` SELECT\n" +
                "\n" +
                "  `factoryNum` AS `factory_num`,\n" +
                "  `injectMode` AS `inject_mode`,\n" +
                "\n" +
                "  `alarmType` AS `alarm_type`,\n" +
                "\n" +
                "  CAST(SUBSTRING(`alarmValue` FROM 1 FOR 1) AS TINYINT) AS `alarm1`,\n" +
                "  CAST(SUBSTRING(`alarmValue` FROM 2 FOR 1) AS TINYINT) AS `alarm2`,\n" +
                "  CAST(SUBSTRING(`alarmValue` FROM 3 FOR 1) AS TINYINT) AS `alarm3`,\n" +
                "  CAST(SUBSTRING(`alarmValue` FROM 4 FOR 1) AS TINYINT) AS `alarm4`,\n" +
                "  CAST(SUBSTRING(`alarmValue` FROM 5 FOR 1) AS TINYINT) AS `alarm5`,\n" +
                "  CAST(SUBSTRING(`alarmValue` FROM 6 FOR 1) AS TINYINT) AS `alarm6`,\n" +
                "  CAST(SUBSTRING(`alarmValue` FROM 7 FOR 1) AS TINYINT) AS `alarm7`,\n" +
                "  CAST(SUBSTRING(`alarmValue` FROM 8 FOR 1) AS TINYINT) AS `alarm8`,\n" +
                "\n" +
                "  CAST(SUBSTRING(`alarmValue` FROM 9 FOR 1) AS TINYINT) AS `alarm9`,\n" +
                "  CAST(SUBSTRING(`alarmValue` FROM 10 FOR 1) AS TINYINT) AS `alarm10`,\n" +
                "  CAST(SUBSTRING(`alarmValue` FROM 11 FOR 1) AS TINYINT) AS `alarm11`,\n" +
                "  CAST(SUBSTRING(`alarmValue` FROM 12 FOR 1) AS TINYINT) AS `alarm12`,\n" +
                "  CAST(SUBSTRING(`alarmValue` FROM 13 FOR 1) AS TINYINT) AS `alarm13`,\n" +
                "  CAST(SUBSTRING(`alarmValue` FROM 14 FOR 1) AS TINYINT) AS `alarm14`,\n" +
                "  CAST(SUBSTRING(`alarmValue` FROM 15 FOR 1) AS TINYINT) AS `alarm15`,\n" +
                "  CAST(SUBSTRING(`alarmValue` FROM 16 FOR 1) AS TINYINT) AS `alarm16`,\n" +
                "\n" +
                "  CAST(SUBSTRING(`alarmValue` FROM 17 FOR 1) AS TINYINT) AS `alarm17`,\n" +
                "  CAST(SUBSTRING(`alarmValue` FROM 18 FOR 1) AS TINYINT) AS `alarm18`,\n" +
                "  CAST(SUBSTRING(`alarmValue` FROM 19 FOR 1) AS TINYINT) AS `alarm19`,\n" +
                "  CAST(SUBSTRING(`alarmValue` FROM 20 FOR 1) AS TINYINT) AS `alarm20`,\n" +
                "  CAST(SUBSTRING(`alarmValue` FROM 21 FOR 1) AS TINYINT) AS `alarm21`,\n" +
                "  CAST(SUBSTRING(`alarmValue` FROM 22 FOR 1) AS TINYINT) AS `alarm22`,\n" +
                "  CAST(SUBSTRING(`alarmValue` FROM 23 FOR 1) AS TINYINT) AS `alarm23`,\n" +
                "  CAST(SUBSTRING(`alarmValue` FROM 24 FOR 1) AS TINYINT) AS `alarm24`,\n" +
                "\n" +
                "  CAST(SUBSTRING(`alarmValue` FROM 25 FOR 1) AS TINYINT) AS `alarm25`,\n" +
                "  CAST(SUBSTRING(`alarmValue` FROM 26 FOR 1) AS TINYINT) AS `alarm26`,\n" +
                "  CAST(SUBSTRING(`alarmValue` FROM 27 FOR 1) AS TINYINT) AS `alarm27`,\n" +
                "  CAST(SUBSTRING(`alarmValue` FROM 28 FOR 1) AS TINYINT) AS `alarm28`,\n" +
                "  CAST(SUBSTRING(`alarmValue` FROM 29 FOR 1) AS TINYINT) AS `alarm29`,\n" +
                "  CAST(SUBSTRING(`alarmValue` FROM 30 FOR 1) AS TINYINT) AS `alarm30`,\n" +
                "  CAST(SUBSTRING(`alarmValue` FROM 31 FOR 1) AS TINYINT) AS `alarm31`,\n" +
                "  CAST(SUBSTRING(`alarmValue` FROM 32 FOR 1) AS TINYINT) AS `alarm32`,  \n" +
                "\n" +
                "  `deptName` AS `dept_name`,\n" +
                "  `roomNo` AS `room_no`,\n" +
                "  `bedNo` AS `bed_no`,\n" +
                "\n" +
                "  `alarmTs` AS `ts`,\n" +
                "   DATE_FORMAT(`ts_ltz`,'yyyy-MM-dd') as `dt`,\n" +
                "   CAST(HOUR(`ts_ltz`) AS STRING) as `hh`\n" +
                "FROM mkt_infusion_pump_equ_alarm_param_detail;");

    }

}
