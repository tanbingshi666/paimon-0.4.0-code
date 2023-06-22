package com.tan.paimon.mkt.infusion.pump.hp.ods;

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * 麦科田 hp 系列输注泵解析 json 数据入湖
 */
public class OriginDataSinkPaimon {

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
        conf.setString("state.checkpoints.dir", "hdfs://hadoop102:8020/meiotds/chk/mkt/ods");

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .withConfiguration(conf)
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        // create hive catalog
        /**
         * CREATE CATALOG meiotds WITH (
         *   'type' = 'paimon',
         *   'metastore' = 'hive',
         *   'uri' = 'thrift://hadoop102:9083',
         *   'warehouse' = 'hdfs://hadoop102:8020/user/hive/warehouse/meiotds'
         * );
         */
        tableEnv.executeSql("CREATE CATALOG meiotds WITH (\n" +
                "  'type' = 'paimon',\n" +
                "  'metastore' = 'hive',\n" +
                "  'uri' = 'thrift://hadoop102:9083',\n" +
                "  'warehouse' = 'hdfs://hadoop102:8020/user/hive/warehouse/meiotds'\n" +
                ");");
        /**
         * USE CATALOG meiotds;
         */
        tableEnv.executeSql("USE CATALOG meiotds;");
        /**
         * CREATE DATABASE IF NOT EXISTS equ_mkt_infusion_pump;
         */
        tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS equ_mkt_infusion_pump;");
        /**
         * USE equ_mkt_infusion_pump;
         */
        tableEnv.executeSql("USE equ_mkt_infusion_pump;");

        // create kafka source TEMPORARY table
        // TEMPORARY TABLE from https://paimon.apache.org/docs/0.4/how-to/creating-tables/#creating-temporary-tables
        /**
         * CREATE TEMPORARY TABLE IF NOT EXISTS kafka_source_hp_series (
         *   `host` STRING,
         *   `port` BIGINT,
         *   `code` BIGINT,
         *   `header` MAP<STRING, STRING>,
         *   `data` MAP<STRING, STRING>,
         *   `ts` BIGINT,
         *   `ts_ltz` AS TO_TIMESTAMP_LTZ(ts, 3)
         * ) WITH (
         *   'connector' = 'kafka',
         *   'topic' = 'MKT_INFUSION_PUMP_HP_SERIES_JSON',
         *   'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092',
         *   'properties.group.id' = 'MKT_INFUSION_PUMP_HP_SERIES_JSON_06_22',
         *   'scan.startup.mode' = 'earliest-offset',
         *   'format' = 'json'
         * );
         */
        tableEnv.executeSql("CREATE TEMPORARY TABLE IF NOT EXISTS kafka_source_hp_series (\n" +
                "  `host` STRING,\n" +
                "  `port` BIGINT,\n" +
                "  `code` BIGINT,\n" +
                "  `header` MAP<STRING, STRING>,\n" +
                "  `data` MAP<STRING, STRING>,\n" +
                "  `ts` BIGINT,\n" +
                "  `ts_ltz` AS TO_TIMESTAMP_LTZ(ts, 3)\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'MKT_INFUSION_PUMP_HP_SERIES_JSON',\n" +
                "  'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092',\n" +
                "  'properties.group.id' = 'MKT_INFUSION_PUMP_HP_SERIES_JSON_06_22',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json'\n" +
                ")");

        // read kafka for test
        /**
         * tableEnv.sqlQuery("select * from kafka_source_hp_series")
         *                 .execute()
         *                 .print();
         */
        /**
         * tableEnv.sqlQuery("select `data`['factoryNum'] as factory_num from kafka_source_hp_series")
         *                 .execute()
         *                 .print();
         */
        /**
         * tableEnv.sqlQuery("SELECT \n" +
         *                         "\n" +
         *                         "    `data`['factoryNum'] as `factory_num`,\n" +
         *                         "    `data`['equType'] as `equ_type`,\n" +
         *                         "    `data`['equNum'] as `equ_num`,\n" +
         *                         "    `data`['workSta'] as `work_sta`,\n" +
         *                         "    `data`['deptName'] as `dept_name`,\n" +
         *                         "    `data`['roomNo'] as `room_no`,\n" +
         *                         "    `data`['bedNo'] as `bed_no`,\n" +
         *                         "    `data`['state'] as `state`,\n" +
         *                         "    `data`['drugName'] as `drug_name`,\n" +
         *                         "    `data`['injectMode'] as `inject_mode`,\n" +
         *                         "    `data`['presetValue'] as `preset_value`,\n" +
         *                         "    `data`['speed'] as `speed`,\n" +
         *                         "    `data`['alreadyInjectTime'] as `already_inject_time`,\n" +
         *                         "    `data`['remainTime'] as `remain_time`,\n" +
         *                         "    `data`['alreadyInjectValue'] as `already_inject_value`,\n" +
         *                         "    `data`['residual'] as `residual`,\n" +
         *                         "    `data`['alarm1'] as `alarm1`,\n" +
         *                         "    `data`['alarm2'] as `alarm2`,\n" +
         *                         "    `data`['alarm3'] as `alarm3`,\n" +
         *                         "    `data`['alarm4'] as `alarm4`,\n" +
         *                         "    `data`['pressureValue'] as `pressure_value`,\n" +
         *                         "    `data`['pressureUint'] as `pressure_uint`,\n" +
         *                         "\n" +
         *                         "    `host`,\n" +
         *                         "    `port`,\n" +
         *                         "    `code`,\n" +
         *                         "    `header`,\n" +
         *                         "    `ts`,\n" +
         *                         "\n" +
         *                         "    DATE_FORMAT(`ts_ltz`,'yyyy-MM-dd') as dt,\n" +
         *                         "    HOUR(`ts_ltz`) as hh\n" +
         *                         "\n" +
         *                         "FROM kafka_source_hp_series;")
         *                 .execute()
         *                 .print();
         */
        /**
         * tableEnv.sqlQuery("SELECT \n" +
         *                         "\n" +
         *                         "    `data`['factoryNum'] as `factory_num`,\n" +
         *                         "    `data`['equType'] as `equ_type`,\n" +
         *                         "    `data`['equNum'] as `equ_num`,\n" +
         *                         "    `data`['workSta'] as `work_sta`,\n" +
         *                         "    `data`['deptName'] as `dept_name`,\n" +
         *                         "    `data`['roomNo'] as `room_no`,\n" +
         *                         "    `data`['bedNo'] as `bed_no`,\n" +
         *                         "    `data`['state'] as `state`,\n" +
         *                         "    `data`['drugName'] as `drug_name`,\n" +
         *                         "    `data`['injectMode'] as `inject_mode`,\n" +
         *                         "    CAST(`data`['presetValue'] AS FLOAT) as `preset_value`,\n" +
         *                         "    CAST(`data`['speed'] AS FLOAT) as `speed`,\n" +
         *                         "    CAST(`data`['alreadyInjectTime'] AS INTEGER) as `already_inject_time`,\n" +
         *                         "    CAST(`data`['remainTime'] AS INTEGER) as `remain_time`,\n" +
         *                         "    CAST(`data`['alreadyInjectValue'] AS FLOAT) as `already_inject_value`,\n" +
         *                         "    CAST(`data`['residual'] AS FLOAT) as `residual`,\n" +
         *                         "    `data`['alarm1'] as `alarm1`,\n" +
         *                         "    `data`['alarm2'] as `alarm2`,\n" +
         *                         "    `data`['alarm3'] as `alarm3`,\n" +
         *                         "    `data`['alarm4'] as `alarm4`,\n" +
         *                         "    CAST(`data`['pressureValue'] AS  FLOAT) as `pressure_value`,\n" +
         *                         "    `data`['pressureUint'] as `pressure_uint`,\n" +
         *                         "\n" +
         *                         "    `host`,\n" +
         *                         "    `port`,\n" +
         *                         "    `code`,\n" +
         *                         "    `header`,\n" +
         *                         "    `ts`,\n" +
         *                         "\n" +
         *                         "    DATE_FORMAT(`ts_ltz`,'yyyy-MM-dd') as dt,\n" +
         *                         "    HOUR(`ts_ltz`) as hh\n" +
         *                         "\n" +
         *                         "FROM kafka_source_hp_series;")
         *                 .execute()
         *                 .print();
         */

        // create paimon table for save mkt infusion pump origin data
        /**
         * tableEnv.executeSql("DROP TABLE ods_equ_mkt_infusion_pump_hp_series");
         */
        tableEnv.executeSql("DROP TABLE ods_equ_mkt_infusion_pump_hp_series");
        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS ods_equ_mkt_infusion_pump_hp_series (\n" +
                "    `factory_num` STRING,\n" +
                "    `equ_type` STRING,\n" +
                "    `equ_num` STRING,\n" +
                "    `work_sta` STRING,\n" +
                "    `dept_name` STRING,\n" +
                "    `room_no` STRING,\n" +
                "    `bed_no` STRING,\n" +
                "    `state` STRING,\n" +
                "    `drug_name` STRING,\n" +
                "    `inject_mode` STRING,\n" +
                "    `preset_value` FLOAT,\n" +
                "    `speed` FLOAT,\n" +
                "    `already_inject_time` INTEGER,\n" +
                "    `remain_time` INTEGER,\n" +
                "    `already_inject_value` FLOAT,\n" +
                "    `residual` FLOAT,\n" +
                "    `alarm1` STRING,\n" +
                "    `alarm2` STRING,\n" +
                "    `alarm3` STRING,\n" +
                "    `alarm4` STRING,\n" +
                "    `pressure_value` FLOAT,\n" +
                "    `pressure_uint` STRING,\n" +
                "\n" +
                "    `host` STRING,\n" +
                "    `port` BIGINT,\n" +
                "    `code` BIGINT,\n" +
                "    `header` MAP<STRING, STRING>,\n" +
                "    `ts` BIGINT,\n" +
                "\n" +
                "    `dt` STRING,\n" +
                "    `hh` STRING,\n" +
                "\n" +
                "    PRIMARY KEY (dt, hh, factory_num, ts) NOT ENFORCED\n" +
                ") PARTITIONED BY (dt, hh) WITH (\n" +
                "    'bucket' = '1',\n" +
                "    'bucket-key' = 'factory_num, ts',\n" +
                "    'write-mode' = 'change-log'\n" +
                ");");

        tableEnv.executeSql("INSERT INTO ods_equ_mkt_infusion_pump_hp_series SELECT \n" +
                "\n" +
                "    `data`['factoryNum'] as `factory_num`,\n" +
                "    `data`['equType'] as `equ_type`,\n" +
                "    `data`['equNum'] as `equ_num`,\n" +
                "    `data`['workSta'] as `work_sta`,\n" +
                "    `data`['deptName'] as `dept_name`,\n" +
                "    `data`['roomNo'] as `room_no`,\n" +
                "    `data`['bedNo'] as `bed_no`,\n" +
                "    `data`['state'] as `state`,\n" +
                "    `data`['drugName'] as `drug_name`,\n" +
                "    `data`['injectMode'] as `inject_mode`,\n" +
                "    CAST(`data`['presetValue'] AS FLOAT) as `preset_value`,\n" +
                "    CAST(`data`['speed'] AS FLOAT) as `speed`,\n" +
                "    CAST(`data`['alreadyInjectTime'] AS INTEGER) as `already_inject_time`,\n" +
                "    CAST(`data`['remainTime'] AS INTEGER) as `remain_time`,\n" +
                "    CAST(`data`['alreadyInjectValue'] AS FLOAT) as `already_inject_value`,\n" +
                "    CAST(`data`['residual'] AS FLOAT) as `residual`,\n" +
                "    `data`['alarm1'] as `alarm1`,\n" +
                "    `data`['alarm2'] as `alarm2`,\n" +
                "    `data`['alarm3'] as `alarm3`,\n" +
                "    `data`['alarm4'] as `alarm4`,\n" +
                "    CAST(`data`['pressureValue'] AS  FLOAT) as `pressure_value`,\n" +
                "    `data`['pressureUint'] as `pressure_uint`,\n" +
                "\n" +
                "    `host`,\n" +
                "    `port`,\n" +
                "    `code`,\n" +
                "    `header`,\n" +
                "    `ts`,\n" +
                "\n" +
                "    DATE_FORMAT(`ts_ltz`,'yyyy-MM-dd') as dt,\n" +
                "    CAST(HOUR(`ts_ltz`) AS STRING) as hh\n" +
                "\n" +
                "FROM kafka_source_hp_series;");
    }
}
