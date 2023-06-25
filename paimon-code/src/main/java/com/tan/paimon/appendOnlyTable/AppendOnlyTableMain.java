package com.tan.paimon.appendOnlyTable;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * 测试 Paimon Append Only Table
 * 官方文档指南：https://paimon.apache.org/docs/0.4/concepts/append-only-table/
 * <p>
 * scan.plan-sort-partition=true 读取 Append Only 分区有序
 * scan.bounded.watermark + filter (默认全增量一体读 先读完全量 如果全量存储大于 end watermark 也会读 加 filter 双重保险) 读取指定 end watermark 之前的数据 (batch)
 */
public class AppendOnlyTableMain {

    public static void main(String[] args) {

        System.setProperty("HADOOP_USER_NAME", "hdfs");

        Configuration conf = new Configuration();
        // basic setting from https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/deployment/config/#basic-setup
        conf.setInteger("parallelism.default", 3);
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
        conf.setString("state.checkpoints.dir", "hdfs://hadoop101:8020/meiotds/chk/mkt/dws");

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .withConfiguration(conf)
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        tableEnv.executeSql("CREATE CATALOG my_catalog WITH (\n" +
                "    'type' = 'paimon',\n" +
                "    'warehouse' = 'hdfs://hadoop101:8020/paimon/code'\n" +
                ");");
        tableEnv.executeSql("USE CATALOG my_catalog;");

        tableEnv.executeSql("CREATE TEMPORARY TABLE IF NOT EXISTS kafka_source_hp_series (\n" +
                "  `factoryNum` STRING,\n" +
                "  `equType` STRING,\n" +
                "  `equNum` STRING,\n" +
                "  `workSta` STRING,\n" +
                "  `deptName` STRING,\n" +
                "  `roomNo` STRING,\n" +
                "  `bedNo` STRING,\n" +
                "  `state` STRING,\n" +
                "  `drugName` STRING,\n" +
                "  `injectMode` STRING,\n" +
                "  `presetValue` FLOAT,\n" +
                "  `speed` FLOAT,\n" +
                "  `alreadyInjectTime` INTEGER,\n" +
                "  `remainTime` INTEGER,\n" +
                "  `alreadyInjectValue` FLOAT,\n" +
                "  `residual` FLOAT,\n" +
                "  `alarm1` STRING,\n" +
                "  `alarm2` STRING,\n" +
                "  `alarm3` STRING,\n" +
                "  `alarm4` STRING,\n" +
                "  `pressureValue` FLOAT,\n" +
                "  `pressureUint` STRING,\n" +
                "  `ts` BIGINT,\n" +
                "  `ts_ltz` AS TO_TIMESTAMP_LTZ(ts, 3),\n" +
                "  WATERMARK FOR ts_ltz AS ts_ltz - INTERVAL '1' SECOND \n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'temp_06_12',\n" +
                "  'properties.bootstrap.servers' = 'hadoop101:9092',\n" +
                "  'properties.group.id' = 'MKT_INFUSION_PUMP_HP_SERIES_JSON_06_22',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json'\n" +
                ");");

        /**
         tableEnv.sqlQuery("select * from kafka_source_hp_series;")
         .execute()
         .print();
         */

        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS ods_kafka_source_hp_series_2 (\n" +
                "  `factory_num` STRING,\n" +
                "  `equ_type` STRING,\n" +
                "  `equ_num` STRING,\n" +
                "  `work_sta` STRING,\n" +
                "  `dept_name` STRING,\n" +
                "  `room_no` STRING,\n" +
                "  `bed_no` STRING,\n" +
                "  `state` STRING,\n" +
                "  `drug_name` STRING,\n" +
                "  `inject_mode` STRING,\n" +
                "  `preset_value` FLOAT,\n" +
                "  `speed` FLOAT,\n" +
                "  `already_inject_time` INTEGER,\n" +
                "  `remain_time` INTEGER,\n" +
                "  `already_inject_value` FLOAT,\n" +
                "  `residual` FLOAT,\n" +
                "  `alarm1` STRING,\n" +
                "  `alarm2` STRING,\n" +
                "  `alarm3` STRING,\n" +
                "  `alarm4` STRING,\n" +
                "  `pressure_value` FLOAT,\n" +
                "  `pressure_uint` STRING,\n" +
                "  `ts` BIGINT,\n" +
                "\n" +
                "  `dt` STRING\n" +
                ") PARTITIONED BY (dt) WITH (\n" +
                "  'bucket' = '3',\n" +
                "  'bucket-key' = 'factory_num',\n" +
                "  'write-mode' = 'append-only'\n" +
                ");");

        tableEnv.executeSql("INSERT INTO ods_kafka_source_hp_series_2 SELECT\n" +
                "\n" +
                "  `factoryNum` AS `factory_num`,\n" +
                "  `equType` AS `equ_type`,\n" +
                "  `equNum` AS `equ_num`,\n" +
                "  `workSta` AS `work_sta`,\n" +
                "  `deptName` AS `dept_name`,\n" +
                "  `roomNo` AS `room_no`,\n" +
                "  `bedNo` AS `bed_no`,\n" +
                "  `state` AS `state`,\n" +
                "  `drugName` AS `drug_name`,\n" +
                "  `injectMode` AS `inject_mode`,\n" +
                "  `presetValue` AS `preset_value`,\n" +
                "  `speed` AS `speed`,\n" +
                "  `alreadyInjectTime` AS `already_inject_time`,\n" +
                "  `remainTime` AS `remain_time`,\n" +
                "  `alreadyInjectValue` AS `already_inject_value`,\n" +
                "  `residual` AS `residual`,\n" +
                "  `alarm1` AS `alarm1`,\n" +
                "  `alarm2` AS `alarm2`,\n" +
                "  `alarm3` AS `alarm3`,\n" +
                "  `alarm4` AS `alarm4`,\n" +
                "  `pressureValue` AS `pressure_value`,\n" +
                "  `pressureUint` AS `pressure_uint`,\n" +
                "  `ts`,\n" +
                "   DATE_FORMAT(ts_ltz,'yyyy-MM-dd') AS `dt`\n" +
                "FROM kafka_source_hp_series;");

    }

}
