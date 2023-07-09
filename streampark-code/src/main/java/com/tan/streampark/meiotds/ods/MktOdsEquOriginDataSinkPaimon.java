package com.tan.streampark.meiotds.ods;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * 麦科田原始解析数据输出到 Paimon
 */
public class MktOdsEquOriginDataSinkPaimon {

    public static void main(String[] args) {

        System.setProperty("HADOOP_USER_NAME", "hdfs");

        Configuration conf = new Configuration();
        // basic setting from https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/deployment/config/#basic-setup
        // conf.setInteger("parallelism.default", 6);
        // checkpoint setting from https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/deployment/config/#checkpointing
        // conf.setLong("execution.checkpointing.interval", 60 * 1000L);
        // conf.setString("execution.checkpointing.mode", "EXACTLY_ONCE");
        // conf.setLong("execution.checkpointing.timeout", 60 * 1000L);
        // conf.setLong("execution.checkpointing.min-pause", 60 * 1000L);
        // conf.setInteger("execution.checkpointing.max-concurrent-checkpoints", 1);
        // conf.setInteger("execution.checkpointing.tolerable-failed-checkpoints", 3);
        // conf.setString("execution.checkpointing.externalized-checkpoint-retention", "RETAIN_ON_CANCELLATION");
        // state backend setting from https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/deployment/config/#checkpoints-and-state-backends
        // conf.setString("state.backend", "hashmap");
        // conf.setString("state.checkpoint-storage", "filesystem");
        // conf.setString("state.checkpoints.dir", "hdfs://hadoop0102:8020/meiotds/chk/ods");

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .withConfiguration(conf)
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        tableEnv.executeSql("CREATE CATALOG meiotds_hive WITH (\n" +
                "    'type' = 'paimon',\n" +
                "    'metastore' = 'hive',\n" +
                "    'uri' = 'thrift://hadoop0104:9083',\n" +
                "    'warehouse' = 'hdfs://hadoop0102:8020/meiotds/warehouse'\n" +
                ");");
        tableEnv.executeSql("USE CATALOG meiotds_hive;");
        tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS meiotds_db;");
        tableEnv.executeSql("USE meiotds_db;");

        tableEnv.executeSql("CREATE TEMPORARY TABLE IF NOT EXISTS kafka_mkt_infusion_pump_hp_series_json (\n" +
                "  `ip` STRING,\n" +
                "  `port` BIGINT,\n" +
                "  `code` BIGINT,\n" +
                "  `header` MAP<STRING, STRING>,\n" +
                "  `startWindow` BIGINT,\n" +
                "  `data` MAP<STRING, STRING>,\n" +
                "  `ts` BIGINT,\n" +
                "  `ts_ltz` AS TO_TIMESTAMP_LTZ(ts, 3),\n" +
                "  WATERMARK FOR ts_ltz AS ts_ltz\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'MKT_INFUSION_PUMP_HP_SERIES_JSON',\n" +
                "  'properties.bootstrap.servers' = 'hadoop0102:9092,hadoop0103:9092,hadoop0104:9092',\n" +
                "  'properties.group.id' = 'MKT_INFUSION_PUMP_HP_SERIES_JSON_2023_07_09',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json'\n" +
                ");");

        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS `meiotds_db`.`ods_equ_mkt_infusion_pump_hp_series`\n" +
                "(\n" +
                "    -- 设备数据字段\n" +
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
                "    -- 基础数据字段\n" +
                "    `ip` STRING,\n" +
                "    `port` BIGINT,\n" +
                "    `code` BIGINT,\n" +
                "    `header` MAP<STRING,STRING>,\n" +
                "    `start_window` BIGINT,\n" +
                "    `ts` BIGINT,\n" +
                "    -- 分区字段\n" +
                "    `dt` STRING\n" +
                ") PARTITIONED BY (dt)\n" +
                "WITH (\n" +
                "      -- 桶个数\n" +
                "      'bucket' = '6',\n" +
                "      -- 桶排序 keys\n" +
                "      'bucket-key' = 'factory_num,start_window',\n" +
                "      -- 是否为只写 默认 false 如果设置为 true 则跳过自动 compaction 和 snapshot 过期 (则需要另起 compact job)\n" +
                "      'write-only' = 'false',\n" +
                "      -- 写模式为 append-only\n" +
                "      'write-mode' = 'append-only',\n" +
                "      -- 最小文件个数执行合并 默认 5\n" +
                "      'compaction.min.file-num' = '5',\n" +
                "      -- 最多文件个数执行合并 默认 50\n" +
                "      'compaction.max.file-num' = '50',\n" +
                "      -- 触发 commit 次数后执行 full compaction\n" +
                "      'full-compaction.delta-commits' = '5'\n" +
                ");");

        tableEnv.executeSql("INSERT INTO `meiotds_db`.`ods_equ_mkt_infusion_pump_hp_series`\n" +
                "SELECT `data`['factoryNum']                         as `factory_num`,\n" +
                "       `data`['equType']                            as `equ_type`,\n" +
                "       `data`['equNum']                             as `equ_num`,\n" +
                "       `data`['workSta']                            as `work_sta`,\n" +
                "       `data`['deptName']                           as `dept_name`,\n" +
                "       `data`['roomNo']                             as `room_no`,\n" +
                "       `data`['bedNo']                              as `bed_no`,\n" +
                "       `data`['state']                              as `state`,\n" +
                "       `data`['drugName']                           as `drug_name`,\n" +
                "       `data`['injectMode']                         as `inject_mode`,\n" +
                "       CAST(`data`['presetValue'] AS FLOAT)         as `preset_value`,\n" +
                "       CAST(`data`['speed'] AS FLOAT)               as `speed`,\n" +
                "       CAST(`data`['alreadyInjectTime'] AS INTEGER) as `already_inject_time`,\n" +
                "       CAST(`data`['remainTime'] AS INTEGER)        as `remain_time`,\n" +
                "       CAST(`data`['alreadyInjectValue'] AS FLOAT)  as `already_inject_value`,\n" +
                "       CAST(`data`['residual'] AS FLOAT)            as `residual`,\n" +
                "       `data`['alarm1']                             as `alarm1`,\n" +
                "       `data`['alarm2']                             as `alarm2`,\n" +
                "       `data`['alarm3']                             as `alarm3`,\n" +
                "       `data`['alarm4']                             as `alarm4`,\n" +
                "       CAST(`data`['pressureValue'] AS FLOAT)       as `pressure_value`,\n" +
                "       `data`['pressureUint']                       as `pressure_uint`,\n" +
                "\n" +
                "       `ip`,\n" +
                "       `port`,\n" +
                "       `code`,\n" +
                "       `header`,\n" +
                "       `startWindow` AS `start_window`,\n" +
                "       `ts`,\n" +
                "\n" +
                "       DATE_FORMAT(`ts_ltz`, 'yyyy-MM-dd')          as dt\n" +
                "\n" +
                "FROM kafka_mkt_infusion_pump_hp_series_json;");


    }

}
