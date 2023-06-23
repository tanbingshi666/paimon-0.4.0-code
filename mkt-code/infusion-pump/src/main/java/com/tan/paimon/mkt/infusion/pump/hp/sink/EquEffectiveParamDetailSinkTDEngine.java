package com.tan.paimon.mkt.infusion.pump.hp.sink;

import com.tan.paimon.mkt.infusion.pump.utils.FlinkStreamingSinkJdbcUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class EquEffectiveParamDetailSinkTDEngine {

    public static void main(String[] args) {

        System.setProperty("HADOOP_USER_NAME", "hdfs");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // checkpoint setting from: https://nightlies.apache.org/flink/flink-docs-release-1.16/zh/docs/dev/datastream/fault-tolerance/checkpointing/
        // env.enableCheckpointing(3 * 60 * 1000L, CheckpointingMode.EXACTLY_ONCE);
        // env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
        // env.getCheckpointConfig().setMinPauseBetweenCheckpoints(60 * 1000L);
        // env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // env.getCheckpointConfig().setTolerableCheckpointFailureNumber(2);
        // env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // state backend setting from: https://nightlies.apache.org/flink/flink-docs-release-1.16/zh/docs/ops/state/state_backends/
        // env.setStateBackend(new HashMapStateBackend());
        // env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop101:8020/meiotds/chk/mkt/hp/dwd");

        String brokers = "hadoop102:9092,hadoop103:9092,hadoop104:9092";
        List<String> topics = Collections.singletonList("MKT_INFUSION_PUMP_EQU_EFFECTIVE_PARAM_DETAIL");
        String groupId = "MKT_INFUSION_PUMP_EQU_EFFECTIVE_PARAM_DETAIL_06_22";
        Properties props = new Properties();

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics(topics)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperty("partition.discovery.interval.ms", "10000") // 动态分区发现策略
                .setProperties(props)
                .build();

        DataStreamSource<String> kafkaDataStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");
        kafkaDataStream.addSink(FlinkStreamingSinkJdbcUtils.getPOJOJdbcSink(
                "com.taosdata.jdbc.rs.RestfulDriver",
                "jdbc:TAOS-RS://hj103:6041/mkt_infusion_pump?user=root&password=taosdata",
                "INSERT INTO ? USING infusion_pump_detail TAGS (? ,? ,? ,? ,? ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                "root",
                "taosdata",
                3
        ));

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
