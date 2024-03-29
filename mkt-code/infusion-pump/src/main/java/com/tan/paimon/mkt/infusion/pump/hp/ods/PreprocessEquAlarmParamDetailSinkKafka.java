package com.tan.paimon.mkt.infusion.pump.hp.ods;

import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.StringUtils;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class PreprocessEquAlarmParamDetailSinkKafka {

    public static void main(String[] args) {

        System.setProperty("HADOOP_USER_NAME", "tanbs");

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
        List<String> topics = Collections.singletonList("MKT_INFUSION_PUMP_HP_SERIES_JSON");
        String groupId = "MKT_INFUSION_PUMP_HP_SERIES_JSON_23_06_09";
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

        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers(brokers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("MKT_INFUSION_PUMP_EQU_ALARM_PARAM_DETAIL")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        DataStreamSource<String> kafkaDataStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");
        kafkaDataStream.filter(line -> !StringUtils.isNullOrWhitespaceOnly(line))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<String>forMonotonousTimestamps()
                                .withTimestampAssigner((SerializableTimestampAssigner<String>) (line, current) -> JSONUtil.parseObj(line).getLong("ts", 0L))
                                .withIdleness(Duration.ofMinutes(1L))
                ).keyBy(line -> {
                    JSONObject json = JSONUtil.parseObj(line).getJSONObject("data");
                    return json.getStr("factoryNum");
                }).process(new KeyedProcessFunction<String, String, String>() {

                    private transient DateTimeFormatter formatter;

                    private ValueState<String> startDateTime;
                    private ValueState<Long> lastTimestamp;

                    @Override
                    public void open(Configuration parameters) throws Exception {

                        formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss").withZone(ZoneId.systemDefault());

                        ValueStateDescriptor<String> startDateTimeDescriptor = new ValueStateDescriptor<>("equ-start-datetime-per", String.class);
                        ValueStateDescriptor<Long> lastTimestampDescriptor = new ValueStateDescriptor<>("last-equ-record-timestamp", Long.class);

                        startDateTime = getRuntimeContext().getState(startDateTimeDescriptor);
                        lastTimestamp = getRuntimeContext().getState(lastTimestampDescriptor);
                    }

                    @Override
                    public void processElement(String line,
                                               KeyedProcessFunction<String, String, String>.Context context,
                                               Collector<String> out) throws Exception {
                        JSONObject json = JSONUtil.parseObj(line);
                        Long ts = json.getLong("ts", 0L);

                        // first record
                        if (StringUtils.isNullOrWhitespaceOnly(startDateTime.value())) {
                            startDateTime.update(formatter.format(Instant.ofEpochMilli(ts)));
                            lastTimestamp.update(ts);
                        } else {
                            Long lastTs = lastTimestamp.value();
                            if (ts - lastTs > 60 * 1000L) {
                                startDateTime.update(formatter.format(Instant.ofEpochMilli(ts)));
                            }
                        }

                        lastTimestamp.update(ts);

                        JSONObject data = json.getJSONObject("data");
                        String injectMode = data.getStr("injectMode");

                        List<String> alarms = Arrays.asList(
                                data.getStr("alarm1"),
                                data.getStr("alarm2"),
                                data.getStr("alarm3"),
                                data.getStr("alarm4"));
                        for (int i = 0; i < alarms.size(); i++) {
                            if (StringUtils.isNullOrWhitespaceOnly(alarms.get(i))) {
                                continue;
                            }
                            JSONObject outputJson = new JSONObject();
                            outputJson.set("factoryNum", context.getCurrentKey());
                            outputJson.set("startDateTime", startDateTime.value());
                            outputJson.set("injectMode", injectMode);
                            outputJson.set("alarmType", i + 1);
                            outputJson.set("alarmValue", alarms.get(i));
                            outputJson.set("alarmTs", ts);

                            outputJson.set("deptName", data.getStr("deptName"));
                            outputJson.set("roomNo", data.getStr("roomNo"));
                            outputJson.set("bedNo", data.getStr("bedNo"));

                            out.collect(outputJson.toJSONString(0));
                        }
                    }
                }).sinkTo(kafkaSink);

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
