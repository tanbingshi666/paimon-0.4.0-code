import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class MockDataSinkKafka {

    public static void main(String[] args) {

        int count = 500;

        String message1 = "{\n" +
                "    \"startDate\": \"2023-06-25 14:17:09\",\n" +
                "    \"equId\":\"10001\",\n" +
                "    \"deptName\":\"nxyk\",\n" +
                "    \"roomNo\":\"1\",\n" +
                "    \"bedNo\":\"1\",\n" +
                "    \"HR\":\"78\",\n" +
                "    \"PVCs\":\"0\",\n" +
                "    \"RR\":null,\n" +
                "    \"SpO2\":null,\n" +
                "    \"PR\":null,\n" +
                "    \"ts\":1687673829100\n" +
                "}";

        String message2 = "{\n" +
                "    \"startDate\": \"2023-06-25 14:17:09\",\n" +
                "    \"equId\":\"10001\",\n" +
                "    \"deptName\":\"nxyk\",\n" +
                "    \"roomNo\":\"1\",\n" +
                "    \"bedNo\":\"1\",\n" +
                "    \"HR\":null,\n" +
                "    \"PVCs\":null,\n" +
                "    \"RR\":\"23\",\n" +
                "    \"SpO2\":null,\n" +
                "    \"PR\":null,\n" +
                "    \"ts\":1687673829100\n" +
                "}";

        String message3 = "{\n" +
                "    \"startDate\": \"2023-06-25 14:17:09\",\n" +
                "    \"equId\":\"10001\",\n" +
                "    \"deptName\":\"nxyk\",\n" +
                "    \"roomNo\":\"1\",\n" +
                "    \"bedNo\":\"1\",\n" +
                "    \"HR\":null,\n" +
                "    \"PVCs\":null,\n" +
                "    \"RR\":null,\n" +
                "    \"SpO2\":\"97\",\n" +
                "    \"PR\":\"74\",\n" +
                "    \"ts\":1687673829100\n" +
                "}";

        String message4 = "{\n" +
                "    \"startDate\": \"2023-06-25 14:17:09\",\n" +
                "    \"equId\":\"10001\",\n" +
                "    \"deptName\":\"nxyk\",\n" +
                "    \"roomNo\":\"1\",\n" +
                "    \"bedNo\":\"1\",\n" +
                "    \"HR\":null,\n" +
                "    \"PVCs\":null,\n" +
                "    \"RR\":null,\n" +
                "    \"SpO2\":\"100\",\n" +
                "    \"PR\":\"74\",\n" +
                "    \"ts\":1687673829100\n" +
                "}";

        String message5 = "{\n" +
                "    \"startDate\": \"2023-06-25 14:17:10\",\n" +
                "    \"equId\":\"10001\",\n" +
                "    \"deptName\":\"手术室:ICU\",\n" +
                "    \"roomNo\":\"ICU-601\",\n" +
                "    \"bedNo\":\"601-01\",\n" +
                "    \"HR\":\"78\",\n" +
                "    \"PVCs\":\"0\",\n" +
                "    \"RR\":null,\n" +
                "    \"SpO2\":null,\n" +
                "    \"PR\":null,\n" +
                "    \"ts\":1687673830000\n" +
                "}";

        String message6 = "{\n" +
                "    \"startDate\": \"2023-06-25 14:17:10\",\n" +
                "    \"equId\":\"10001\",\n" +
                "    \"deptName\":\"手术室:ICU\",\n" +
                "    \"roomNo\":\"ICU-601\",\n" +
                "    \"bedNo\":\"601-01\",\n" +
                "    \"HR\":null,\n" +
                "    \"PVCs\":null,\n" +
                "    \"RR\":\"23\",\n" +
                "    \"SpO2\":null,\n" +
                "    \"PR\":null,\n" +
                "    \"ts\":1687673830000\n" +
                "}";

        String message7 = "{\n" +
                "    \"startDate\": \"2023-06-25 14:17:10\",\n" +
                "    \"equId\":\"10001\",\n" +
                "    \"deptName\":\"手术室:ICU\",\n" +
                "    \"roomNo\":\"ICU-601\",\n" +
                "    \"bedNo\":\"601-01\",\n" +
                "    \"HR\":null,\n" +
                "    \"PVCs\":null,\n" +
                "    \"RR\":null,\n" +
                "    \"SpO2\":\"97\",\n" +
                "    \"PR\":\"74\",\n" +
                "    \"ts\":1687673830000\n" +
                "}";


        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "hadoop102:9092");
        //向kafka集群发送消息,除了消息值本身,还包括key信息,key信息用于消息在partition之间均匀分布。
        //发送消息的key,类型为String,使用String类型的序列化器
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //发送消息的value,类型为String,使用String类型的序列化器
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //创建一个KafkaProducer对象，传入上面创建的Properties对象
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(kafkaProps);

        for (int i = 0; i < 1; i++) {
            JSONObject json = JSONUtil.parseObj(message7);
            producer.send(new ProducerRecord<>("temp_06_14", json.toJSONString(0)));
            try {
                Thread.sleep(2 * 1000L);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        producer.close();

    }

}
