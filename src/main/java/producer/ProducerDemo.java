package producer;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import partitioner.RandomPartitioner;

import java.util.Properties;

/**
 * Created by coderjiang@sina.com on 2016/10/12.
 */
public class ProducerDemo {

    private static final String TOPIC = "demo3";
    private static final String BROKER_LIST = "192.168.3.48:9092";

    public static void main(String[] args) throws InterruptedException {
        for(int i = 0; i < 2; i++) {
            Producer<String, String> producer = initProducer();
            sendOne(producer, TOPIC);
        }
    }

    private static Producer<String, String> initProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", BROKER_LIST);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("partitioner.class", RandomPartitioner.class.getName());

        Producer producer = new KafkaProducer(props);

        return producer;
    }

    public static void sendOne(Producer<String, String> producer, String topic) throws InterruptedException {

        producer.send(new ProducerRecord<String, String>(TOPIC, "{\n" +
                "\"Json解析\":\"支持格式化高亮折叠\",\n" +
                "\"支持XML转换\":\"支持XML转换Json,Json转XML\",\n" +
                "\"Json格式验证\":\"更详细准确的错误信息\"}"));

        producer.close();
    }

}
