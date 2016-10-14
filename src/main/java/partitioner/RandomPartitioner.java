package partitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;
import java.util.Random;

/**
 * Created by coderjiang@sina.com on 2016/10/12.
 */
public class RandomPartitioner implements Partitioner {

    private Random ran = new Random();

    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        int partitionerNum = cluster.partitionsForTopic(topic).size();
        return ran.nextInt(partitionerNum);
    }

    public void close() {

    }

    public void configure(Map<String, ?> configs) {

    }
}
