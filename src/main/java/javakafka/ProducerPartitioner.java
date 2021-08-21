package javakafka;

import kafka.KafkaData;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * @author HuGuangtao
 * @version 1.0
 * @date 2021/8/20 10:29
 */
public class ProducerPartitioner implements Partitioner {
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {

        // 生产根据写入数据的数字，偶数分区0，奇数分区1
        KafkaData kafkaData = (KafkaData) value;
        String value1 = kafkaData.data();
        String[] words = value1.split("_");
        int number = Integer.parseInt(words[1]);
        if (number % 2 == 0) {
            return 0;
        } else {
            return 1;
        }
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
