package javakafka;

import kafka.KafkaData;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Map;

/**
 * 定义反序列化类，用于把byte[] 反序列化成kafkaData对象
 *
 * @author HuGuangtao
 * @version 1.0
 * @date 2021/8/20 9:23
 */
public class KafkaDataDeserializer implements Deserializer {

    @Override
    public void configure(Map configs, boolean isKey) {

    }


    @Override
    public Object deserialize(String topic, byte[] data) {
        // byte[] ---> KafkaData
        if (data == null) {
            return null;
        }
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(data);
        ObjectInputStream objectInputStream = null;
        Object object = null;
        try {
            objectInputStream = new ObjectInputStream(byteArrayInputStream);
            object = objectInputStream.readObject();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            try {
                objectInputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return (KafkaData) object;
    }

    @Override
    public void close() {

    }
}
