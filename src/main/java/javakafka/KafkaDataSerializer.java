package javakafka;

import kafka.KafkaData;
import org.apache.kafka.common.serialization.Serializer;

import java.io.*;
import java.util.Map;

/**
 * 自定义序列化类，把KafkaData对象序列化成byte[]
 *
 * @author HuGuangtao
 * @version 1.0
 * @date 2021/8/20 9:23
 */
public class KafkaDataSerializer implements Serializer<KafkaData> {


    @Override
    public void configure(Map configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, KafkaData data) {
        // 利用java的序列化框架 ObjectOutputStream.writeObject
        // KafkaData --> byte[]
        if (data == null) {
            return null;
        }
        ByteArrayOutputStream bos = null;
        ObjectOutputStream oos = null;
        try {
            bos = new ByteArrayOutputStream();
            oos = new ObjectOutputStream(bos);
            oos.writeObject(data);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                oos.close();
                bos.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
        return bos.toByteArray();
    }

    @Override
    public void close() {

    }
}
