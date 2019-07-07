import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaProducerClient {
    public KafkaProducer producer = null;

    public KafkaProducerClient(String host) {
        producer = createKafkaProducer(host);
    }

    private KafkaProducer createKafkaProducer(final String host) {
        final Properties props = new Properties();

        props.put(ProducerConfig.CLIENT_ID_CONFIG, "aviation-dataset-consumer-client");
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, host);


        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }
}
