package de.azapps.kafkabackup.restore.common;

import java.util.Map;
import lombok.Setter;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class KafkaConsumerFactory {

  @Setter
  private static KafkaConsumerFactory factory;

  public static  KafkaConsumerFactory getFactory() {
    if(factory == null) {
      factory = new KafkaConsumerFactory();
    }

    return factory;
  }

  public <K, V> KafkaConsumer<K, V> createConsumer(Class<K> key, Class<V> value, Map<String, Object> config) {
    return new KafkaConsumer<>(config);
  }
}
