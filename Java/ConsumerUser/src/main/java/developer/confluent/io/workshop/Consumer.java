package developer.confluent.io.workshop;

import io.confluent.developer.avro.User;
import org.springframework.stereotype.Service;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;



@Service
@RequiredArgsConstructor
public class Consumer {

  @KafkaListener(topics = {"user"})
  public void consume(ConsumerRecord<Integer, User> record) {
    System.out.println("received = " + record.value() + " with key " + record.key());
  }
}
