package developer.confluent.io.workshop;

import io.confluent.developer.avro.Transaction;
import org.springframework.stereotype.Service;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;



@Service
@RequiredArgsConstructor
public class Consumer {

  @KafkaListener(topics = {"avro"})
  public void consume(ConsumerRecord<Integer, Transaction> record) {
    System.out.println("received = " + record.value() + " with key " + record.key());
  }
}
