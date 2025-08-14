package developer.confluent.io.workshop;

import com.github.javafaker.Faker;

import io.confluent.developer.avro.Transaction;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import lombok.RequiredArgsConstructor;



@Service
@RequiredArgsConstructor
public class Producer {

  // private final KafkaTemplate<Integer, Hobbit> template;
  private final  KafkaTemplate<String, Transaction> template;

  Faker faker;

  @EventListener(ApplicationStartedEvent.class)
  public void generate() {

    faker = Faker.instance();
    while (true) {
      final String cust_id = faker.idNumber().valid();
      String formatted_cust_id = cust_id.replaceAll("-", "");
      final String trans_id = faker.number().digits(9);
      final String card_number = faker.business().creditCardNumber();
      String formatted_card_number = card_number.replaceAll("-", "");
      final String amount = faker.commerce().price();
      Transaction send_value = new Transaction(formatted_cust_id,trans_id,formatted_card_number,amount);
      template.send("transaction",formatted_card_number, send_value);
      System.out.println("Sent "+send_value);
      try {
        Thread.sleep(100); // Sleep for 100 miliseconds
      } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
      }
    }
  }
}
