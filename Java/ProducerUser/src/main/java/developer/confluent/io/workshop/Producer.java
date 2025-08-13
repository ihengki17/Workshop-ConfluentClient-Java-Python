package developer.confluent.io.workshop;

import com.github.javafaker.Faker;
import io.confluent.developer.avro.User;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import lombok.RequiredArgsConstructor;



@Service
@RequiredArgsConstructor
public class Producer {

  // private final KafkaTemplate<Integer, Hobbit> template;
  private final  KafkaTemplate<String, User> template;

  Faker faker;

  @EventListener(ApplicationStartedEvent.class)
  public void generate() {

    faker = Faker.instance();
    while (true) {
      final String cust_id = faker.idNumber().valid();
      String formatted_cust_id = cust_id.replaceAll("-", "");
      final String ktp1 = "327106301";
      final String ktp2 = faker.number().digits(8);
      String formatted_ktp = ktp1.concat(ktp2);
      final String fullname = faker.name().fullName();
      final String phone_number = faker.phoneNumber().cellPhone();
      User send_value = new User(formatted_cust_id,formatted_ktp,fullname,phone_number);
      template.send("avro",formatted_ktp, send_value);
      System.out.println("Sent "+send_value);
      try {
        Thread.sleep(100); // Sleep for 5 seconds
      } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
      }
    }
  }
}
