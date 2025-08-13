import os
from dotenv import load_dotenv
from faker import Faker
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer


class User(object):
    def __init__(self, cust_id, ssn, fullname, phone_number):
        self.cust_id = cust_id
        self.ssn = ssn
        self.fullname = fullname
        self.phone_number = phone_number


def user_to_dict(user, ctx):
    return dict(cust_id=user.cust_id,
                ssn=user.ssn,
                fullname=user.fullname,
                phone_number=user.phone_number)


def delivery_report(err, msg):
    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


def main():
    load_dotenv('../.env')
    BOOTSTRAP_SERVER=os.getenv('BOOTSTRAP_SERVER')
    CONFLUENT_API_KEY=os.getenv('CONFLUENT_API_KEY')
    CONFLUENT_API_SECRET=os.getenv('CONFLUENT_API_SECRET')
    SCHEMA_REGISTRY_URL=os.getenv('SCHEMA_REGISTRY_URL')
    SCHEMA_REGISTRY_CREDENTIAL=os.getenv('SCHEMA_REGISTRY_CREDENTIAL')
    TOPIC='user'
    
    fake = Faker()
    fake_id = Faker('id_ID')
    schema = "User.avsc"
    
    with open(f"../avro/{schema}") as f:
        schema_str = f.read()

    schema_registry_conf = {'url': SCHEMA_REGISTRY_URL,
                            'basic.auth.user.info': SCHEMA_REGISTRY_CREDENTIAL}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    avro_serializer = AvroSerializer(schema_registry_client,
                                     schema_str,
                                     user_to_dict)

    string_serializer = StringSerializer('utf_8')

    producer_conf = {
    'bootstrap.servers': BOOTSTRAP_SERVER,
    'sasl.mechanism': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': CONFLUENT_API_KEY,
    'sasl.password': CONFLUENT_API_SECRET}

    producer = Producer(producer_conf)

    print("Producing user records to topic {}. ^C to exit.".format(TOPIC))
    while True:
        # Serve on_delivery callbacks from previous calls to produce()
        producer.poll(0.0)
        try:
            usr_ssn = str(fake.ssn())
            usr_custid = str(fake.bothify(text='ID-####-???'))
            usr_full_name = str(fake.name())
            usr_phone_number = str(fake_id.phone_number())
            user = User(ssn=usr_ssn,
                        cust_id=usr_custid,
                        fullname=usr_full_name,
                        phone_number=usr_phone_number)
            producer.produce(topic=TOPIC,
                             key=string_serializer(usr_ssn),
                             value=avro_serializer(user, SerializationContext(TOPIC, MessageField.VALUE)),
                             on_delivery=delivery_report)
        except KeyboardInterrupt:
            break
        except ValueError:
            print("Invalid input, discarding record...")
            continue

    print("\nFlushing records...")
    producer.flush()


if __name__ == '__main__':

    main()
