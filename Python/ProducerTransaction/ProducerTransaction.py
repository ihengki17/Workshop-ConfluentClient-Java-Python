import os
from dotenv import load_dotenv
from faker import Faker
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer


class Transaction(object):
    def __init__(self, cust_id, trans_id, card_number, amount):
        self.cust_id = cust_id
        self.trans_id = trans_id
        self.card_number = card_number
        self.amount = amount


def transaction_to_dict(transaction, ctx):
    return dict(cust_id=transaction.cust_id,
                trans_id=transaction.trans_id,
                card_number=transaction.card_number,
                amount=transaction.amount)


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
    TOPIC='transaction'
    
    fake = Faker()
    schema = "Transaction.avsc"
    
    with open(f"../avro/{schema}") as f:
        schema_str = f.read()

    schema_registry_conf = {'url': SCHEMA_REGISTRY_URL,
                            'basic.auth.user.info': SCHEMA_REGISTRY_CREDENTIAL}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    avro_serializer = AvroSerializer(schema_registry_client,
                                     schema_str,
                                     transaction_to_dict)

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
            trans_custid = str(fake.bothify(text='ID-####-???'))
            trans_transid = str(fake.random_number(digits=9))
            trans_cardnumber = str(fake.credit_card_number())
            trans_amount = str(fake.pyfloat(left_digits=6, right_digits=0, positive=True, min_value=10000, max_value=500000))
            transaction = Transaction(cust_id=trans_custid,
                        trans_id=trans_transid,
                        card_number=trans_cardnumber,
                        amount=trans_amount)
            producer.produce(topic=TOPIC,
                             key=string_serializer(trans_custid),
                             value=avro_serializer(transaction, SerializationContext(TOPIC, MessageField.VALUE)),
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
