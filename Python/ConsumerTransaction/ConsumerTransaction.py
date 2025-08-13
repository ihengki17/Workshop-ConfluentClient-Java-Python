import os
from dotenv import load_dotenv
from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer


class Transaction(object):
    def __init__(self, cust_id, trans_id, card_number, amount):
        self.cust_id = cust_id
        self.trans_id = trans_id
        self.card_number = card_number
        self.amount = amount


def dict_to_transaction(obj, ctx):
    if obj is None:
        return None
    return Transaction(cust_id=obj['cust_id'],
                trans_id=obj['trans_id'],
                card_number=obj['card_number'],
                amount=obj['amount'])


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
    GROUP_ID='transaction-group'
    TOPIC='transaction'
    
   
    schema = "Transaction.avsc"
    
    with open(f"../avro/{schema}") as f:
        schema_str = f.read()

    schema_registry_conf = {'url': SCHEMA_REGISTRY_URL,
                            'basic.auth.user.info': SCHEMA_REGISTRY_CREDENTIAL}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    avro_deserializer = AvroDeserializer(schema_registry_client,
                                     schema_str,
                                     dict_to_transaction)

    consumer_conf = {
    'bootstrap.servers': BOOTSTRAP_SERVER,
    'sasl.mechanism': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': CONFLUENT_API_KEY,
    'sasl.password': CONFLUENT_API_SECRET,
    'group.id': GROUP_ID,
    'auto.offset.reset': "earliest"}

    consumer = Consumer(consumer_conf)
    consumer.subscribe([TOPIC])

    while True:
        try:
            # SIGINT can't be handled when polling, limit timeout to 1 second.
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            transaction = avro_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
            if transaction is not None:
                print("Transaction record {}: cust_id: {}\n"
                      "\ttrans_id: {}\n"
                      "\tcard_number: {}\n"
                      "\tamount: {}\n"
                      .format(msg.key(), transaction.cust_id,
                              transaction.trans_id,
                              transaction.card_number,
                              transaction.amount))
        except KeyboardInterrupt:
            break

    consumer.close()


if __name__ == '__main__':

    main()

