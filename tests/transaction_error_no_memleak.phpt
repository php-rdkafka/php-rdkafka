--TEST--
RdKafka\Producer::initTransactions() does not leak the rd_kafka_error_t on error
--FILE--
<?php

$conf = new RdKafka\Conf();
$conf->set('metadata.broker.list', '127.0.0.1:9092');

$producer = new RdKafka\Producer($conf);

// transactional.id is not configured, so each call returns an error object
// that the extension owns and must destroy.
for ($i = 0; $i < 5; $i++) {
    try {
        $producer->initTransactions(1000);
    } catch (RdKafka\KafkaErrorException $e) {
    }
}

unset($producer);

echo "OK\n";
--EXPECT--
OK
