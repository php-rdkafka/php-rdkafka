--TEST--
RdKafka\Producer::initTransactions() does not leak the rd_kafka_error_t on error
--SKIPIF--
<?php
require __DIR__ . '/helpers/integration-tests-check.php';
if (!class_exists("RdKafka\\KafkaErrorException")) {
    echo "skip";
}
--FILE--
<?php
require __DIR__ . '/helpers/integration-tests-check.php';

$conf = new RdKafka\Conf();
$conf->set('metadata.broker.list', getenv('TEST_KAFKA_BROKERS'));

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
