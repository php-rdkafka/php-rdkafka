--TEST--
KafkaConsumer::close() reports close errors and destruction warns about them
--SKIPIF--
<?php
require __DIR__ . '/helpers/integration-tests-check.php';
--FILE--
<?php
require __DIR__ . '/helpers/integration-tests-check.php';
require __DIR__ . '/helpers/fenced-consumer.php';

$consumer = createFencedConsumer(getenv('TEST_KAFKA_BROKERS'));

try {
    $consumer->close();
} catch (RdKafka\Exception $e) {
    echo get_class($e), ': ', $e->getMessage(), "\n";
    var_dump($e->getCode() === RD_KAFKA_RESP_ERR__FATAL);
}

try {
    $consumer->getAssignment();
} catch (Exception $e) {
    echo $e->getMessage(), "\n";
}

$consumer = createFencedConsumer(getenv('TEST_KAFKA_BROKERS'));
unset($consumer);

?>
--EXPECTF--
RdKafka\Exception: Local: Fatal error
bool(true)
RdKafka\KafkaConsumer::__construct() has not been called, or RdKafka\KafkaConsumer::close() was already called

Warning: rd_kafka_consumer_close failed: Local: Fatal error in %s on line %d
