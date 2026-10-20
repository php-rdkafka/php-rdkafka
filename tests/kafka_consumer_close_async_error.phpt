--TEST--
KafkaConsumer::closeAsync() reports initiation errors
--SKIPIF--
<?php
require __DIR__ . '/helpers/integration-tests-check.php';
if (!method_exists(RdKafka\KafkaConsumer::class, 'closeAsync')) {
    die('skip requires librdkafka >= 1.9.0');
}
--FILE--
<?php
require __DIR__ . '/helpers/integration-tests-check.php';
require __DIR__ . '/helpers/fenced-consumer.php';

$consumer = createFencedConsumer(getenv('TEST_KAFKA_BROKERS'));

try {
    $consumer->closeAsync();
} catch (RdKafka\KafkaErrorException $e) {
    echo get_class($e), ': ', $e->getMessage(), "\n";
    var_dump($e->isFatal());
}

// The failed initiation leaves the consumer open
var_dump($consumer->isClosed());

try {
    $consumer->close();
} catch (RdKafka\Exception $e) {
    echo get_class($e), ': ', $e->getMessage(), "\n";
}

?>
--EXPECT--
RdKafka\KafkaErrorException: Fatal consumer error: Broker: Static consumer fenced by other consumer with same group.instance.id (RD_KAFKA_RESP_ERR_FENCED_INSTANCE_ID)
bool(true)
bool(false)
RdKafka\Exception: Local: Fatal error
