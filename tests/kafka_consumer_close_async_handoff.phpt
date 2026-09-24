--TEST--
KafkaConsumer::closeAsync() hands the consumer's partitions over to the rest of the group
--SKIPIF--
<?php
require __DIR__ . '/helpers/integration-tests-check.php';
if (!method_exists(RdKafka\KafkaConsumer::class, 'closeAsync')) {
    die('skip requires librdkafka >= 1.9.0');
}
--FILE--
<?php
require __DIR__ . '/helpers/integration-tests-check.php';
require __DIR__ . '/helpers/close-async-handoff.php';

foreach (['roundrobin' => 'eager', 'cooperative-sticky' => 'cooperative'] as $strategy => $protocol) {
    echo "$protocol\n";
    testCloseAsyncHandoff(getenv('TEST_KAFKA_BROKERS'), ['partition.assignment.strategy' => $strategy]);
}

echo "cooperative, closed while closing asynchronously\n";
testCloseAsyncHandoff(getenv('TEST_KAFKA_BROKERS'), ['partition.assignment.strategy' => 'cooperative-sticky'], true);

?>
--EXPECT--
eager
Assigned one partition to each consumer
Both consumers received their records
Departing consumer closed and the other owns both partitions
Remaining consumer received a new record from the transferred partition
Closed both consumers
cooperative
Assigned one partition to each consumer
Both consumers received their records
Departing consumer closed and the other owns both partitions
Remaining consumer received a new record from the transferred partition
Closed both consumers
cooperative, closed while closing asynchronously
Assigned one partition to each consumer
Both consumers received their records
Departing consumer closed while closing asynchronously
RdKafka\Queue is not initialized or its client has been closed
RdKafka\Queue is not initialized or its client has been closed
RdKafka\Topic is not initialized or its client has been closed
Departing consumer closed and the other owns both partitions
Remaining consumer received a new record from the transferred partition
Closed both consumers
