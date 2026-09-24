--TEST--
KafkaConsumer::splitPartitionQueue() routes a partition's messages to its own queue
--SKIPIF--
<?php
require __DIR__ . '/helpers/integration-tests-check.php';
--FILE--
<?php
require __DIR__ . '/helpers/integration-tests-check.php';
require __DIR__ . '/helpers/split-partition-queue.php';

foreach (['range' => 'eager', 'cooperative-sticky' => 'cooperative'] as $strategy => $protocol) {
    echo "$protocol\n";
    testSplitPartitionQueueRouting(getenv('TEST_KAFKA_BROKERS'), ['partition.assignment.strategy' => $strategy]);
}

?>
--EXPECT--
eager
consumer queue: other 0, other 1, other 2, EOF other topic
partition queue: split 0, split 1, split 2, EOF split topic
committed: 3, 3
partition queue after reassignment: split 3
partition queue after reassignment: split 4
cooperative
consumer queue: other 0, other 1, other 2, EOF other topic
partition queue: split 0, split 1, split 2, EOF split topic
committed: 3, 3
partition queue after reassignment: split 3
partition queue after reassignment: split 4
