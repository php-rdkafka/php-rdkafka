--TEST--
KafkaConsumer closes and hands over partitions with the consumer group protocol (KIP-848)
--SKIPIF--
<?php
require __DIR__ . '/helpers/kip848-integration-tests-check.php';
if (!method_exists(RdKafka\KafkaConsumer::class, 'closeAsync')) {
    die('skip requires librdkafka >= 1.9.0');
}
--FILE--
<?php
require __DIR__ . '/helpers/kip848-integration-tests-check.php';
require __DIR__ . '/helpers/close-async-handoff.php';
require __DIR__ . '/helpers/split-partition-queue.php';

$brokers = getenv('TEST_KAFKA_KIP848_BROKERS');

echo "Handoff\n";
testCloseAsyncHandoff($brokers, ['group.protocol' => 'consumer']);

echo "Partition queue routing\n";
testSplitPartitionQueueRouting($brokers, ['group.protocol' => 'consumer']);

$topicName = sprintf('test_rdkafka_%s', uniqid());

$conf = new RdKafka\Conf();
$conf->set('metadata.broker.list', $brokers);
$producer = new RdKafka\Producer($conf);
$producer->newTopic($topicName)->produce(0, 0, 'message');

if ($producer->flush(10000) !== RD_KAFKA_RESP_ERR_NO_ERROR) {
    throw new RuntimeException('Timed out creating the test topic');
}

function createConsumer(string $brokers, string $topicName, bool $throwOnRevoke = false): RdKafka\KafkaConsumer
{
    $conf = new RdKafka\Conf();
    $conf->set('metadata.broker.list', $brokers);
    $conf->set('group.id', sprintf('test_rdkafka_group_%s', uniqid()));
    $conf->set('group.protocol', 'consumer');
    $conf->set('log_level', '0');

    $conf->setRebalanceCb(function (RdKafka\KafkaConsumer $consumer, int $err, array $partitions) use ($throwOnRevoke) {
        if ($err === RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS) {
            $consumer->incrementalAssign($partitions);
        } elseif ($throwOnRevoke) {
            throw new RuntimeException('revoke callback');
        } else {
            $consumer->incrementalUnassign($partitions);
        }
    });

    $consumer = new RdKafka\KafkaConsumer($conf);
    $consumer->subscribe([$topicName]);

    return $consumer;
}

function waitForAssignment(RdKafka\KafkaConsumer $consumer): void
{
    $deadline = microtime(true) + 30;
    while ($consumer->getAssignment() === [] && microtime(true) < $deadline) {
        $consumer->consume(100);
    }

    if ($consumer->getAssignment() === []) {
        throw new RuntimeException('Timed out waiting for the assignment');
    }
}

echo "Destruction with an assignment\n";
$consumer = createConsumer($brokers, $topicName);
waitForAssignment($consumer);
unset($consumer);

echo "Close when the revoke callback throws before acknowledging\n";
$consumer = createConsumer($brokers, $topicName, true);
waitForAssignment($consumer);

try {
    $consumer->close();
} catch (RuntimeException $e) {
    echo 'Closed, then threw: ', $e->getMessage(), "\n";
}

?>
--EXPECT--
Handoff
Assigned one partition to each consumer
Both consumers received their records
Departing consumer closed and the other owns both partitions
Remaining consumer received a new record from the transferred partition
Closed both consumers
Partition queue routing
consumer queue: other 0, other 1, other 2, EOF other topic
partition queue: split 0, split 1, split 2, EOF split topic
committed: 3, 3
partition queue after reassignment: split 3
partition queue after reassignment: split 4
Destruction with an assignment
Close when the revoke callback throws before acknowledging
Closed, then threw: revoke callback
