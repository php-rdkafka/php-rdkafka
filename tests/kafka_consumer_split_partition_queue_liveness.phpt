--TEST--
Consuming from a partition queue keeps the consumer in its group
--SKIPIF--
<?php
require __DIR__ . '/helpers/integration-tests-check.php';
--FILE--
<?php
require __DIR__ . '/helpers/integration-tests-check.php';

$topicName = sprintf('test_rdkafka_%s', uniqid());

$conf = new RdKafka\Conf();
$conf->set('metadata.broker.list', getenv('TEST_KAFKA_BROKERS'));
$producer = new RdKafka\Producer($conf);
$producer->newTopic($topicName)->produce(0, 0, 'message');

if ($producer->flush(10000) !== RD_KAFKA_RESP_ERR_NO_ERROR) {
    throw new RuntimeException('Timed out creating the test topic');
}

$conf = new RdKafka\Conf();
$conf->set('metadata.broker.list', getenv('TEST_KAFKA_BROKERS'));
$conf->set('group.id', sprintf('test_rdkafka_group_%s', uniqid()));
$conf->set('auto.offset.reset', 'earliest');
$conf->set('session.timeout.ms', '6000');
$conf->set('max.poll.interval.ms', '7000');
$conf->set('log_level', '0');

$rebalances = [];
$conf->setRebalanceCb(function (RdKafka\KafkaConsumer $consumer, int $err, array $partitions) use (&$rebalances) {
    $rebalances[] = $err === RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS ? 'assign' : 'revoke';

    if ($err === RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS) {
        $consumer->splitPartitionQueue($partitions[0]->getTopic(), $partitions[0]->getPartition());
        $consumer->assign($partitions);
    } else {
        $consumer->assign(null);
    }
});

$consumer = new RdKafka\KafkaConsumer($conf);
$consumer->subscribe([$topicName]);

$deadline = microtime(true) + 30;
while ($rebalances === [] && microtime(true) < $deadline) {
    $consumer->consume(100);
}

// Only the partition queue is served for twice max.poll.interval.ms
$queue = $consumer->splitPartitionQueue($topicName, 0);
$records = 0;
$until = microtime(true) + 14;
while (microtime(true) < $until) {
    $message = $queue->consume(100);

    if ($message !== null && $message->err === RD_KAFKA_RESP_ERR_NO_ERROR) {
        $records++;
    }
}

var_dump($records);

// A missed poll interval would have queued an error and a revocation
$message = $consumer->consume(1000);
var_dump($message === null || $message->err === RD_KAFKA_RESP_ERR__PARTITION_EOF);
var_dump($rebalances);
var_dump(count($consumer->getAssignment()));

unset($queue);
$consumer->close();

?>
--EXPECT--
int(1)
bool(true)
array(1) {
  [0]=>
  string(6) "assign"
}
int(1)
