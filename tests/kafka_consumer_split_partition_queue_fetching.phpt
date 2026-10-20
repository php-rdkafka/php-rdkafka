--TEST--
KafkaConsumer::splitPartitionQueue() while the partition is being fetched
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
$topic = $producer->newTopic($topicName);

function produce(RdKafka\Producer $producer, RdKafka\ProducerTopic $topic, string $payload): void
{
    $topic->produce(0, 0, $payload);

    if ($producer->flush(10000) !== RD_KAFKA_RESP_ERR_NO_ERROR) {
        throw new RuntimeException('Timed out producing the test message');
    }
}

function consumeRecord(callable $consume): RdKafka\Message
{
    $deadline = microtime(true) + 10;

    while (microtime(true) < $deadline) {
        $message = $consume();

        if ($message !== null && $message->err !== RD_KAFKA_RESP_ERR__PARTITION_EOF) {
            return $message;
        }
    }

    throw new RuntimeException('Timed out waiting for a record');
}

function waitForLength(RdKafka\Queue $queue, int $length): void
{
    $deadline = microtime(true) + 10;
    while ($queue->getLength() < $length) {
        if (microtime(true) > $deadline) {
            throw new RuntimeException("Timed out waiting for $length queued events");
        }

        usleep(10000);
    }
}

foreach (['message 0', 'message 1', 'message 2'] as $payload) {
    produce($producer, $topic, $payload);
}

$conf = new RdKafka\Conf();
$conf->set('metadata.broker.list', getenv('TEST_KAFKA_BROKERS'));
$conf->set('group.id', sprintf('test_rdkafka_group_%s', uniqid()));
$conf->set('enable.auto.commit', 'false');
$conf->set('log_level', '0');

$consumer = new RdKafka\KafkaConsumer($conf);
$consumer->assign([new RdKafka\TopicPartition($topicName, 0, RD_KAFKA_OFFSET_BEGINNING)]);

echo "Records fetched before splitting stay on the consumer queue\n";
echo consumeRecord(fn () => $consumer->consume(100))->payload, "\n";
waitForLength($consumer->getConsumerQueue(), 2);
$queue = $consumer->splitPartitionQueue($topicName, 0);
echo consumeRecord(fn () => $consumer->consume(100))->payload, "\n";
echo consumeRecord(fn () => $consumer->consume(100))->payload, "\n";

echo "Later records arrive on the partition queue\n";
produce($producer, $topic, 'message 3');
echo consumeRecord(fn () => $queue->consume(100))->payload, "\n";

echo "Releasing the partition queue keeps the partition split\n";
produce($producer, $topic, 'message 4');
waitForLength($queue, 1);
unset($queue);
produce($producer, $topic, 'message 5');
var_dump($consumer->consume(1000));
$queue = $consumer->splitPartitionQueue($topicName, 0);
echo consumeRecord(fn () => $queue->consume(100))->payload, "\n";
echo consumeRecord(fn () => $queue->consume(100))->payload, "\n";

echo "Records queued before unassignment are discarded\n";
produce($producer, $topic, 'message 6');
waitForLength($queue, 1);
$consumer->assign(null);
var_dump($queue->getLength() > 0);
var_dump($queue->consume(0));
var_dump($queue->getLength());

$consumer->close();

?>
--EXPECT--
Records fetched before splitting stay on the consumer queue
message 0
message 1
message 2
Later records arrive on the partition queue
message 3
Releasing the partition queue keeps the partition split
NULL
message 4
message 5
Records queued before unassignment are discarded
bool(true)
NULL
int(0)
