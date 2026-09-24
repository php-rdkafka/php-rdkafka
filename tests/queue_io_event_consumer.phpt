--TEST--
Partition and consumer queue notifications wake the queue that has work
--SKIPIF--
<?php
require __DIR__ . '/helpers/integration-tests-check.php';
if (!method_exists(RdKafka\Queue::class, 'ioEventEnable')) {
    die('skip RdKafka\Queue::ioEventEnable() is not available on this platform');
}
--FILE--
<?php
require __DIR__ . '/helpers/integration-tests-check.php';

function notificationPair(): array
{
    [$read, $write] = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
    stream_set_blocking($read, false);
    stream_set_blocking($write, false);

    return [$read, $write];
}

/*
 * Serves only the queues whose stream is readable, as an event loop would,
 * until $done returns true. Returns the records each queue returned.
 */
function serveNotifiedQueues(array $queues, array $streams, callable $done): array
{
    $records = [];
    $deadline = microtime(true) + 10;

    while (!$done($records)) {
        if (microtime(true) > $deadline) {
            throw new RuntimeException('Timed out waiting for notifications: ' . json_encode($records));
        }

        $readable = $streams;
        $writable = [];
        $exceptional = [];
        stream_select($readable, $writable, $exceptional, 0, 100000);

        foreach ($readable as $name => $stream) {
            fread($stream, 1024);

            while (($message = $queues[$name]->consume(0)) !== null) {
                if ($message->err === RD_KAFKA_RESP_ERR_NO_ERROR) {
                    $records[$name][] = $message->payload;
                } elseif ($message->err !== RD_KAFKA_RESP_ERR__PARTITION_EOF) {
                    throw new RuntimeException($message->errstr(), $message->err);
                }
            }
        }
    }

    return $records;
}

$splitTopic = sprintf('test_rdkafka_%s', uniqid());
$otherTopic = sprintf('test_rdkafka_%s', uniqid());

$conf = new RdKafka\Conf();
$conf->set('metadata.broker.list', getenv('TEST_KAFKA_BROKERS'));
$producer = new RdKafka\Producer($conf);

$produce = function (string $topicName, string $payload) use ($producer): void {
    $producer->newTopic($topicName)->produce(0, 0, $payload);

    if ($producer->flush(10000) !== RD_KAFKA_RESP_ERR_NO_ERROR) {
        throw new RuntimeException("Timed out producing to $topicName");
    }
};

$produce($splitTopic, 'split record');
$produce($otherTopic, 'other record');

$conf = new RdKafka\Conf();
$conf->set('metadata.broker.list', getenv('TEST_KAFKA_BROKERS'));
$conf->set('group.id', sprintf('test_rdkafka_group_%s', uniqid()));
$conf->set('enable.auto.commit', 'false');
$conf->set('log_level', '0');

$commits = 0;
$conf->setOffsetCommitCb(function () use (&$commits) {
    $commits++;
});

$consumer = new RdKafka\KafkaConsumer($conf);
$queues = [
    'partition queue' => $consumer->splitPartitionQueue($splitTopic, 0),
    'consumer queue' => $consumer->getConsumerQueue(),
];

$streams = [];
foreach ($queues as $name => $queue) {
    [$streams[$name], $write] = notificationPair();
    $queue->ioEventEnable($write);
}

echo "Records wake their own queue\n";
$consumer->assign([
    new RdKafka\TopicPartition($splitTopic, 0, RD_KAFKA_OFFSET_BEGINNING),
    new RdKafka\TopicPartition($otherTopic, 0, RD_KAFKA_OFFSET_BEGINNING),
]);
$records = serveNotifiedQueues($queues, $streams, fn ($records) => count($records, COUNT_RECURSIVE) - count($records) === 2);
ksort($records);
var_dump($records);

echo "A callback wakes the consumer queue\n";
$consumer->commitAsync([new RdKafka\TopicPartition($otherTopic, 0, 1)]);
serveNotifiedQueues($queues, $streams, function () use (&$commits) {
    return $commits === 1;
});
var_dump($commits);

echo "A partial service pass leaves work that needs no new notification\n";
$produce($splitTopic, 'first');
$produce($splitTopic, 'second');
$deadline = microtime(true) + 10;
while ($queues['partition queue']->getLength() < 2 && microtime(true) < $deadline) {
    usleep(10000);
}
echo $queues['partition queue']->consume(0)->payload, "\n";
var_dump($queues['partition queue']->getLength() > 0);
echo $queues['partition queue']->consume(0)->payload, "\n";

echo "A stale record wakes the partition queue without a record to return\n";
foreach ($streams as $stream) {
    fread($stream, 1024);
}
$produce($splitTopic, 'stale');
$deadline = microtime(true) + 10;
while ($queues['partition queue']->getLength() === 0 && microtime(true) < $deadline) {
    usleep(10000);
}
$consumer->assign(null);
$readable = $streams;
$writable = [];
$exceptional = [];
stream_select($readable, $writable, $exceptional, 0);
var_dump(isset($readable['partition queue']));
var_dump($queues['partition queue']->consume(0));

foreach ($queues as $queue) {
    $queue->ioEventEnable(null);
}
unset($queues, $queue);
$consumer->close();

?>
--EXPECT--
Records wake their own queue
array(2) {
  ["consumer queue"]=>
  array(1) {
    [0]=>
    string(12) "other record"
  }
  ["partition queue"]=>
  array(1) {
    [0]=>
    string(12) "split record"
  }
}
A callback wakes the consumer queue
int(1)
A partial service pass leaves work that needs no new notification
first
bool(true)
second
A stale record wakes the partition queue without a record to return
bool(true)
NULL
