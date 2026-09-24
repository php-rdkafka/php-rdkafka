--TEST--
RdKafka\Queue::getLength() reports queued events without serving them
--SKIPIF--
<?php
require __DIR__ . '/helpers/integration-tests-check.php';
--FILE--
<?php
require __DIR__ . '/helpers/integration-tests-check.php';

$topicName = sprintf('test_rdkafka_%s', uniqid());

function waitFor(callable $condition, string $description): void
{
    $deadline = microtime(true) + 10;
    while (!$condition()) {
        if (microtime(true) > $deadline) {
            throw new RuntimeException("Timed out waiting for $description");
        }

        usleep(10000);
    }
}

echo "Producer main queue\n";
$conf = new RdKafka\Conf();
$conf->set('metadata.broker.list', getenv('TEST_KAFKA_BROKERS'));

$reports = 0;
$conf->setDrMsgCb(function (RdKafka\Producer $producer, RdKafka\Message $message) use (&$reports) {
    if ($message->err !== RD_KAFKA_RESP_ERR_NO_ERROR) {
        throw new RuntimeException($message->errstr(), $message->err);
    }

    $reports++;
});

$producer = new RdKafka\Producer($conf);
$queue = $producer->getMainQueue();
$topic = $producer->newTopic($topicName);

for ($i = 0; $i < 3; $i++) {
    $topic->produce(0, 0, "message $i");
}

// Delivery reports wait in the main queue until poll() serves them
waitFor(fn () => $queue->getLength() > 0, 'delivery reports');
var_dump($reports);

waitFor(function () use ($producer, &$reports) {
    $producer->poll(0);

    return $reports === 3;
}, 'all delivery reports');
waitFor(function () use ($producer, $queue) {
    $producer->poll(0);

    return $queue->getLength() === 0;
}, 'an empty queue');
var_dump($reports);

echo "Consumer new queue\n";
$conf = new RdKafka\Conf();
$conf->set('metadata.broker.list', getenv('TEST_KAFKA_BROKERS'));
$conf->set('log_level', '0');

$consumer = new RdKafka\Consumer($conf);
$queue = $consumer->newQueue();
$topic = $consumer->newTopic($topicName);
$topic->consumeQueueStart(0, RD_KAFKA_OFFSET_BEGINNING, $queue);

waitFor(fn () => $queue->getLength() > 0, 'fetched messages');
echo $queue->consume(1000)->payload, "\n";

$topic->consumeStop(0);

?>
--EXPECT--
Producer main queue
int(0)
int(3)
Consumer new queue
message 0
