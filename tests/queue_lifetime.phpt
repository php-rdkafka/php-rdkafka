--TEST--
Client queues are shared per queue, keep their client alive and are invalidated on close
--FILE--
<?php

function createConf(): RdKafka\Conf
{
    $conf = new RdKafka\Conf();
    $conf->set('group.id', 'queue-lifetime');

    // Silences librdkafka's missing-broker warning.
    $conf->set('log_level', '0');

    return $conf;
}

function expectException(callable $callback): void
{
    try {
        $callback();
        echo "No exception\n";
    } catch (Throwable $e) {
        echo get_class($e), ': ', $e->getMessage(), "\n";
    }
}

function isCollected(WeakReference $reference): bool
{
    gc_collect_cycles();

    return $reference->get() === null;
}

echo "Shared queues\n";
$producer = new RdKafka\Producer(createConf());
var_dump($producer->getMainQueue() === $producer->getMainQueue());

$consumer = new RdKafka\Consumer(createConf());
var_dump($consumer->getMainQueue() === $consumer->getMainQueue());
var_dump($consumer->newQueue() === $consumer->newQueue());

$kafkaConsumer = new RdKafka\KafkaConsumer(createConf());
var_dump($kafkaConsumer->getConsumerQueue() === $kafkaConsumer->getConsumerQueue());

$partitionQueue = $kafkaConsumer->splitPartitionQueue('queue-lifetime', 0);
var_dump($partitionQueue === $kafkaConsumer->splitPartitionQueue('queue-lifetime', 0));
var_dump($partitionQueue === $kafkaConsumer->splitPartitionQueue('queue-lifetime', 1));
var_dump($partitionQueue === $kafkaConsumer->splitPartitionQueue('queue-lifetime-2', 0));
var_dump($partitionQueue === $kafkaConsumer->getConsumerQueue());

$reference = WeakReference::create($partitionQueue);
unset($partitionQueue);
var_dump($reference->get() === null);

echo "Fresh queue length\n";
var_dump($consumer->newQueue()->getLength());

echo "Queue keeps its client alive\n";
foreach ([
    'producer main queue' => [fn () => new RdKafka\Producer(createConf()), fn ($client) => $client->getMainQueue()],
    'consumer main queue' => [fn () => new RdKafka\Consumer(createConf()), fn ($client) => $client->getMainQueue()],
    'consumer new queue' => [fn () => new RdKafka\Consumer(createConf()), fn ($client) => $client->newQueue()],
    'KafkaConsumer consumer queue' => [fn () => new RdKafka\KafkaConsumer(createConf()), fn ($client) => $client->getConsumerQueue()],
    'KafkaConsumer partition queue' => [fn () => new RdKafka\KafkaConsumer(createConf()), fn ($client) => $client->splitPartitionQueue('queue-lifetime', 0)],
] as $name => [$createClient, $getQueue]) {
    $client = $createClient();
    $queue = $getQueue($client);
    $reference = WeakReference::create($client);
    unset($client);
    $keptAlive = $reference->get() !== null;
    unset($queue);
    echo $name, ': ', var_export($keptAlive && $reference->get() === null, true), "\n";
}

echo "Queues are invalidated on close\n";
$kafkaConsumer = new RdKafka\KafkaConsumer(createConf());
$consumerQueue = $kafkaConsumer->getConsumerQueue();
$partitionQueue = $kafkaConsumer->splitPartitionQueue('queue-lifetime', 0);
$alias = $partitionQueue;
$kafkaConsumer->close();
expectException(fn () => $consumerQueue->getLength());
expectException(fn () => $alias->consume(0));
unset($consumerQueue, $partitionQueue, $alias, $kafkaConsumer);

echo "Queue cycles can be collected\n";
foreach ([
    'producer main queue' => fn (RdKafka\Conf $conf) => (new RdKafka\Producer($conf))->getMainQueue(),
    'KafkaConsumer consumer queue' => fn (RdKafka\Conf $conf) => (new RdKafka\KafkaConsumer($conf))->getConsumerQueue(),
    'KafkaConsumer partition queue' => fn (RdKafka\Conf $conf) => (new RdKafka\KafkaConsumer($conf))->splitPartitionQueue('queue-lifetime', 0),
] as $name => $createQueue) {
    $capture = new stdClass();
    $conf = createConf();
    $conf->setErrorCb(function () use ($capture) { });
    $capture->queue = $createQueue($conf);
    $reference = WeakReference::create($capture->queue);
    unset($capture, $conf);
    echo $name, ': ', var_export(isCollected($reference), true), "\n";
}

echo "Invalid partition queue arguments\n";
$kafkaConsumer = new RdKafka\KafkaConsumer(createConf());
expectException(fn () => $kafkaConsumer->splitPartitionQueue("queue-lifetime\0suffix", 0));
expectException(fn () => $kafkaConsumer->splitPartitionQueue('queue-lifetime', -1));
expectException(fn () => $kafkaConsumer->splitPartitionQueue(str_repeat('t', 600), 0));

echo "Only new queues can receive a legacy topic's messages\n";
$topic = $consumer->newTopic('queue-lifetime');
expectException(fn () => $topic->consumeQueueStart(0, RD_KAFKA_OFFSET_BEGINNING, $consumer->getMainQueue()));
expectException(fn () => $topic->consumeQueueStart(0, RD_KAFKA_OFFSET_BEGINNING, $kafkaConsumer->getConsumerQueue()));
expectException(fn () => $topic->consumeQueueStart(0, RD_KAFKA_OFFSET_BEGINNING, $kafkaConsumer->splitPartitionQueue('queue-lifetime', 0)));

?>
--EXPECTF--
Shared queues
bool(true)
bool(true)
bool(false)
bool(true)
bool(true)
bool(false)
bool(false)
bool(false)
bool(true)
Fresh queue length
int(0)
Queue keeps its client alive
producer main queue: true
consumer main queue: true
consumer new queue: true
KafkaConsumer consumer queue: true
KafkaConsumer partition queue: true
Queues are invalidated on close
Exception: RdKafka\Queue is not initialized or its client has been closed
Exception: RdKafka\Queue is not initialized or its client has been closed
Queue cycles can be collected
producer main queue: true
KafkaConsumer consumer queue: true
KafkaConsumer partition queue: true
Invalid partition queue arguments
ValueError: RdKafka\KafkaConsumer::splitPartitionQueue(): Argument #1 ($topic) must not contain any null bytes
InvalidArgumentException: Out of range value '-1' for $partition
RdKafka\Exception: %s
Only new queues can receive a legacy topic's messages
InvalidArgumentException: RdKafka\ConsumerTopic::consumeQueueStart() requires a queue created by RdKafka\Consumer::newQueue()
InvalidArgumentException: RdKafka\ConsumerTopic::consumeQueueStart() requires a queue created by RdKafka\Consumer::newQueue()
InvalidArgumentException: RdKafka\ConsumerTopic::consumeQueueStart() requires a queue created by RdKafka\Consumer::newQueue()
