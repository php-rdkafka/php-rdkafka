--TEST--
A throwing consumer callback stops consumption before the next message
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

for ($i = 0; $i < 3; $i++) {
    $topic->produce(0, 0, "message $i");
}

if ($producer->flush(10000) !== RD_KAFKA_RESP_ERR_NO_ERROR) {
    throw new RuntimeException('Timed out producing the test messages');
}

function createConf(): RdKafka\Conf
{
    $conf = new RdKafka\Conf();
    $conf->set('metadata.broker.list', getenv('TEST_KAFKA_BROKERS'));
    $conf->set('group.id', sprintf('test_rdkafka_group_%s', uniqid()));
    $conf->set('auto.offset.reset', 'earliest');
    $conf->set('enable.auto.commit', 'false');
    $conf->set('enable.partition.eof', 'true');
    $conf->set('log_level', '0');

    return $conf;
}

function consumeUntilEof(RdKafka\KafkaConsumer $consumer): array
{
    $payloads = [];
    $deadline = microtime(true) + 30;

    while (microtime(true) < $deadline) {
        $message = $consumer->consume(1000);

        if ($message === null) {
            continue;
        }

        if ($message->err === RD_KAFKA_RESP_ERR__PARTITION_EOF) {
            return $payloads;
        }

        if ($message->err !== RD_KAFKA_RESP_ERR_NO_ERROR) {
            throw new RuntimeException($message->errstr(), $message->err);
        }

        $payloads[] = $message->payload;
    }

    throw new RuntimeException('Timed out waiting for partition EOF');
}

foreach ([
    'KafkaConsumer::consume()' => fn (RdKafka\KafkaConsumer $consumer) => $consumer->consume(10000),
    'Queue::consume()' => fn (RdKafka\KafkaConsumer $consumer) => $consumer->getConsumerQueue()->consume(10000),
] as $name => $consume) {
    echo "Rebalance callback during $name\n";

    $conf = createConf();
    $thrown = false;
    $conf->setRebalanceCb(function (RdKafka\KafkaConsumer $consumer, int $err, array $partitions) use (&$thrown) {
        if ($err !== RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS) {
            $consumer->assign(null);
            return;
        }

        $consumer->assign($partitions);

        if (!$thrown) {
            $thrown = true;
            throw new RuntimeException('rebalance callback');
        }
    });

    $consumer = new RdKafka\KafkaConsumer($conf);
    $consumer->subscribe([$topicName]);

    $deadline = microtime(true) + 30;
    while (!$thrown && microtime(true) < $deadline) {
        try {
            $consume($consumer);
        } catch (RuntimeException $e) {
            echo 'Caught: ', $e->getMessage(), "\n";
        }
    }

    // The message fetched after the assignment was not handed out while the
    // exception was pending
    echo implode(', ', consumeUntilEof($consumer)), "\n";
    $consumer->close();
}

echo "Consume callback\n";
$consumer = new RdKafka\Consumer(createConf());
$topic = $consumer->newTopic($topicName);
$topic->consumeStart(0, RD_KAFKA_OFFSET_BEGINNING);

$thrown = false;
$deadline = microtime(true) + 30;
while (!$thrown && microtime(true) < $deadline) {
    try {
        $topic->consumeCallback(0, 1000, function (RdKafka\Message $message) {
            throw new RuntimeException("consume callback for {$message->payload}");
        });
    } catch (RuntimeException $e) {
        $thrown = true;
        echo 'Caught: ', $e->getMessage(), "\n";
    }
}

for ($i = 0; $i < 2; $i++) {
    $message = $topic->consume(0, 10000);
    echo $message ? $message->payload : 'No message', "\n";
}

$topic->consumeStop(0);

?>
--EXPECT--
Rebalance callback during KafkaConsumer::consume()
Caught: rebalance callback
message 0, message 1, message 2
Rebalance callback during Queue::consume()
Caught: rebalance callback
message 0, message 1, message 2
Consume callback
Caught: consume callback for message 0
message 1
message 2
