--TEST--
Conf callbacks: offsetCommitCb and statsCb fire correctly
--SKIPIF--
<?php
require __DIR__ . '/helpers/integration-tests-check.php';
--FILE--
<?php
require __DIR__ . '/helpers/integration-tests-check.php';

$conf = new RdKafka\Conf();

$conf->set('metadata.broker.list', getenv('TEST_KAFKA_BROKERS'));

$producer = new RdKafka\Producer($conf);

$topicName = sprintf("test_rdkafka_%s", uniqid());
$topic = $producer->newTopic($topicName);

for ($i = 0; $i < 10; $i++) {
    $topic->produce(0, 0, "message $i");
    $producer->poll(0);
}

while ($producer->getOutQLen()) {
    $producer->poll(50);
}

$conf = new RdKafka\Conf();

$conf->set('auto.offset.reset', 'earliest');
$conf->set('metadata.broker.list', getenv('TEST_KAFKA_BROKERS'));
$conf->set('group.id', sprintf("test_rdkafka_group_%s", uniqid()));
$conf->set('statistics.interval.ms', 10);
$conf->set('enable.partition.eof', 'true');

$offsetsCommitted = 0;
$conf->setOffsetCommitCb(function ($consumer, $error, $topicPartitions) use (&$offsetsCommitted) {
    echo "Offset " . $topicPartitions[0]->getOffset() . " committed.\n";
    $offsetsCommitted++;
});

$statsCbCalled = false;
$conf->setStatsCb(function ($consumer, $json) use (&$statsCbCalled) {
    if ($statsCbCalled) {
        return;
    }

    $statsCbCalled = true;
});

$consumer = new RdKafka\KafkaConsumer($conf);
$consumer->subscribe([$topicName]);

$messagesConsumed = 0;
$deadline = microtime(true) + 40;

while (($messagesConsumed < 10 || $offsetsCommitted < 10 || !$statsCbCalled) && microtime(true) < $deadline) {
    $msg = $consumer->consume(100);

    if (!$msg || RD_KAFKA_RESP_ERR__PARTITION_EOF === $msg->err) {
        continue;
    }

    if (RD_KAFKA_RESP_ERR_NO_ERROR !== $msg->err) {
        throw new Exception($msg->errstr(), $msg->err);
    }

    $messagesConsumed++;
    $consumer->commit($msg);
}

var_dump($messagesConsumed);
var_dump($statsCbCalled);

--EXPECT--
Offset 1 committed.
Offset 2 committed.
Offset 3 committed.
Offset 4 committed.
Offset 5 committed.
Offset 6 committed.
Offset 7 committed.
Offset 8 committed.
Offset 9 committed.
Offset 10 committed.
int(10)
bool(true)
