--TEST--
Produce, consume queue
--SKIPIF--
<?php
require __DIR__ . '/helpers/integration-tests-check.php';
--FILE--
<?php
require __DIR__ . '/helpers/integration-tests-check.php';

$delivered = 0;

$conf = new RdKafka\Conf();
$conf->setErrorCb(function ($producer, $err, $errstr) {
    printf("%s: %s\n", rd_kafka_err2str($err), $errstr);
    exit;
});
$conf->setDrMsgCb(function ($producer, $msg) use (&$delivered) {
    if ($msg->err) {
        throw new Exception("Message delivery failed: " . $msg->errstr());
    }
    $delivered++;
});
$conf->set('metadata.broker.list', getenv('TEST_KAFKA_BROKERS'));

$producer = new RdKafka\Producer($conf);

$topicNames = [
    sprintf("test_rdkafka_0_%s", uniqid()),
    sprintf("test_rdkafka_1_%s", uniqid()),
];

$topics = array_map(function ($topicName) use ($producer) {
    return $producer->newTopic($topicName);
}, $topicNames);

if (!$producer->getMetadata(false, reset($topics), 10*1000)) {
    echo "Failed to get metadata, is broker down?\n";
}

for ($i = 0; $i < 10; $i++) {
    $topics[$i%2]->produce(0, 0, "message $i");
    $producer->poll(0);
}

while ($producer->getOutQLen()) {
    $producer->poll(50);
}

printf("%d messages delivered\n", $delivered);

$conf = new RdKafka\Conf();
// Required to detect actual reaching of partition EOF for both topics
$conf->set('enable.partition.eof', 'true');
$conf->setErrorCb(function ($producer, $err, $errstr) {
    printf("%s: %s\n", rd_kafka_err2str($err), $errstr);
    exit;
});

$conf->set('metadata.broker.list', getenv('TEST_KAFKA_BROKERS'));

$consumer = new RdKafka\Consumer($conf);

$queue = $consumer->newQueue();

array_walk($topicNames, function ($topicName) use ($consumer, $queue) {
    $topic = $consumer->newTopic($topicName);
    $topic->consumeQueueStart(0, RD_KAFKA_OFFSET_BEGINNING, $queue);
});

$messages = [];
$receivedTopicEofs = [];

while (count($receivedTopicEofs) < 2) {
    $msg = $queue->consume(15000);
    if (!$msg) {
        // Still waiting for messages
        continue;
    }

    if (RD_KAFKA_RESP_ERR__PARTITION_EOF === $msg->err) {
        // Reached actual EOF
        $receivedTopicEofs[$msg->topic_name] = true;
        continue;
    }

    if (RD_KAFKA_RESP_ERR_NO_ERROR !== $msg->err) {
        throw new Exception($msg->errstr(), $msg->err);
    }

    $messages[] = sprintf("Got message: %s from %s", $msg->payload, $msg->topic_name);
}

sort($messages);
echo implode("\n", $messages), "\n";

--EXPECTF--
10 messages delivered
Got message: message 0 from test_rdkafka_0_%s
Got message: message 1 from test_rdkafka_1_%s
Got message: message 2 from test_rdkafka_0_%s
Got message: message 3 from test_rdkafka_1_%s
Got message: message 4 from test_rdkafka_0_%s
Got message: message 5 from test_rdkafka_1_%s
Got message: message 6 from test_rdkafka_0_%s
Got message: message 7 from test_rdkafka_1_%s
Got message: message 8 from test_rdkafka_0_%s
Got message: message 9 from test_rdkafka_1_%s
