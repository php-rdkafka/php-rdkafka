--TEST--
Produce, consume
--SKIPIF--
<?php
require __DIR__ . '/helpers/integration-tests-check.php';
if (!class_exists("RdKafka\\KafkaErrorException")) {
    echo "skip";
}
--FILE--
<?php
require __DIR__ . '/helpers/integration-tests-check.php';

$delivered = 0;

$conf = new RdKafka\Conf();
$conf->set('transactional.id', 'transactional-producer');
$conf->setLogCb(function ($kafka, $level, $facility, $message) {});
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

$producer->initTransactions(10000);
$producer->beginTransaction();

$topicName = sprintf("test_rdkafka_%s", uniqid());

$topic = $producer->newTopic($topicName);

if (!$producer->getMetadata(false, $topic, 10*1000)) {
    echo "Failed to get metadata, is broker down?\n";
}

for ($i = 0; $i < 10; $i++) {
    $topic->produce(0, 0, "message $i");
    $producer->poll(0);
}

while ($producer->getOutQLen()) {
    $producer->poll(50);
}

$producer->commitTransaction(10000);

printf("%d messages delivered\n", $delivered);

$conf = new RdKafka\Conf();
$conf->set('metadata.broker.list', getenv('TEST_KAFKA_BROKERS'));
$conf->setErrorCb(function ($producer, $err, $errstr) {
    printf("%s: %s\n", rd_kafka_err2str($err), $errstr);
    exit;
});

$consumer = new RdKafka\Consumer($conf);

$topic = $consumer->newTopic($topicName);
$topic->consumeStart(0, RD_KAFKA_OFFSET_BEGINNING);

$messages = [];

while (true) {
    $msg = $topic->consume(0, 1000);
    // librdkafka before 1.0 returns message with RD_KAFKA_RESP_ERR__PARTITION_EOF when reaching topic end.
    if (!$msg || $msg->err === RD_KAFKA_RESP_ERR__PARTITION_EOF) {
        break;
    }

    if (RD_KAFKA_RESP_ERR_NO_ERROR !== $msg->err) {
        throw new Exception($msg->errstr(), $msg->err);
    }

    printf("Got message: %s\n", $msg->payload);
}
--EXPECT--
10 messages delivered
Got message: message 0
Got message: message 1
Got message: message 2
Got message: message 3
Got message: message 4
Got message: message 5
Got message: message 6
Got message: message 7
Got message: message 8
Got message: message 9
