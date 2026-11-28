--TEST--
Message timestamp is available for null payloads
--SKIPIF--
<?php
require __DIR__ . '/helpers/integration-tests-check.php';
--FILE--
<?php
require __DIR__ . '/helpers/integration-tests-check.php';

$timestamp = PHP_INT_SIZE >= 8 ? (int) (microtime(true) * 1000) : 1700000000;
$timestamps = [$timestamp, $timestamp + 456];
$topicName = sprintf('test_rdkafka_%s', uniqid());

$conf = new RdKafka\Conf();
$conf->set('metadata.broker.list', getenv('TEST_KAFKA_BROKERS'));
$delivered = 0;
$conf->setDrMsgCb(function ($producer, $message) use (&$delivered, $timestamps) {
    if ($message->err) {
        throw new Exception($message->errstr(), $message->err);
    }

    var_dump($message->payload);
    var_dump($message->timestamp === $timestamps[$delivered]);
    $delivered++;
});

$producer = new RdKafka\Producer($conf);
$topic = $producer->newTopic($topicName);

if (!$producer->getMetadata(false, $topic, 10 * 1000)) {
    echo "Failed to get metadata, is broker down?\n";
}

foreach ($timestamps as $timestamp) {
    $topic->producev(0, 0, null, null, [], $timestamp);
}
$producer->flush(10 * 1000);

$conf = new RdKafka\Conf();
$conf->set('metadata.broker.list', getenv('TEST_KAFKA_BROKERS'));

$consumer = new RdKafka\Consumer($conf);
$topic = $consumer->newTopic($topicName);
$topic->consumeStart(0, RD_KAFKA_OFFSET_BEGINNING);

$consumed = 0;
while ($consumed < count($timestamps)) {
    $message = $topic->consume(0, 1000);
    if ($message === null) {
        continue;
    }

    if (RD_KAFKA_RESP_ERR_NO_ERROR !== $message->err) {
        throw new Exception($message->errstr(), $message->err);
    }

    var_dump($message->payload);
    var_dump($message->timestamp === $timestamps[$consumed]);
    $consumed++;
}

$topic->consumeStop(0);
--EXPECT--
NULL
bool(true)
NULL
bool(true)
NULL
bool(true)
NULL
bool(true)
