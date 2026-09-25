--TEST--
Pause and resume consumption
--SKIPIF--
<?php
require __DIR__ . '/helpers/integration-tests-check.php';
--FILE--
<?php
require __DIR__ . '/helpers/integration-tests-check.php';

$conf = new RdKafka\Conf();
$conf->set('metadata.broker.list', getenv('TEST_KAFKA_BROKERS'));
$conf->setDrMsgCb(function ($producer, $msg) {
    if ($msg->err) {
        throw new Exception("Message delivery failed: " . $msg->errstr());
    }
});

$producer = new RdKafka\Producer($conf);

$topicName = sprintf("test_rdkafka_%s", uniqid());
$topic = $producer->newTopic($topicName);

$topic->produce(0, 0, "message 0");
$topic->produce(0, 0, "message 1");

$result = $producer->flush(10000);
if (RD_KAFKA_RESP_ERR_NO_ERROR !== $result) {
    throw new Exception(rd_kafka_err2str($result), $result);
}

$conf = new RdKafka\Conf();
$conf->set('metadata.broker.list', getenv('TEST_KAFKA_BROKERS'));

$consumer = new RdKafka\Consumer($conf);

$topic = $consumer->newTopic($topicName);
$topic->consumeStart(0, RD_KAFKA_OFFSET_BEGINNING);

$topicPartitions = [
    new RdKafka\TopicPartition($topicName, 0),
];

var_dump($topic->consume(0, 10000)->payload);

// Pausing drops prefetched messages, so "message 1" is only fetched again after resuming
$consumer->pausePartitions($topicPartitions);
var_dump($topic->consume(0, 1000));

$consumer->resumePartitions($topicPartitions);
var_dump($topic->consume(0, 10000)->payload);
--EXPECT--
string(9) "message 0"
NULL
string(9) "message 1"
