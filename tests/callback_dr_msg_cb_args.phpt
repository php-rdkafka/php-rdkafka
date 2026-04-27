--TEST--
Conf::setDrMsgCb fires with correct Message arguments
--SKIPIF--
<?php
require __DIR__ . '/helpers/integration-tests-check.php';
--FILE--
<?php
require __DIR__ . '/helpers/integration-tests-check.php';

$deliveries = [];

$conf = new RdKafka\Conf();
$conf->set('metadata.broker.list', getenv('TEST_KAFKA_BROKERS'));
$conf->setLogCb(function () {});
$conf->setDrMsgCb(function ($producer, $msg) use (&$deliveries) {
    $deliveries[] = [
        'producer_type' => get_class($producer),
        'msg_type'      => get_class($msg),
        'err'           => $msg->err,
        'topic'         => $msg->topic_name,
        'payload'       => $msg->payload,
    ];
});

$producer = new RdKafka\Producer($conf);
$topicName = sprintf("test_rdkafka_%s", uniqid());
$topic = $producer->newTopic($topicName);

$topic->produce(RD_KAFKA_PARTITION_UA, 0, 'hello-dr-msg');
$producer->flush(10000);

var_dump(count($deliveries) === 1);
var_dump($deliveries[0]['producer_type']);
var_dump($deliveries[0]['msg_type']);
var_dump($deliveries[0]['err'] === RD_KAFKA_RESP_ERR_NO_ERROR);
var_dump($deliveries[0]['topic']);
var_dump($deliveries[0]['payload']);
--EXPECTF--
bool(true)
string(16) "RdKafka\Producer"
string(15) "RdKafka\Message"
bool(true)
string(%d) "test_rdkafka_%s"
string(12) "hello-dr-msg"
