--TEST--
KafkaConsumer::consume() returns null when no message is available within timeout
--SKIPIF--
<?php
require __DIR__ . '/helpers/integration-tests-check.php';
--FILE--
<?php
require __DIR__ . '/helpers/integration-tests-check.php';

$topic = sprintf('test_rdkafka_%s', uniqid());

// Seed the topic so it exists on the broker
$conf = new RdKafka\Conf();
$conf->set('metadata.broker.list', getenv('TEST_KAFKA_BROKERS'));
$conf->setLogCb(function () {});
$conf->setDrMsgCb(function ($producer, $msg) {
    if ($msg->err) {
        throw new Exception("Seed delivery failed: " . $msg->errstr());
    }
});
$producer = new RdKafka\Producer($conf);
$producer->newTopic($topic)->produce(RD_KAFKA_PARTITION_UA, 0, 'seed');
$result = $producer->flush(5000);
if (RD_KAFKA_RESP_ERR_NO_ERROR !== $result) {
    throw new Exception(rd_kafka_err2str($result), $result);
}
unset($producer);

// Subscribe at latest — no new messages, so consume() should return null
$conf = new RdKafka\Conf();
$conf->set('group.id', sprintf('test_rdkafka_%s', uniqid()));
$conf->set('metadata.broker.list', getenv('TEST_KAFKA_BROKERS'));
$conf->set('auto.offset.reset', 'latest');
$conf->setLogCb(function () {});

$consumer = new RdKafka\KafkaConsumer($conf);
$consumer->subscribe([$topic]);

$msg = $consumer->consume(3000);
var_dump($msg);

$consumer->close();
--EXPECT--
NULL
