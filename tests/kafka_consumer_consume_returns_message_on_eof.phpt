--TEST--
KafkaConsumer::consume() returns a Message (not null) on partition EOF
--SKIPIF--
<?php
require __DIR__ . '/helpers/integration-tests-check.php';
--FILE--
<?php
require __DIR__ . '/helpers/integration-tests-check.php';

$topic = sprintf('test_rdkafka_%s', uniqid());

// Seed one message
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

// Consume from earliest with partition EOF enabled
$conf = new RdKafka\Conf();
$conf->set('group.id', sprintf('test_rdkafka_%s', uniqid()));
$conf->set('metadata.broker.list', getenv('TEST_KAFKA_BROKERS'));
$conf->set('auto.offset.reset', 'earliest');
$conf->set('enable.partition.eof', 'true');
$conf->setLogCb(function () {});

$consumer = new RdKafka\KafkaConsumer($conf);
$consumer->subscribe([$topic]);

// Consume the seeded message
while (true) {
    $msg = $consumer->consume(5000);
    if ($msg === null) {
        continue;
    }
    if ($msg->err === RD_KAFKA_RESP_ERR_NO_ERROR) {
        break;
    }
}

// Next consume should hit EOF — must be a Message, not null
$eof = $consumer->consume(5000);
var_dump($eof instanceof RdKafka\Message);
var_dump($eof->err === RD_KAFKA_RESP_ERR__PARTITION_EOF);

$consumer->close();
--EXPECT--
bool(true)
bool(true)
