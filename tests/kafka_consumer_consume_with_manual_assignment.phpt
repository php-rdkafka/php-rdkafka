--TEST--
KafkaConsumer::consume() returns manually assigned records
--SKIPIF--
<?php
require __DIR__ . '/helpers/integration-tests-check.php';
--FILE--
<?php
require __DIR__ . '/helpers/integration-tests-check.php';

$conf = new RdKafka\Conf();
$conf->set('metadata.broker.list', getenv('TEST_KAFKA_BROKERS'));
$conf->setLogCb(function () {});

$producer = new RdKafka\Producer($conf);
$topicName = sprintf('test_rdkafka_%s', uniqid());
$topic = $producer->newTopic($topicName);

for ($i = 0; $i < 3; $i++) {
    $topic->produce(0, 0, "message $i");
}

if (RD_KAFKA_RESP_ERR_NO_ERROR !== $producer->flush(10000)) {
    throw new Exception('Unable to seed records');
}

$conf = new RdKafka\Conf();
$conf->set('metadata.broker.list', getenv('TEST_KAFKA_BROKERS'));
$conf->set('group.id', sprintf('test_rdkafka_group_%s', uniqid()));
$conf->set('enable.auto.commit', 'false');
$conf->set('enable.auto.offset.store', 'false');
$conf->setLogCb(function () {});

$consumer = new RdKafka\KafkaConsumer($conf);
$consumer->assign([
    new RdKafka\TopicPartition($topicName, 0, RD_KAFKA_OFFSET_BEGINNING),
]);

$messages = [];
$deadline = microtime(true) + 40;

while (count($messages) < 3 && microtime(true) < $deadline) {
    $message = $consumer->consume(1000);

    if ($message === null) {
        continue;
    }

    if (RD_KAFKA_RESP_ERR_NO_ERROR !== $message->err) {
        throw new Exception($message->errstr(), $message->err);
    }

    $messages[] = $message->payload;
}

var_dump($messages);

$consumer->close();
--EXPECT--
array(3) {
  [0]=>
  string(9) "message 0"
  [1]=>
  string(9) "message 1"
  [2]=>
  string(9) "message 2"
}
