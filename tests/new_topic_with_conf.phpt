--TEST--
newTopic with topic conf
--SKIPIF--
<?php
require __DIR__ . '/helpers/integration-tests-check.php';
--FILE--
<?php
require __DIR__ . '/helpers/integration-tests-check.php';

$conf = new RdKafka\Conf();
$conf->set('metadata.broker.list', getenv('TEST_KAFKA_BROKERS'));

$topicName = "test";
$topicConf = new RdKafka\TopicConf();

$producer = new RdKafka\Producer($conf);
var_dump(get_class($producer->newtopic($topicName, $topicConf)));

$conf = new RdKafka\Conf();
$conf->set('metadata.broker.list', getenv('TEST_KAFKA_BROKERS'));
$conf->set('group.id', sprintf("test_rdkafka_group_%s", uniqid()));

$consumer = new RdKafka\Consumer($conf);
var_dump(get_class($consumer->newtopic($topicName, $topicConf)));

$kafkaConsumer = new RdKafka\KafkaConsumer($conf);
var_dump(get_class($kafkaConsumer->newtopic($topicName, $topicConf)));
--EXPECT--
string(21) "RdKafka\ProducerTopic"
string(21) "RdKafka\ConsumerTopic"
string(26) "RdKafka\KafkaConsumerTopic"
