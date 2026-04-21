--TEST--
Bug 74
--SKIPIF--
<?php
require __DIR__ . '/helpers/integration-tests-check.php';
--FILE--
<?php
require __DIR__ . '/helpers/integration-tests-check.php';

$conf = new RdKafka\Conf();
$conf->set('metadata.broker.list', getenv('TEST_KAFKA_BROKERS'));

$consumer = new RdKafka\Consumer($conf);
$topic = $consumer->newTopic("batman", null);

$producer = new RdKafka\Producer($conf);

if (class_exists('RdKafka\TopicPartition')) {
    $tp = new RdKafka\TopicPartition("batman", 0);
}
--EXPECT--
