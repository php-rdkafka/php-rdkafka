--TEST--
Admin DTOs - basic object creation and configuration without a broker
--FILE--
<?php
$conf = new RdKafka\Conf();
$conf->set('metadata.broker.list', 'localhost:9092');
$conf->setLogCb(function () {});

$producer = new RdKafka\Producer($conf);
var_dump($producer instanceof RdKafka\Producer);

$topic = new RdKafka\Admin\NewTopic("test", 1, 1);
var_dump($topic instanceof RdKafka\Admin\NewTopic);

$topic->setConfig("cleanup.policy", "compact");

$delTopic = new RdKafka\Admin\DeleteTopic("test");
var_dump($delTopic instanceof RdKafka\Admin\DeleteTopic);

$newParts = new RdKafka\Admin\NewPartitions("test", 3);
var_dump($newParts instanceof RdKafka\Admin\NewPartitions);

$options = $producer->newAdminOptions(RD_KAFKA_ADMIN_OP_CREATETOPICS);
$options->setRequestTimeout(5000);
$options->setOperationTimeout(10000);
$options->setValidateOnly(true);
var_dump($options instanceof RdKafka\Admin\AdminOptions);

$queue = $producer->newQueue();
var_dump($queue instanceof RdKafka\Queue);

echo "OK\n";
--EXPECT--
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
OK
