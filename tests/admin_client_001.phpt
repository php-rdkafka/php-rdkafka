--TEST--
AdminClient - basic object creation and configuration
--FILE--
<?php
$conf = new RdKafka\Conf();
$conf->set('bootstrap.servers', 'localhost:9092');
$admin = new RdKafka\Admin\AdminClient($conf);
var_dump($admin instanceof RdKafka\Admin\AdminClient);

$topic = new RdKafka\Admin\NewTopic("test", 1, 1);
var_dump($topic instanceof RdKafka\Admin\NewTopic);

$topic->setConfig("cleanup.policy", "compact");

$delTopic = new RdKafka\Admin\DeleteTopic("test");
var_dump($delTopic instanceof RdKafka\Admin\DeleteTopic);

$newParts = new RdKafka\Admin\NewPartitions("test", 3);
var_dump($newParts instanceof RdKafka\Admin\NewPartitions);

$options = $admin->newAdminOptions(RD_KAFKA_ADMIN_OP_CREATETOPICS);
$options->setRequestTimeout(5000);
$options->setOperationTimeout(10000);
$options->setValidateOnly(true);
var_dump($options instanceof RdKafka\Admin\AdminOptions);

echo "OK\n";
--EXPECT--
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
OK
