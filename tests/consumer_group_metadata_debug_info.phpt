--TEST--
RdKafka\ConsumerGroupMetadata var_dump shows group metadata fields
--SKIPIF--
<?php
require __DIR__ . '/helpers/integration-tests-check.php';
if (!defined('RD_KAFKA_CONSUMER_GROUP_METADATA_GETTERS')) {
    die('skip ConsumerGroupMetadata getters not available in this librdkafka build');
}
--FILE--
<?php
require __DIR__ . '/helpers/integration-tests-check.php';

$conf = new RdKafka\Conf();
$conf->set('metadata.broker.list', getenv('TEST_KAFKA_BROKERS'));
$conf->set('group.id', 'test-consumer-group-metadata');
$conf->setLogCb(function () {});

$consumer = new RdKafka\KafkaConsumer($conf);
$metadata = $consumer->getConsumerGroupMetadata();

var_dump($metadata);
--EXPECTF--
object(RdKafka\ConsumerGroupMetadata)#%d (4) {
  ["group_id"]=>
  string(28) "test-consumer-group-metadata"
  ["member_id"]=>
  string(0) ""
  ["group_instance_id"]=>
  NULL
  ["generation_id"]=>
  int(-1)
}
