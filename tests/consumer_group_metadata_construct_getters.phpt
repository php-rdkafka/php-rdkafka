--TEST--
RdKafka\ConsumerGroupMetadata constructor and getter methods
--SKIPIF--
<?php
require __DIR__ . '/helpers/integration-tests-check.php';
if (!defined('RD_KAFKA_CONSUMER_GROUP_METADATA_GETTERS')) {
    die('skip ConsumerGroupMetadata getters not available in this librdkafka build');
}
--FILE--
<?php
require __DIR__ . '/helpers/integration-tests-check.php';

// Simple constructor (group_id only)
$meta = new RdKafka\ConsumerGroupMetadata('my-group');
var_dump($meta->getGroupId());
var_dump($meta->getMemberId());
var_dump($meta->getGroupInstanceId());
var_dump($meta->getGenerationId());
--EXPECT--
string(8) "my-group"
string(0) ""
NULL
int(-1)
