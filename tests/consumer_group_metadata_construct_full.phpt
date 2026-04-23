--TEST--
RdKafka\ConsumerGroupMetadata full constructor with generation_id, member_id, group_instance_id
--SKIPIF--
<?php
require __DIR__ . '/helpers/integration-tests-check.php';
if (!method_exists('RdKafka\ConsumerGroupMetadata', 'getGroupId')) {
    die('skip requires librdkafka >= 2.0.0');
}
--FILE--
<?php
require __DIR__ . '/helpers/integration-tests-check.php';

$meta = new RdKafka\ConsumerGroupMetadata('my-group', 5, 'member-abc', 'instance-xyz');
var_dump($meta->getGroupId());
var_dump($meta->getMemberId());
var_dump($meta->getGroupInstanceId());
var_dump($meta->getGenerationId());

// null group_instance_id
$meta2 = new RdKafka\ConsumerGroupMetadata('my-group', 3, 'member-123');
var_dump($meta2->getGroupInstanceId());
--EXPECT--
string(8) "my-group"
string(10) "member-abc"
string(12) "instance-xyz"
int(5)
NULL
