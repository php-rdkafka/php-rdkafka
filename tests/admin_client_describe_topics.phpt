--TEST--
AdminClient - describeTopics integration
--SKIPIF--
<?php
require __DIR__ . '/integration-tests-check.php';
if (!method_exists(RdKafka\Admin\AdminClient::class, 'describeTopics')) {
    die('skip describeTopics not available (requires librdkafka >= 2.3.0)');
}
--FILE--
<?php
require __DIR__ . '/integration-tests-check.php';

$conf = new RdKafka\Conf();
$conf->set('metadata.broker.list', getenv('TEST_KAFKA_BROKERS'));

$admin = new RdKafka\Admin\AdminClient($conf);
$topicName = sprintf("test_rdkafka_%s", uniqid());

$newTopic = new RdKafka\Admin\NewTopic($topicName, 2, 1);
$admin->createTopics([$newTopic]);

sleep(1);

$descriptions = $admin->describeTopics([$topicName]);

printf("result count: %d\n", count($descriptions));

$desc = $descriptions[0];
printf("class: %s\n", get_class($desc));
printf("name matches: %s\n", $desc->name === $topicName ? 'true' : 'false');
printf("is_internal: %s\n", $desc->is_internal ? 'true' : 'false');
printf("error: %d\n", $desc->error);
printf("topic_id is string: %s\n", is_string($desc->topic_id) ? 'true' : 'false');
printf("partition count: %d\n", count($desc->partitions));

$part = $desc->partitions[0];
printf("partition class: %s\n", get_class($part));
printf("partition id: %d\n", $part->partition);

printf("leader class: %s\n", get_class($part->leader));
printf("leader has valid id: %s\n", is_int($part->leader->id) ? 'true' : 'false');
printf("leader has host: %s\n", is_string($part->leader->host) && strlen($part->leader->host) > 0 ? 'true' : 'false');
printf("leader has port: %s\n", is_int($part->leader->port) && $part->leader->port > 0 ? 'true' : 'false');

printf("isr count >= 1: %s\n", count($part->isr) >= 1 ? 'true' : 'false');
printf("isr[0] class: %s\n", get_class($part->isr[0]));
printf("replicas count >= 1: %s\n", count($part->replicas) >= 1 ? 'true' : 'false');
printf("replicas[0] class: %s\n", get_class($part->replicas[0]));

$admin->deleteTopics([new RdKafka\Admin\DeleteTopic($topicName)]);

echo "OK\n";
--EXPECT--
result count: 1
class: RdKafka\Admin\TopicDescription
name matches: true
is_internal: false
error: 0
topic_id is string: true
partition count: 2
partition class: RdKafka\Admin\TopicPartitionInfo
partition id: 0
leader class: RdKafka\Admin\Node
leader has valid id: true
leader has host: true
leader has port: true
isr count >= 1: true
isr[0] class: RdKafka\Admin\Node
replicas count >= 1: true
replicas[0] class: RdKafka\Admin\Node
OK
