--TEST--
AdminClient - createTopics and deleteTopics integration
--SKIPIF--
<?php
require __DIR__ . '/helpers/integration-tests-check.php';
--FILE--
<?php
require __DIR__ . '/helpers/integration-tests-check.php';

$conf = new RdKafka\Conf();
$conf->set('metadata.broker.list', getenv('TEST_KAFKA_BROKERS'));

$admin = new RdKafka\Admin\AdminClient($conf);
$topicName = sprintf("test_rdkafka_%s", uniqid());

$newTopic = new RdKafka\Admin\NewTopic($topicName, 1, 1);
$results = $admin->createTopics([$newTopic]);

printf("createTopics count: %d\n", count($results));
printf("class: %s\n", get_class($results[0]));
printf("error: %d\n", $results[0]->error);
printf("error (getter): %d\n", $results[0]->getError());
printf("error_string: %s\n", var_export($results[0]->error_string, true));
printf("error_string (getter): %s\n", var_export($results[0]->getErrorString(), true));
printf("name matches: %s\n", $results[0]->name === $topicName ? 'true' : 'false');
printf("name (getter) matches: %s\n", $results[0]->getName() === $topicName ? 'true' : 'false');

$deleteTopic = new RdKafka\Admin\DeleteTopic($topicName);
$results = $admin->deleteTopics([$deleteTopic]);

printf("deleteTopics count: %d\n", count($results));
printf("error: %d\n", $results[0]->error);
printf("name matches: %s\n", $results[0]->name === $topicName ? 'true' : 'false');

echo "OK\n";
--EXPECT--
createTopics count: 1
class: RdKafka\Admin\TopicResult
error: 0
error (getter): 0
error_string: NULL
error_string (getter): NULL
name matches: true
name (getter) matches: true
deleteTopics count: 1
error: 0
name matches: true
OK
