--TEST--
Admin - createTopics and deleteTopics integration (queue/event API)
--SKIPIF--
<?php
require __DIR__ . '/helpers/integration-tests-check.php';
--FILE--
<?php
require __DIR__ . '/helpers/integration-tests-check.php';

$conf = new RdKafka\Conf();
$conf->set('metadata.broker.list', getenv('TEST_KAFKA_BROKERS'));

$producer = new RdKafka\Producer($conf);
$queue = $producer->newQueue();
$topicName = sprintf("test_rdkafka_%s", uniqid());

// CREATE
$opts = $producer->newAdminOptions(RD_KAFKA_ADMIN_OP_CREATETOPICS);
$opts->setRequestTimeout(5000);
$producer->createTopics([new RdKafka\Admin\NewTopic($topicName, 1, 1)], $queue, $opts);
$event = $queue->poll(10000);
printf("createTopics event type: %d (%s)\n", $event->getType(), $event->getName());

$results = $event->getCreateTopicsResult();
printf("createTopics count: %d\n", count($results));
printf("class: %s\n", get_class($results[0]));
printf("error: %d\n", $results[0]->error);
printf("error (getter): %d\n", $results[0]->getError());
printf("error_string: %s\n", var_export($results[0]->error_string, true));
printf("error_string (getter): %s\n", var_export($results[0]->getErrorString(), true));
printf("name matches: %s\n", $results[0]->name === $topicName ? 'true' : 'false');
printf("name (getter) matches: %s\n", $results[0]->getName() === $topicName ? 'true' : 'false');
unset($event);

// DELETE
$opts = $producer->newAdminOptions(RD_KAFKA_ADMIN_OP_DELETETOPICS);
$opts->setRequestTimeout(5000);
$producer->deleteTopics([new RdKafka\Admin\DeleteTopic($topicName)], $queue, $opts);
$event = $queue->poll(10000);
$results = $event->getDeleteTopicsResult();

printf("deleteTopics count: %d\n", count($results));
printf("error: %d\n", $results[0]->error);
printf("name matches: %s\n", $results[0]->name === $topicName ? 'true' : 'false');

echo "OK\n";
--EXPECTF--
createTopics event type: 100 (CreateTopicsResult)
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
