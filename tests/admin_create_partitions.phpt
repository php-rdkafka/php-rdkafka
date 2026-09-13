--TEST--
Admin - createPartitions integration (queue/event API)
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

// CREATE topic with 1 partition
$opts = $producer->newAdminOptions(RD_KAFKA_ADMIN_OP_CREATETOPICS);
$opts->setRequestTimeout(5000);
$producer->createTopics([new RdKafka\Admin\NewTopic($topicName, 1, 1)], $queue, $opts);
$event = $queue->poll(10000);
$results = $event->getCreateTopicsResult();
printf("createTopics error: %d\n", $results[0]->error);
unset($event);

sleep(1);

// EXPAND to 3 partitions
$opts = $producer->newAdminOptions(RD_KAFKA_ADMIN_OP_CREATEPARTITIONS);
$opts->setRequestTimeout(5000);
$producer->createPartitions([new RdKafka\Admin\NewPartitions($topicName, 3)], $queue, $opts);
$event = $queue->poll(10000);
$results = $event->getCreatePartitionsResult();

printf("createPartitions count: %d\n", count($results));
printf("error: %d\n", $results[0]->error);
printf("name matches: %s\n", $results[0]->name === $topicName ? 'true' : 'false');
unset($event);

sleep(1);

// Verify via metadata
$topic = $producer->newTopic($topicName);
$metadata = $producer->getMetadata(false, $topic, 10*1000);
foreach ($metadata->getTopics() as $t) {
    if ($t->getTopic() === $topicName) {
        printf("partition count after increase: %d\n", count($t->getPartitions()));
    }
}

// CLEANUP
$opts = $producer->newAdminOptions(RD_KAFKA_ADMIN_OP_DELETETOPICS);
$opts->setRequestTimeout(5000);
$producer->deleteTopics([new RdKafka\Admin\DeleteTopic($topicName)], $queue, $opts);
$queue->poll(10000);

echo "OK\n";
--EXPECT--
createTopics error: 0
createPartitions count: 1
error: 0
name matches: true
partition count after increase: 3
OK
