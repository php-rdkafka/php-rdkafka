--TEST--
AdminClient - createPartitions integration
--SKIPIF--
<?php
require __DIR__ . '/integration-tests-check.php';
--FILE--
<?php
require __DIR__ . '/integration-tests-check.php';

$conf = new RdKafka\Conf();
$conf->set('metadata.broker.list', getenv('TEST_KAFKA_BROKERS'));

$admin = new RdKafka\Admin\AdminClient($conf);
$topicName = sprintf("test_rdkafka_%s", uniqid());

$newTopic = new RdKafka\Admin\NewTopic($topicName, 1, 1);
$results = $admin->createTopics([$newTopic]);
printf("createTopics error: %d\n", $results[0]->error);

sleep(1);

$newParts = new RdKafka\Admin\NewPartitions($topicName, 3);
$results = $admin->createPartitions([$newParts]);

printf("createPartitions count: %d\n", count($results));
printf("error: %d\n", $results[0]->error);
printf("name matches: %s\n", $results[0]->name === $topicName ? 'true' : 'false');

sleep(1);

$producer = new RdKafka\Producer($conf);
$topic = $producer->newTopic($topicName);
$metadata = $producer->getMetadata(false, $topic, 10*1000);
$topics = $metadata->getTopics();
foreach ($topics as $t) {
    if ($t->getTopic() === $topicName) {
        printf("partition count after increase: %d\n", count($t->getPartitions()));
    }
}

$admin->deleteTopics([new RdKafka\Admin\DeleteTopic($topicName)]);

echo "OK\n";
--EXPECT--
createTopics error: 0
createPartitions count: 1
error: 0
name matches: true
partition count after increase: 3
OK
