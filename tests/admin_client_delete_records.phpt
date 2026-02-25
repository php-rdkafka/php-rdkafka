--TEST--
AdminClient - deleteRecords integration
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

$producerConf = new RdKafka\Conf();
$producerConf->set('metadata.broker.list', getenv('TEST_KAFKA_BROKERS'));
$producerConf->setDrMsgCb(function ($producer, $msg) {
    if ($msg->err) {
        throw new Exception("Message delivery failed: " . $msg->errstr());
    }
});

$producer = new RdKafka\Producer($producerConf);
$topic = $producer->newTopic($topicName);

for ($i = 0; $i < 10; $i++) {
    $topic->produce(0, 0, "message $i");
    $producer->poll(0);
}

while ($producer->getOutQLen()) {
    $producer->poll(50);
}

sleep(1);

$topicPartition = new RdKafka\TopicPartition($topicName, 0, 5);
$results = $admin->deleteRecords([$topicPartition]);

printf("deleteRecords count: %d\n", count($results));
printf("class: %s\n", get_class($results[0]));
printf("topic matches: %s\n", $results[0]->getTopic() === $topicName ? 'true' : 'false');
printf("partition: %d\n", $results[0]->getPartition());
printf("offset: %d\n", $results[0]->getOffset());

$admin->deleteTopics([new RdKafka\Admin\DeleteTopic($topicName)]);

echo "OK\n";
--EXPECT--
createTopics error: 0
deleteRecords count: 1
class: RdKafka\TopicPartition
topic matches: true
partition: 0
offset: 5
OK
