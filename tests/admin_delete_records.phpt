--TEST--
Admin - deleteRecords integration (queue/event API)
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

// CREATE topic
$opts = $producer->newAdminOptions(RD_KAFKA_ADMIN_OP_CREATETOPICS);
$opts->setRequestTimeout(5000);
$producer->createTopics([new RdKafka\Admin\NewTopic($topicName, 1, 1)], $queue, $opts);
$event = $queue->poll(10000);
$results = $event->getCreateTopicsResult();
printf("createTopics error: %d\n", $results[0]->error);
unset($event);

// PRODUCE 10 messages
$producerConf = new RdKafka\Conf();
$producerConf->set('metadata.broker.list', getenv('TEST_KAFKA_BROKERS'));
$producerConf->setDrMsgCb(function ($producer, $msg) {
    if ($msg->err) {
        throw new Exception("Message delivery failed: " . $msg->errstr());
    }
});

$writer = new RdKafka\Producer($producerConf);
$topic = $writer->newTopic($topicName);

for ($i = 0; $i < 10; $i++) {
    $topic->produce(0, 0, "message $i");
    $writer->poll(0);
}

$writer->flush(10 * 1000);

// DELETE RECORDS up to offset 5
$opts = $producer->newAdminOptions(RD_KAFKA_ADMIN_OP_DELETERECORDS);
$opts->setRequestTimeout(10000);
$producer->deleteRecords([new RdKafka\TopicPartition($topicName, 0, 5)], $queue, $opts);
$event = $queue->poll(15000);
$results = $event->getDeleteRecordsResult();

printf("deleteRecords count: %d\n", count($results));
printf("class: %s\n", get_class($results[0]));
printf("topic matches: %s\n", $results[0]->getTopic() === $topicName ? 'true' : 'false');
printf("partition: %d\n", $results[0]->getPartition());
printf("offset: %d\n", $results[0]->getOffset());
unset($event);

// CLEANUP
$opts = $producer->newAdminOptions(RD_KAFKA_ADMIN_OP_DELETETOPICS);
$opts->setRequestTimeout(5000);
$producer->deleteTopics([new RdKafka\Admin\DeleteTopic($topicName)], $queue, $opts);
$queue->poll(10000);

echo "OK\n";
--EXPECT--
createTopics error: 0
deleteRecords count: 1
class: RdKafka\TopicPartition
topic matches: true
partition: 0
offset: 5
OK
