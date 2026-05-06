--TEST--
Admin - Event::get*Result throws when called on the wrong event type
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
$topic = sprintf("event-type-mismatch-%s", uniqid());

// Submit a CreateTopics so we get a known event type back.
$opts = $producer->newAdminOptions(RD_KAFKA_ADMIN_OP_CREATETOPICS);
$opts->setRequestTimeout(5000);
$producer->createTopics([new RdKafka\Admin\NewTopic($topic, 1, 1)], $queue, $opts);

$event = $queue->poll(10000);
printf("event type matches CREATETOPICS_RESULT: %s\n",
    $event->getType() === RD_KAFKA_EVENT_CREATETOPICS_RESULT ? 'true' : 'false');

// Right accessor — works.
$results = $event->getCreateTopicsResult();
printf("getCreateTopicsResult count: %d\n", count($results));

// Wrong accessor — should throw RdKafka\Exception.
try {
    $event->getDeleteTopicsResult();
    echo "FAIL: getDeleteTopicsResult did not throw\n";
} catch (RdKafka\Exception $e) {
    printf("getDeleteTopicsResult threw: %s\n", $e->getMessage());
}

// Another wrong accessor for good measure.
try {
    $event->getDeleteRecordsResult();
    echo "FAIL: getDeleteRecordsResult did not throw\n";
} catch (RdKafka\Exception $e) {
    printf("getDeleteRecordsResult threw: %s\n", $e->getMessage());
}

unset($event);

// Cleanup
$opts = $producer->newAdminOptions(RD_KAFKA_ADMIN_OP_DELETETOPICS);
$opts->setRequestTimeout(5000);
$producer->deleteTopics([new RdKafka\Admin\DeleteTopic($topic)], $queue, $opts);
$queue->poll(10000);

echo "OK\n";
--EXPECTF--
event type matches CREATETOPICS_RESULT: true
getCreateTopicsResult count: 1
getDeleteTopicsResult threw: Event is not a DeleteTopics result (got type 100 / CreateTopicsResult)
getDeleteRecordsResult threw: Event is not a DeleteRecords result (got type 100 / CreateTopicsResult)
OK
