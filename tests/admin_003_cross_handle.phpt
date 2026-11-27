--TEST--
Admin methods reject Queue and AdminOptions from a different RdKafka handle
--FILE--
<?php
$conf = new RdKafka\Conf();
$conf->set('metadata.broker.list', 'localhost:9092');
$conf->setLogCb(function () {});

$a = new RdKafka\Producer($conf);
$b = new RdKafka\Producer($conf);

$queueB = $b->newQueue();
$optsB = $b->newAdminOptions(RD_KAFKA_ADMIN_OP_CREATETOPICS);
$topic = new RdKafka\Admin\NewTopic("test", 1, 1);

try {
    $a->createTopics([$topic], $queueB);
    echo "FAIL: foreign Queue did not throw\n";
} catch (RdKafka\Exception $e) {
    echo $e->getMessage() . "\n";
}

$queueA = $a->newQueue();
try {
    $a->createTopics([$topic], $queueA, $optsB);
    echo "FAIL: foreign AdminOptions did not throw\n";
} catch (RdKafka\Exception $e) {
    echo $e->getMessage() . "\n";
}

echo "OK\n";
--EXPECT--
Queue was created from a different RdKafka handle
AdminOptions was created from a different RdKafka handle
OK
