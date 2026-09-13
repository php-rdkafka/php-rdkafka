--TEST--
Queue::poll returns null on timeout, RdKafka\Event class shape
--FILE--
<?php
$conf = new RdKafka\Conf();
$conf->set('metadata.broker.list', 'localhost:9092');
$conf->setLogCb(function () {});

$producer = new RdKafka\Producer($conf);
$queue = $producer->newQueue();

// Empty queue, very short timeout — should be null
$start = microtime(true);
$event = $queue->poll(50);
$elapsed = (microtime(true) - $start) * 1000;
var_dump($event === null);
var_dump($elapsed < 200); // should not block beyond timeout (with some slack)

// Class introspection
var_dump(class_exists(RdKafka\Event::class));
$ref = new ReflectionClass(RdKafka\Event::class);
$ctor = $ref->getConstructor();
var_dump($ctor !== null && $ctor->isPrivate());

$expectedMethods = [
    'getType', 'getName', 'getError', 'getErrorString',
    'getCreateTopicsResult', 'getDeleteTopicsResult', 'getCreatePartitionsResult',
    'getDeleteRecordsResult',
];
foreach ($expectedMethods as $m) {
    if (!$ref->hasMethod($m)) {
        echo "missing method: $m\n";
    }
}

// A few of the constants we expose
var_dump(defined('RD_KAFKA_EVENT_NONE'));
var_dump(defined('RD_KAFKA_EVENT_CREATETOPICS_RESULT'));
var_dump(defined('RD_KAFKA_EVENT_DELETETOPICS_RESULT'));

echo "OK\n";
--EXPECT--
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
OK
