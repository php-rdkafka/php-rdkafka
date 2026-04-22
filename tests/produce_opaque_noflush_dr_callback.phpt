--TEST--
Produce with opaque, no flush, with delivery callback
--SKIPIF--
<?php
require __DIR__ . '/helpers/integration-tests-check.php';
?>
--FILE--
<?php
require __DIR__ . '/helpers/integration-tests-check.php';

$conf = new RdKafka\Conf();
$conf->set('metadata.broker.list', getenv('TEST_KAFKA_BROKERS'));

$conf->setDrMsgCb(function ($rdkafka, $msg) {
    var_dump($rdkafka, $msg);
});

$producer = new RdKafka\Producer($conf);

$topicName = sprintf("test_rdkafka_%s", uniqid());

$topic = $producer->newTopic($topicName);

if (!$producer->getMetadata(false, $topic, 10*1000)) {
    echo "Failed to get metadata, is broker down?\n";
}

for ($i = 0; $i < 10; $i++) {
    $topic->produce(0, 0, "message $i", null, "opaque $i");
}

echo "Expect no leaks\n";
--EXPECT--
Expect no leaks
