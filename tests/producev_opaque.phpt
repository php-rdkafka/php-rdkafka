--TEST--
Producev with opaque
--SKIPIF--
<?php
require __DIR__ . '/helpers/integration-tests-check.php';
?>
--FILE--
<?php
require __DIR__ . '/helpers/integration-tests-check.php';

$conf = new RdKafka\Conf();
$conf->set('metadata.broker.list', getenv('TEST_KAFKA_BROKERS'));

$opaques = [];
$conf->setDrMsgCb(function ($producer, $msg) use (&$opaques) {
    $opaques[] = $msg->opaque;
});

$producer = new RdKafka\Producer($conf);

$topicName = sprintf("test_rdkafka_%s", uniqid());

$topic = $producer->newTopic($topicName);

if (!$producer->getMetadata(false, $topic, 10*1000)) {
    echo "Failed to get metadata, is broker down?\n";
}

for ($i = 0; $i < 10; $i++) {
    $topic->producev(0, 0, "message $i", null, [], 0, "opaque $i");
}

$producer->flush(10*1000);

var_dump($opaques);
--EXPECT--
array(10) {
  [0]=>
  string(8) "opaque 0"
  [1]=>
  string(8) "opaque 1"
  [2]=>
  string(8) "opaque 2"
  [3]=>
  string(8) "opaque 3"
  [4]=>
  string(8) "opaque 4"
  [5]=>
  string(8) "opaque 5"
  [6]=>
  string(8) "opaque 6"
  [7]=>
  string(8) "opaque 7"
  [8]=>
  string(8) "opaque 8"
  [9]=>
  string(8) "opaque 9"
}
