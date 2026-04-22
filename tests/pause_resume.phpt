--TEST--
Pause and resume partitions
--SKIPIF--
<?php
require __DIR__ . '/helpers/integration-tests-check.php';
--FILE--
<?php
require __DIR__ . '/helpers/integration-tests-check.php';

$conf = new RdKafka\Conf();
$conf->set('metadata.broker.list', getenv('TEST_KAFKA_BROKERS'));

$producer = new RdKafka\Producer($conf);

$topicName = sprintf("test_rdkafka_%s", uniqid());
$topic = $producer->newTopic($topicName);

var_dump($producer->pausePartitions([
    new RdKafka\TopicPartition($topicName, 0),
]));
var_dump($producer->resumePartitions([
    new RdKafka\TopicPartition($topicName, 0),
]));

$conf = new RdKafka\Conf();
$conf->set('metadata.broker.list', getenv('TEST_KAFKA_BROKERS'));
$conf->set('group.id', sprintf("test_rdkafka_group_%s", uniqid()));

$consumer = new RdKafka\KafkaConsumer($conf);
$consumer->assign([
    new RdKafka\TopicPartition($topicName, 0),
]);

var_dump($consumer->pausePartitions([
    new RdKafka\TopicPartition($topicName, 0),
]));
var_dump($consumer->resumePartitions([
    new RdKafka\TopicPartition($topicName, 0),
]));
var_dump($consumer->resumePartitions([
    new RdKafka\TopicPartition("", -1),
]));
--EXPECTF--
array(1) {
  [0]=>
  object(RdKafka\TopicPartition)#5 (4) {
    ["topic"]=>
    string(26) "test_rdkafka_%s"
    ["partition"]=>
    int(0)
    ["offset"]=>
    int(0)
    ["err"]=>
    int(0)
  }
}
array(1) {
  [0]=>
  object(RdKafka\TopicPartition)#4 (4) {
    ["topic"]=>
    string(26) "test_rdkafka_%s"
    ["partition"]=>
    int(0)
    ["offset"]=>
    int(0)
    ["err"]=>
    int(0)
  }
}
array(1) {
  [0]=>
  object(RdKafka\TopicPartition)#6 (4) {
    ["topic"]=>
    string(26) "test_rdkafka_%s"
    ["partition"]=>
    int(0)
    ["offset"]=>
    int(0)
    ["err"]=>
    int(0)
  }
}
array(1) {
  [0]=>
  object(RdKafka\TopicPartition)#5 (4) {
    ["topic"]=>
    string(26) "test_rdkafka_%s"
    ["partition"]=>
    int(0)
    ["offset"]=>
    int(0)
    ["err"]=>
    int(0)
  }
}
array(1) {
  [0]=>
  object(RdKafka\TopicPartition)#6 (4) {
    ["topic"]=>
    string(0) ""
    ["partition"]=>
    int(-1)
    ["offset"]=>
    int(0)
    ["err"]=>
    int(-190)
  }
}
