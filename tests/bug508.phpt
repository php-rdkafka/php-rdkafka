--TEST--
Bug 508
--SKIPIF--
<?php
require __DIR__ . '/helpers/integration-tests-check.php';
--FILE--
<?php
require __DIR__ . '/helpers/integration-tests-check.php';

$topicName = sprintf("test_rdkafka_%s", uniqid());

$conf = new RdKafka\Conf();
$conf->set('metadata.broker.list', getenv('TEST_KAFKA_BROKERS'));
$conf->setDrMsgCb(function ($producer, $msg) use (&$delivered) {
    if ($msg->err) {
        throw new Exception("Message delivery failed: " . $msg->errstr());
    }
    $delivered++;
});

$producer = new RdKafka\Producer($conf);
$topic = $producer->newTopic($topicName);

if (!$producer->getMetadata(false, $topic, 10*1000)) {
    echo "Failed to get metadata, is broker down?\n";
}

$topic->produce(0, 0, "message");

while ($producer->getOutQLen()) {
    $producer->poll(50);
}

printf("%d messages delivered\n", $delivered);

$conf = new RdKafka\Conf();
$conf->set('metadata.broker.list', getenv('TEST_KAFKA_BROKERS'));
$conf->set('enable.partition.eof', 'true');

$consumer = new RdKafka\Consumer($conf);
$topic = $consumer->newTopic($topicName);
$topic->consumeStart(0, RD_KAFKA_OFFSET_BEGINNING);

while (true) {
    $msg = $topic->consume(0, 1000);
    if (!$msg) {
        continue;
    }
    // All props are initialized and readable in all cases
    var_dump([
        'err' => $msg->err,
        'topic_name' => $msg->topic_name,
        'timestamp' => $msg->timestamp,
        'partition' => $msg->partition,
        'payload' => $msg->payload,
        'len' => $msg->len,
        'key' => $msg->key,
        'offset' => $msg->offset,
        'headers' => $msg->headers,
        'opaque' => $msg->opaque,
    ]);
    echo "--------------\n";
    if ($msg->err === RD_KAFKA_RESP_ERR__PARTITION_EOF) {
        echo "EOF\n";
        break;
    }
}
--EXPECTF--
1 messages delivered
array(10) {
  ["err"]=>
  int(0)
  ["topic_name"]=>
  string(%d) "test_rdkafka_%s"
  ["timestamp"]=>
  int(%f)
  ["partition"]=>
  int(0)
  ["payload"]=>
  string(7) "message"
  ["len"]=>
  int(7)
  ["key"]=>
  NULL
  ["offset"]=>
  int(0)
  ["headers"]=>
  array(0) {
  }
  ["opaque"]=>
  NULL
}
--------------
array(10) {
  ["err"]=>
  int(-%d)
  ["topic_name"]=>
  string(%d) "test_rdkafka_%s"
  ["timestamp"]=>
  int(-1)
  ["partition"]=>
  int(0)
  ["payload"]=>
  string(%d) "%s"
  ["len"]=>
  int(%d)
  ["key"]=>
  NULL
  ["offset"]=>
  int(1)
  ["headers"]=>
  array(0) {
  }
  ["opaque"]=>
  NULL
}
--------------
EOF
