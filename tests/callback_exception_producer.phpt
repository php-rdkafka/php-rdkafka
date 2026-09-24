--TEST--
A throwing producer callback leaves later delivery reports for the next poll
--FILE--
<?php

function produceAndPurge(RdKafka\Producer $producer, int $count): void
{
    $topic = $producer->newTopic('callback-exception');

    for ($i = 0; $i < $count; $i++) {
        $topic->produce(RD_KAFKA_PARTITION_UA, 0, "message $i", null, "opaque $i");
    }

    // Purging queued messages delivers their reports without a broker
    $producer->purge(RD_KAFKA_PURGE_F_QUEUE);
}

function pollExpectingException(RdKafka\Producer $producer): void
{
    try {
        $producer->poll(0);
        echo "No exception\n";
    } catch (RuntimeException $e) {
        echo 'Caught: ', $e->getMessage(), "\n";
    }
}

echo "Delivery report callback\n";
$conf = new RdKafka\Conf();
$conf->set('log_level', '0');

$reports = [];
$conf->setDrMsgCb(function (RdKafka\Producer $producer, RdKafka\Message $message) use (&$reports) {
    $reports[] = $message->opaque;

    if (count($reports) === 1) {
        throw new RuntimeException("delivery report for {$message->opaque}");
    }
});

$producer = new RdKafka\Producer($conf);
produceAndPurge($producer, 3);
pollExpectingException($producer);
var_dump($reports);
$producer->poll(0);
var_dump($reports);

echo "Error callback\n";
$conf = new RdKafka\Conf();
$conf->set('log_level', '0');
$conf->set('metadata.broker.list', '127.0.0.1:1');

$errors = 0;
$conf->setErrorCb(function () use (&$errors) {
    if (++$errors === 1) {
        throw new RuntimeException('error callback');
    }
});

$reports = [];
$conf->setDrMsgCb(function (RdKafka\Producer $producer, RdKafka\Message $message) use (&$reports) {
    $reports[] = $message->opaque;
});

$producer = new RdKafka\Producer($conf);

// Wait for the connection error to be queued ahead of the delivery report
$deadline = microtime(true) + 10;
while ($producer->getMainQueue()->getLength() === 0 && microtime(true) < $deadline) {
    usleep(10000);
}

produceAndPurge($producer, 1);
pollExpectingException($producer);
var_dump($reports);
$producer->poll(0);
var_dump($reports);

?>
--EXPECT--
Delivery report callback
Caught: delivery report for opaque 0
array(1) {
  [0]=>
  string(8) "opaque 0"
}
array(3) {
  [0]=>
  string(8) "opaque 0"
  [1]=>
  string(8) "opaque 1"
  [2]=>
  string(8) "opaque 2"
}
Error callback
Caught: error callback
array(0) {
}
array(1) {
  [0]=>
  string(8) "opaque 0"
}
