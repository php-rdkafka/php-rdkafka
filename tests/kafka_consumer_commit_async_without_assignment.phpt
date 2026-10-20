--TEST--
KafkaConsumer::commitAsync() without an assignment reports an empty partition list
--FILE--
<?php
$conf = new RdKafka\Conf();
$conf->set('group.id', 'test_commit_without_assignment');
$conf->set('enable.auto.commit', 'false');
$conf->setLogCb(function () {});

$called = false;
$conf->setOffsetCommitCb(function ($consumer, $error, $partitions) use (&$called) {
    $called = true;
    var_dump($error === RD_KAFKA_RESP_ERR__NO_OFFSET);
    var_dump($partitions);
});

$consumer = new RdKafka\KafkaConsumer($conf);
$consumer->commitAsync();

$deadline = time() + 5;
while (!$called && time() < $deadline) {
    $consumer->consume(100);
}

var_dump($called);
--EXPECT--
bool(true)
array(0) {
}
bool(true)
