--TEST--
Conf::setLogCb() logs are served by polling when log.queue is disabled
--FILE--
<?php

$facilities = [];

$conf = new RdKafka\Conf();
$conf->set('debug', 'generic');
$conf->setLogCb(function ($kafka, $level, $facility, $message) use (&$facilities) {
    $facilities[get_class($kafka)][] = $facility;
});
$conf->set('log.queue', 'false');

$producer = new RdKafka\Producer($conf);
$consumer = new RdKafka\Consumer($conf);
$conf->set('group.id', 'test-log-queue');
$kafkaConsumer = new RdKafka\KafkaConsumer($conf);

var_dump($facilities);

$producer->poll(0);
$consumer->poll(0);
$kafkaConsumer->consume(0);

var_dump(in_array('INIT', $facilities['RdKafka\Producer']));
var_dump(in_array('INIT', $facilities['RdKafka\Consumer']));
var_dump(in_array('INIT', $facilities['RdKafka\KafkaConsumer']));
--EXPECT--
array(0) {
}
bool(true)
bool(true)
bool(true)
