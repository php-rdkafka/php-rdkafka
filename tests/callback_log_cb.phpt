--TEST--
Conf::setLogCb fires with correct arguments
--SKIPIF--
<?php
if (!extension_loaded('rdkafka')) die('skip rdkafka extension not loaded');
--FILE--
<?php
$conf = new RdKafka\Conf();
$conf->set('metadata.broker.list', 'localhost:9999');
$conf->set('debug', 'generic');

$calls = [];
$conf->setLogCb(function ($kafka, $level, $facility, $message) use (&$calls) {
    $calls[] = [
        'kafka_type'    => get_class($kafka),
        'level_is_int'  => is_int($level),
        'facility_type' => gettype($facility),
        'message_type'  => gettype($message),
    ];
});

$producer = new RdKafka\Producer($conf);
$producer->poll(0);

var_dump(count($calls) > 0);
var_dump($calls[0]['kafka_type']);
var_dump($calls[0]['level_is_int']);
var_dump($calls[0]['facility_type']);
var_dump($calls[0]['message_type']);
--EXPECT--
bool(true)
string(16) "RdKafka\Producer"
bool(true)
string(6) "string"
string(6) "string"
