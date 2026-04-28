--TEST--
RdKafka\Conf::get() and RdKafka\TopicConf::get() retrieve configuration values
--SKIPIF--
<?php
if (!extension_loaded('rdkafka')) die('skip rdkafka extension not loaded');
--FILE--
<?php
$conf = new RdKafka\Conf();
$conf->set('group.id', 'test-group');
var_dump($conf->get('group.id'));

$conf->set('message.max.bytes', '2000000');
var_dump($conf->get('message.max.bytes'));

// Unknown property throws
try {
    $conf->get('does.not.exist');
} catch (RdKafka\Exception $e) {
    echo "Exception: " . $e->getMessage() . "\n";
}

$topicConf = new RdKafka\TopicConf();
$topicConf->set('request.timeout.ms', '5678');
var_dump($topicConf->get('request.timeout.ms'));

// Unknown topic property throws
try {
    $topicConf->get('does.not.exist');
} catch (RdKafka\Exception $e) {
    echo "Exception: " . $e->getMessage() . "\n";
}
--EXPECT--
string(10) "test-group"
string(7) "2000000"
Exception: Unknown configuration property "does.not.exist"
string(4) "5678"
Exception: Unknown configuration property "does.not.exist"
