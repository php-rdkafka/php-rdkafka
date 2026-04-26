--TEST--
RdKafka\Conf::dump includes topic-level properties
--SKIPIF--
<?php
if (!extension_loaded('rdkafka')) die('skip rdkafka extension not loaded');
--FILE--
<?php
$conf = new RdKafka\Conf();

// global-level property
$conf->set('socket.timeout.ms', '60000');

// topic-level property (stored in the embedded default_topic_conf)
$conf->set('auto.offset.reset', 'earliest');

$dump = $conf->dump();

var_dump(isset($dump['socket.timeout.ms']));
var_dump(isset($dump['auto.offset.reset']));

// TopicConf::dump() should still work independently
$topicConf = new RdKafka\TopicConf();
$topicConf->set('auto.offset.reset', 'latest');
$topicDump = $topicConf->dump();
var_dump(isset($topicDump['auto.offset.reset']));
--EXPECT--
bool(true)
bool(true)
bool(true)
