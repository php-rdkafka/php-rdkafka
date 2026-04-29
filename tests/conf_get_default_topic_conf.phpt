--TEST--
RdKafka\Conf::getDefaultTopicConf() returns null when no topic properties set, TopicConf otherwise
--SKIPIF--
<?php
if (!extension_loaded('rdkafka')) die('skip rdkafka extension not loaded');
--FILE--
<?php
// Returns null when no topic-level properties have been set
$conf = new RdKafka\Conf();
var_dump($conf->getDefaultTopicConf());

// Returns a TopicConf once a topic-level property is set
$conf->set('auto.offset.reset', 'earliest');
$topicConf = $conf->getDefaultTopicConf();
var_dump($topicConf instanceof RdKafka\TopicConf);

// get() works on the returned TopicConf
var_dump($topicConf->get('auto.offset.reset'));

// dump() works on the returned TopicConf
$dump = $topicConf->dump();
var_dump(isset($dump['auto.offset.reset']));

// The returned TopicConf is a live view — changes via Conf::set() are reflected
$conf->set('auto.offset.reset', 'latest');
var_dump($topicConf->get('auto.offset.reset'));
--EXPECT--
NULL
bool(true)
string(8) "smallest"
bool(true)
string(7) "largest"
