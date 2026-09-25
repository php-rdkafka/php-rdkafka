--TEST--
RdKafka\Conf::getDefaultTopicConf() returns null when no topic properties set, a TopicConf copy otherwise
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

// The returned TopicConf is a copy, so changes via Conf::set() are not reflected
$conf->set('auto.offset.reset', 'latest');
var_dump($topicConf->get('auto.offset.reset'));
var_dump($conf->getDefaultTopicConf()->get('auto.offset.reset'));

// Changes to the copy do not affect the Conf
$topicConf->set('auto.offset.reset', 'error');
var_dump($conf->getDefaultTopicConf()->get('auto.offset.reset'));

// The copy remains usable after the Conf's default topic conf is replaced
$conf->setDefaultTopicConf(new RdKafka\TopicConf());
var_dump($topicConf->get('auto.offset.reset'));

// The copy remains usable after the Conf is destroyed
$conf->set('auto.offset.reset', 'earliest');
$topicConf = $conf->getDefaultTopicConf();
unset($conf);
var_dump($topicConf->get('auto.offset.reset'));
--EXPECTF--
NULL
bool(true)
string(8) "smallest"
bool(true)
string(8) "smallest"
string(7) "largest"
string(7) "largest"

Deprecated: Method RdKafka\Conf::setDefaultTopicConf() is deprecated in %s on line %d
string(5) "error"
string(8) "smallest"
