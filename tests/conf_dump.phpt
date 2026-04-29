--TEST--
RdKafka\Conf::dump returns global properties only; topic-level properties via getDefaultTopicConf()
--SKIPIF--
<?php
if (!extension_loaded('rdkafka')) die('skip rdkafka extension not loaded');
--FILE--
<?php
$conf = new RdKafka\Conf();
$conf->set('group.id', 'test-group');         // global
$conf->set('auto.offset.reset', 'earliest'); // topic-level

$dump = $conf->dump();

// global property appears in dump()
var_dump(isset($dump['group.id']));

// topic-level property does NOT appear in dump()
var_dump(isset($dump['auto.offset.reset']));

// topic-level property is accessible via getDefaultTopicConf()
$topicConf = $conf->getDefaultTopicConf();
var_dump($topicConf instanceof RdKafka\TopicConf);
$topicDump = $topicConf->dump();
var_dump(isset($topicDump['auto.offset.reset']));

// TopicConf::dump() still works independently
$standalone = new RdKafka\TopicConf();
$standalone->set('auto.offset.reset', 'latest');
$standaloneDump = $standalone->dump();
var_dump(isset($standaloneDump['auto.offset.reset']));
--EXPECT--
bool(true)
bool(false)
bool(true)
bool(true)
bool(true)
