--TEST--
TopicPartition get/set metadata
--SKIPIF--
<?php
if (!extension_loaded('rdkafka')) die('skip rdkafka extension not loaded');
--FILE--
<?php
$tp = new RdKafka\TopicPartition('test-topic', 0, 100);

// default is null
var_dump($tp->getMetadata());

// set a string value
$result = $tp->setMetadata('my-metadata');
var_dump($result === $tp); // fluent interface

var_dump($tp->getMetadata());

// set binary metadata
$tp->setMetadata("\x00\x01\x02\x03");
var_dump(strlen($tp->getMetadata()));
var_dump($tp->getMetadata() === "\x00\x01\x02\x03");

// clear with null
$tp->setMetadata(null);
var_dump($tp->getMetadata());
--EXPECT--
NULL
bool(true)
string(11) "my-metadata"
int(4)
bool(true)
NULL
