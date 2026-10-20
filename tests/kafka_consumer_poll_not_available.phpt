--TEST--
KafkaConsumer::poll() is not available
--FILE--
<?php
var_dump(method_exists(RdKafka\KafkaConsumer::class, 'poll'));
--EXPECT--
bool(false)
