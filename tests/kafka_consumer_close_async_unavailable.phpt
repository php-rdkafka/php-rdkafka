--TEST--
KafkaConsumer::closeAsync() and isClosed() are not available before librdkafka 1.9.0
--SKIPIF--
<?php
if (RD_KAFKA_BUILD_VERSION >= 0x010900ff) {
    die('skip requires librdkafka < 1.9.0');
}
--FILE--
<?php

var_dump(method_exists(RdKafka\KafkaConsumer::class, 'closeAsync'));
var_dump(method_exists(RdKafka\KafkaConsumer::class, 'isClosed'));

?>
--EXPECT--
bool(false)
bool(false)
