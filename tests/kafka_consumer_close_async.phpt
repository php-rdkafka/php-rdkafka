--TEST--
KafkaConsumer::closeAsync() closes the consumer while consume() serves it
--SKIPIF--
<?php
if (!method_exists(RdKafka\KafkaConsumer::class, 'closeAsync')) {
    die('skip requires librdkafka >= 1.9.0');
}
--FILE--
<?php

function createConsumer(): RdKafka\KafkaConsumer
{
    $conf = new RdKafka\Conf();
    $conf->set('group.id', 'close-async');

    // Silences librdkafka's missing-broker warning.
    $conf->set('log_level', '0');

    return new RdKafka\KafkaConsumer($conf);
}

function waitUntilClosed(RdKafka\KafkaConsumer $consumer): void
{
    $deadline = microtime(true) + 10;
    while (!$consumer->isClosed() && microtime(true) < $deadline) {
        $consumer->consume(10);
    }
}

function expectException(callable $callback): void
{
    try {
        $callback();
        echo "No exception\n";
    } catch (Exception $e) {
        echo get_class($e), ': ', $e->getMessage(), "\n";
    }
}

echo "Closing asynchronously\n";
$consumer = createConsumer();
var_dump($consumer->isClosed());
$consumer->closeAsync();
$consumer->closeAsync();
waitUntilClosed($consumer);
var_dump($consumer->isClosed());

echo "The closed group is reported\n";
expectException(fn () => $consumer->getRebalanceProtocol());

echo "Final close after the consumer closed\n";
$consumer->close();
expectException(fn () => $consumer->isClosed());

echo "Final close while the consumer is closing\n";
$consumer = createConsumer();
$consumer->closeAsync();
$consumer->close();
expectException(fn () => $consumer->closeAsync());

echo "Destruction after the consumer closed\n";
$consumer = createConsumer();
$consumer->closeAsync();
waitUntilClosed($consumer);
unset($consumer);

echo "Destruction while the consumer is closing\n";
$consumer = createConsumer();
$consumer->closeAsync();
unset($consumer);

echo "Done\n";

?>
--EXPECT--
Closing asynchronously
bool(false)
bool(true)
The closed group is reported
RdKafka\Exception: The consumer group has been closed
Final close after the consumer closed
Exception: RdKafka\KafkaConsumer::__construct() has not been called, or RdKafka\KafkaConsumer::close() was already called
Final close while the consumer is closing
Exception: RdKafka\KafkaConsumer::__construct() has not been called, or RdKafka\KafkaConsumer::close() was already called
Destruction after the consumer closed
Destruction while the consumer is closing
Done
