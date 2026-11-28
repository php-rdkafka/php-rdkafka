--TEST--
ProducerTopic::producev() does not enqueue messages when header conversion throws
--FILE--
<?php

class ThrowingHeader
{
    public function __toString(): string
    {
        throw new RuntimeException('Conversion failed');
    }
}

$conf = new RdKafka\Conf();
// Keep broker log events out of the queue-length assertions.
$conf->set('log_level', '0');

$producer = new RdKafka\Producer($conf);
$topic = $producer->newTopic('producev_header_conversion_exception');

try {
    $topic->producev(0, 0, 'invalid', null, [
        'accepted' => 'value',
        'rejected' => new ThrowingHeader(),
    ]);
} catch (RuntimeException $exception) {
    echo $exception->getMessage(), "\n";
}

var_dump($producer->getOutQLen());

set_error_handler(static function ($severity, $message) {
    throw new ErrorException($message, 0, $severity);
});

try {
    $topic->producev(0, 0, 'invalid', null, [
        'accepted' => 'value',
        'rejected' => [],
    ]);
} catch (ErrorException $exception) {
    echo $exception->getMessage(), "\n";
} finally {
    restore_error_handler();
}

var_dump($producer->getOutQLen());

$topic->producev(0, 0, 'valid', null, ['accepted' => 'value']);
var_dump($producer->getOutQLen());
--EXPECT--
Conversion failed
int(0)
Array to string conversion
int(0)
int(1)
