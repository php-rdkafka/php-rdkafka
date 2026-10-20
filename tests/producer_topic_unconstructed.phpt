--TEST--
ProducerTopic methods reject unconstructed objects
--FILE--
<?php

$reflection = new ReflectionClass(RdKafka\ProducerTopic::class);

foreach (['produce', 'producev'] as $method) {
    $topic = $reflection->newInstanceWithoutConstructor();

    try {
        $topic->$method(0, 0, 'message');
    } catch (Exception $exception) {
        printf("%s: %s\n", $method, $exception->getMessage());
    }
}
--EXPECT--
produce: RdKafka\Topic::__construct() has not been called
producev: RdKafka\Topic::__construct() has not been called
