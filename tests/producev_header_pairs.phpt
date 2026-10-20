--TEST--
ProducerTopic::producev() accepts header pairs
--FILE--
<?php

$conf = new RdKafka\Conf();
// This brokerless test suppresses the expected missing-bootstrap warning.
$conf->set('log_level', '0');
$conf->set('queue.buffering.max.messages', '2');

$producer = new RdKafka\Producer($conf);
$topic = $producer->newTopic('producev_header_pairs');

$invalid = [
    [['name']],
    [['name', 'value', 'extra']],
    [[1, 'value']],
    [['name', 1]],
    [['name', 'value'], 'value'],
    [['name', 'value'], ['name' => 'name', 'value' => 'value']],
];

foreach ($invalid as $headers) {
    try {
        $topic->producev(0, 0, 'invalid', null, $headers);
    } catch (InvalidArgumentException $exception) {
        echo $exception->getMessage(), "\n";
    }
}

var_dump($producer->getOutQLen());

$value = 'second';
$headers = [
    ['trace', 'first'],
    ['trace', &$value],
    ['optional', null],
];
$expected = [
    ['trace', 'first'],
    ['trace', 'second'],
    ['optional', null],
];

$topic->producev(0, 0, 'pairs', null, $headers);

var_dump($producer->getOutQLen());
var_dump($headers === $expected);

// Only a list selects pairs, so this is still a map with an array value.
$topic->producev(0, 0, 'map', null, [1 => ['name', 'value']]);

var_dump($producer->getOutQLen());

try {
    $topic->producev(0, 0, 'full', null, [['name', 'value']]);
} catch (RdKafka\Exception $exception) {
    echo $exception->getMessage(), "\n";
}
--EXPECTF--
Invalid header pair at index 0 for $headers, expected [string, ?string]
Invalid header pair at index 0 for $headers, expected [string, ?string]
Invalid header pair at index 0 for $headers, expected [string, ?string]
Invalid header pair at index 0 for $headers, expected [string, ?string]
Invalid header pair at index 1 for $headers, expected [string, ?string]
Invalid header pair at index 1 for $headers, expected [string, ?string]
int(0)
int(1)
bool(true)

Warning: Array to string conversion in %s on line %d
int(2)
Local: Queue full
