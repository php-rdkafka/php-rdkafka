--TEST--
ProducerTopic::producev() preserves header values
--FILE--
<?php

class StringableHeader
{
    public function __toString(): string
    {
        return 'converted';
    }
}

$conf = new RdKafka\Conf();
// This brokerless test suppresses the expected missing-bootstrap warning.
$conf->set('log_level', '0');

$producer = new RdKafka\Producer($conf);
$topic = $producer->newTopic('producev_preserves_header_values');

$object = new StringableHeader();
$number = 42;
$headers = [
    'object' => $object,
    'reference' => &$number,
    'null' => null,
];
$expected = [
    'object' => $object,
    'reference' => 42,
    'null' => null,
];

$topic->producev(0, 0, 'message', null, $headers);

var_dump($headers === $expected);
var_dump($headers['object'] === $object);
$number = 43;
var_dump($headers['reference']);
var_dump($headers['null']);
--EXPECT--
bool(true)
bool(true)
int(43)
NULL
