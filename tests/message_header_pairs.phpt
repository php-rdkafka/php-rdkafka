--TEST--
Message::getHeaderPairs() preserves header order, repeated names and null values
--SKIPIF--
<?php
require __DIR__ . '/helpers/integration-tests-check.php';
--FILE--
<?php
require __DIR__ . '/helpers/integration-tests-check.php';

// Each case holds the produced headers, the expected pairs and the expected map.
$cases = [
    'repeated names' => [
        [['trace', 'first'], ['other', 'value'], ['trace', 'second']],
        [['trace', 'first'], ['other', 'value'], ['trace', 'second']],
        ['trace' => 'second', 'other' => 'value'],
    ],
    'null value' => [
        [['present', 'value'], ['optional', null]],
        [['present', 'value'], ['optional', null]],
        ['present' => 'value', 'optional' => ''],
    ],
    'repeated names and values of every kind' => [
        [['trace', 'first'], ['trace', 'second'], ['optional', null], ['empty', ''], ['binary', "\x00\xff"]],
        [['trace', 'first'], ['trace', 'second'], ['optional', null], ['empty', ''], ['binary', "\x00\xff"]],
        ['trace' => 'second', 'optional' => '', 'empty' => '', 'binary' => "\x00\xff"],
    ],
    // librdkafka does not return the length of header names.
    'null byte in name' => [
        [["nul\0suffix", 'value']],
        [['nul', 'value']],
        ['nul' => 'value'],
    ],
    'map' => [
        ['name' => 'value', 7 => 'seven'],
        [['name', 'value'], ['7', 'seven']],
        ['name' => 'value', 7 => 'seven'],
    ],
    'no headers' => [null, [], []],
];

$conf = new RdKafka\Conf();
$conf->set('metadata.broker.list', getenv('TEST_KAFKA_BROKERS'));
$delivered = [];
$conf->setDrMsgCb(function ($producer, $message) use (&$delivered) {
    if ($message->err) {
        throw new Exception($message->errstr(), $message->err);
    }
    $delivered[] = $message;
});

$producer = new RdKafka\Producer($conf);
$topicName = sprintf('test_rdkafka_%s', uniqid());
$topic = $producer->newTopic($topicName);

if (!$producer->getMetadata(false, $topic, 10 * 1000)) {
    echo "Failed to get metadata, is broker down?\n";
}

foreach ($cases as $name => [$headers]) {
    $topic->producev(0, 0, $name, null, $headers);
}

if ($producer->flush(10 * 1000) !== RD_KAFKA_RESP_ERR_NO_ERROR) {
    throw new Exception('Messages were not delivered');
}

$conf = new RdKafka\Conf();
$conf->set('metadata.broker.list', getenv('TEST_KAFKA_BROKERS'));

$consumer = new RdKafka\Consumer($conf);
$topic = $consumer->newTopic($topicName);
$topic->consumeStart(0, RD_KAFKA_OFFSET_BEGINNING);

$consumed = [];
$deadline = microtime(true) + 10;
while (count($consumed) < count($cases) && microtime(true) < $deadline) {
    $message = $topic->consume(0, 1000);
    if ($message === null) {
        continue;
    }

    if (RD_KAFKA_RESP_ERR_NO_ERROR !== $message->err) {
        throw new Exception($message->errstr(), $message->err);
    }

    $consumed[] = $message;
}

$topic->consumeStop(0);
unset($topic, $consumer, $producer, $conf);

foreach ($consumed as $index => $message) {
    [, $pairs, $map] = $cases[$message->payload];

    printf(
        "%s: pairs %s, delivery report %s, map %s\n",
        $message->payload,
        var_export($message->getHeaderPairs() === $pairs, true),
        var_export($delivered[$index]->getHeaderPairs() === $pairs, true),
        var_export($message->headers === $map, true)
    );

    // Writes to the returned pairs or the map must not reach the snapshot.
    $clone = clone $message;
    $serialized = unserialize(serialize($message));
    $returned = $message->getHeaderPairs();
    if ($returned !== []) {
        $returned[0][1] = 'changed';
    }
    $message->headers[array_key_first($message->headers) ?? 'added'] = 'changed';

    var_dump(
        $message->getHeaderPairs() === $pairs
        && $clone->getHeaderPairs() === $pairs
        && $serialized->getHeaderPairs() === $pairs
    );
}
--EXPECT--
repeated names: pairs true, delivery report true, map true
bool(true)
null value: pairs true, delivery report true, map true
bool(true)
repeated names and values of every kind: pairs true, delivery report true, map true
bool(true)
null byte in name: pairs true, delivery report true, map true
bool(true)
map: pairs true, delivery report true, map true
bool(true)
no headers: pairs true, delivery report true, map true
bool(true)
