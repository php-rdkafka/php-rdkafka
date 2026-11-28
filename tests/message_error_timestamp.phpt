--TEST--
Message timestamp remains compatible for errors
--FILE--
<?php

$messages = [];

$conf = new RdKafka\Conf();
// This brokerless test suppresses the expected missing-bootstrap logs.
$conf->set('log_level', '0');
$conf->set('message.timeout.ms', '1');
$conf->setDrMsgCb(function ($producer, $message) use (&$messages) {
    $messages[] = $message;
});

$producer = new RdKafka\Producer($conf);
$topic = $producer->newTopic('message_error_timestamp');

$topic->producev(0, 0, 'with payload');
$topic->producev(0, 0, null);

$producer->flush(10 * 1000);

var_dump($messages[0]->payload);
var_dump($messages[0]->timestamp);
var_dump($messages[1]->payload);
var_dump($messages[1]->timestamp);
--EXPECT--
string(12) "with payload"
int(-1)
NULL
NULL
