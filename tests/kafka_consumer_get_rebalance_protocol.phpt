--TEST--
KafkaConsumer::getRebalanceProtocol() returns "NONE" before group join
--FILE--
<?php

$conf = new RdKafka\Conf();
$conf->set('metadata.broker.list', 'localhost:9092');
$conf->set('group.id', 'test-rebalance-protocol');
$conf->setLogCb(function () {});

$consumer = new RdKafka\KafkaConsumer($conf);

$protocol = $consumer->getRebalanceProtocol();

var_dump($protocol);
--EXPECT--
string(4) "NONE"
