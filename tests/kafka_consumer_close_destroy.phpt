--TEST--
KafkaConsumer::close() followed by object destruction does not segfault
--SKIPIF--
<?php
require __DIR__ . '/helpers/integration-tests-check.php';
--FILE--
<?php
require __DIR__ . '/helpers/integration-tests-check.php';

// Reproduce bug where close() called rd_kafka_consumer_close() and set rk=NULL
// without calling rd_kafka_destroy(), causing the destructor to skip cleanup
// and resulting in an unclean librdkafka shutdown / segfault.
for ($i = 0; $i < 5; $i++) {
    $conf = new RdKafka\Conf();
    $conf->set('group.id', sprintf('test_close_destroy_%s', uniqid()));
    $conf->set('metadata.broker.list', getenv('TEST_KAFKA_BROKERS'));
    $conf->setLogCb(function ($kafka, $level, $facility, $message) {});

    $consumer = new RdKafka\KafkaConsumer($conf);
    $consumer->subscribe([sprintf('test_rdkafka_%s', uniqid())]);
    $consumer->close();
    unset($consumer);
}

echo "OK\n";
--EXPECT--
OK
