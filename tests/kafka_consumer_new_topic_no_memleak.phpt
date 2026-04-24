--TEST--
KafkaConsumer::newTopic() does not leak the rd_kafka_topic_t handle on destruction
--SKIPIF--
<?php
require __DIR__ . '/helpers/integration-tests-check.php';
--FILE--
<?php
require __DIR__ . '/helpers/integration-tests-check.php';

for ($i = 0; $i < 5; $i++) {
    $conf = new RdKafka\Conf();
    $conf->set('group.id', sprintf('test_new_topic_memleak_%s', uniqid()));
    $conf->set('metadata.broker.list', getenv('TEST_KAFKA_BROKERS'));
    $conf->setLogCb(function () {});

    $consumer = new RdKafka\KafkaConsumer($conf);
    $topic = $consumer->newTopic('test_rdkafka_new_topic_memleak');
    unset($topic);
    $consumer->close();
    unset($consumer);
}

echo "OK\n";
--EXPECT--
OK
