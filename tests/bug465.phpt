--TEST--
Bug #465
--SKIPIF--
<?php
require __DIR__ . '/helpers/integration-tests-check.php';
--FILE--
<?php
require __DIR__ . '/helpers/integration-tests-check.php';

$n = 0;

foreach (getTopicS() as $topicMetadata) {
    $n++;
    $topicMetadata->getTopic();
}

var_dump($n > 0);

function getTopics() {
    $conf = new RdKafka\Conf();
    $conf->set('metadata.broker.list', getenv('TEST_KAFKA_BROKERS'));

    $topicName = sprintf("test_rdkafka_%s", uniqid());

    // Seed the topic so the metadata below lists at least one topic,
    // regardless of which tests ran before this one
    $producer = new RdKafka\Producer($conf);
    $producer->newTopic($topicName)->produce(RD_KAFKA_PARTITION_UA, 0, 'seed');
    if (RD_KAFKA_RESP_ERR_NO_ERROR !== $producer->flush(10*1000)) {
        throw new RuntimeException('Failed to seed topic');
    }
    unset($producer);

    $consumer = new RdKafka\Consumer($conf);

    $consumer->newTopic($topicName);

    return $consumer->getMetadata(true, null, 2*1000)->getTopics();
}
--EXPECT--
bool(true)
