--TEST--
Transactional consume-transform-produce using sendOffsetsToTransaction
--SKIPIF--
<?php
require __DIR__ . '/helpers/integration-tests-check.php';
if (!class_exists("RdKafka\\KafkaErrorException")) {
    echo "skip";
}
--FILE--
<?php
require __DIR__ . '/helpers/integration-tests-check.php';

$sourceTopic = sprintf("test_rdkafka_%s", uniqid());
$outputTopic = sprintf("test_rdkafka_%s", uniqid());
$groupId     = sprintf("test_rdkafka_%s", uniqid());

// Seed the source topic with 5 messages using a plain producer
$conf = new RdKafka\Conf();
$conf->set('metadata.broker.list', getenv('TEST_KAFKA_BROKERS'));
$conf->setLogCb(function () {});
$conf->setDrMsgCb(function ($producer, $msg) {
    if ($msg->err) {
        throw new Exception("Seed delivery failed: " . $msg->errstr());
    }
});
$seedProducer = new RdKafka\Producer($conf);
$seedTopic = $seedProducer->newTopic($sourceTopic);
for ($i = 0; $i < 5; $i++) {
    $seedTopic->produce(0, 0, "source $i");
    $seedProducer->poll(0);
}
$seedProducer->flush(10000);

// Transactional producer
$conf = new RdKafka\Conf();
$conf->set('metadata.broker.list', getenv('TEST_KAFKA_BROKERS'));
$conf->set('transactional.id', sprintf('test-txn-%s', uniqid()));
$conf->setLogCb(function () {});
$conf->setDrMsgCb(function ($producer, $msg) {
    if ($msg->err) {
        throw new Exception("Txn delivery failed: " . $msg->errstr());
    }
});
$producer = new RdKafka\Producer($conf);
$producer->initTransactions(10000);

// Consumer with read_committed isolation so it only sees committed output
$conf = new RdKafka\Conf();
$conf->set('metadata.broker.list', getenv('TEST_KAFKA_BROKERS'));
$conf->set('group.id', $groupId);
$conf->set('auto.offset.reset', 'earliest');
$conf->set('enable.auto.commit', 'false');
$conf->set('isolation.level', 'read_committed');
$conf->setLogCb(function () {});
$consumer = new RdKafka\KafkaConsumer($conf);
$consumer->subscribe([$sourceTopic]);

// Consume-transform-produce loop
$producer->beginTransaction();

$outTopic = $producer->newTopic($outputTopic);
$offsets  = [];
$consumed = 0;

while ($consumed < 5) {
    $msg = $consumer->consume(10000);
    if ($msg->err !== RD_KAFKA_RESP_ERR_NO_ERROR) {
        continue;
    }

    $outTopic->produce(0, 0, "transformed " . $msg->payload);
    $producer->poll(0);

    $offsets[] = new RdKafka\TopicPartition($msg->topic_name, $msg->partition, $msg->offset + 1);
    $consumed++;
}

// Send consumer offsets as part of the transaction
$metadata = $consumer->getConsumerGroupMetadata();
$producer->sendOffsetsToTransaction($offsets, $metadata, 10000);
unset($metadata);
$producer->commitTransaction(10000);

echo "Transaction committed\n";

// Explicitly close consumers and producers before moving to verification
$consumer->close();
unset($consumer, $producer, $seedProducer);

// Verify output topic contains transformed messages
$conf = new RdKafka\Conf();
$conf->set('metadata.broker.list', getenv('TEST_KAFKA_BROKERS'));
$conf->setLogCb(function () {});
$lowLevelConsumer = new RdKafka\Consumer($conf);
$verifyTopic = $lowLevelConsumer->newTopic($outputTopic);
$verifyTopic->consumeStart(0, RD_KAFKA_OFFSET_BEGINNING);

$received = 0;
while (true) {
    $msg = $verifyTopic->consume(0, 5000);
    if (!$msg || $msg->err === RD_KAFKA_RESP_ERR__PARTITION_EOF) {
        break;
    }
    if ($msg->err !== RD_KAFKA_RESP_ERR_NO_ERROR) {
        throw new Exception($msg->errstr(), $msg->err);
    }
    printf("output: %s\n", $msg->payload);
    $received++;
}

printf("%d messages in output topic\n", $received);
--EXPECT--
Transaction committed
output: transformed source 0
output: transformed source 1
output: transformed source 2
output: transformed source 3
output: transformed source 4
5 messages in output topic
