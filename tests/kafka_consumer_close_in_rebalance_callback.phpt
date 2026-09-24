--TEST--
KafkaConsumer::close() cannot be called from a rebalance callback
--SKIPIF--
<?php
require __DIR__ . '/helpers/integration-tests-check.php';
--FILE--
<?php
require __DIR__ . '/helpers/integration-tests-check.php';

function closeFromRebalanceCallback(RdKafka\KafkaConsumer $consumer, string $context): void
{
    try {
        $consumer->close();
    } catch (RdKafka\Exception $e) {
        echo "$context: ", $e->getMessage(), "\n";
    }
}

$topicName = sprintf('test_rdkafka_%s', uniqid());

$conf = new RdKafka\Conf();
$conf->set('metadata.broker.list', getenv('TEST_KAFKA_BROKERS'));

$producer = new RdKafka\Producer($conf);
$topic = $producer->newTopic($topicName);
$topic->produce(0, 0, 'message');

$deadline = microtime(true) + 10;
while ($producer->getOutQLen() > 0 && microtime(true) < $deadline) {
    $producer->poll(100);
}

if ($producer->getOutQLen() > 0) {
    throw new RuntimeException('Timed out creating the test topic');
}

$conf = new RdKafka\Conf();
$conf->set('auto.offset.reset', 'earliest');
$conf->set('enable.auto.commit', 'false');
$conf->set('log_level', '0');
$conf->set('metadata.broker.list', getenv('TEST_KAFKA_BROKERS'));
$conf->set('group.id', sprintf('test_rdkafka_group_%s', uniqid()));

$assigned = false;
$conf->setRebalanceCb(function (RdKafka\KafkaConsumer $consumer, int $error, array $partitions) use (&$assigned): void {
    if ($error === RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS) {
        $consumer->assign($partitions);

        if (!$assigned) {
            $assigned = true;
            closeFromRebalanceCallback($consumer, 'Assign callback');
        }

        return;
    }

    if ($error === RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS) {
        $consumer->assign(null);
        closeFromRebalanceCallback($consumer, 'Revoke callback');

        return;
    }

    throw new RuntimeException("Unexpected rebalance error: $error");
});

$consumer = new RdKafka\KafkaConsumer($conf);
$consumer->subscribe([$topicName]);

$deadline = microtime(true) + 30;
while (!$assigned && microtime(true) < $deadline) {
    $message = $consumer->consume(100);

    if ($message !== null && !in_array($message->err, [RD_KAFKA_RESP_ERR_NO_ERROR, RD_KAFKA_RESP_ERR__PARTITION_EOF], true)) {
        throw new RuntimeException($message->errstr(), $message->err);
    }
}

if (!$assigned) {
    throw new RuntimeException('Timed out waiting for assignment');
}

$consumer->close();
echo "Closed after rebalance callbacks\n";

?>
--EXPECT--
Assign callback: RdKafka\KafkaConsumer::close() cannot be called from a callback
Revoke callback: RdKafka\KafkaConsumer::close() cannot be called from a callback
Closed after rebalance callbacks
