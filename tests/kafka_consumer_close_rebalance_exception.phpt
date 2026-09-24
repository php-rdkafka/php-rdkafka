--TEST--
KafkaConsumer::close() releases partitions when the rebalance callback does not
--SKIPIF--
<?php
require __DIR__ . '/helpers/integration-tests-check.php';
--FILE--
<?php
require __DIR__ . '/helpers/integration-tests-check.php';

$topicName = sprintf('test_rdkafka_%s', uniqid());

$conf = new RdKafka\Conf();
$conf->set('metadata.broker.list', getenv('TEST_KAFKA_BROKERS'));
$producer = new RdKafka\Producer($conf);
$producer->newTopic($topicName)->produce(0, 0, 'message');

if ($producer->flush(10000) !== RD_KAFKA_RESP_ERR_NO_ERROR) {
    throw new RuntimeException('Timed out creating the test topic');
}

function release(RdKafka\KafkaConsumer $consumer, array $partitions, bool $cooperative): void
{
    $cooperative ? $consumer->incrementalUnassign($partitions) : $consumer->assign(null);
}

function createAssignedConsumer(string $topicName, string $strategy, callable $onRevoke, ?callable $onLog = null): RdKafka\KafkaConsumer
{
    $conf = new RdKafka\Conf();
    $conf->set('metadata.broker.list', getenv('TEST_KAFKA_BROKERS'));
    $conf->set('group.id', sprintf('test_rdkafka_group_%s', uniqid()));
    $conf->set('partition.assignment.strategy', $strategy);
    $conf->set('log_level', '0');

    if ($onLog) {
        $conf->set('debug', 'cgrp');
        $conf->setLogCb($onLog);
    }

    $cooperative = false;
    $conf->setRebalanceCb(function (RdKafka\KafkaConsumer $consumer, int $err, array $partitions) use (&$cooperative, $onRevoke) {
        if ($err === RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS) {
            $cooperative = $consumer->getRebalanceProtocol() === 'COOPERATIVE';
            $cooperative ? $consumer->incrementalAssign($partitions) : $consumer->assign($partitions);

            return;
        }

        $onRevoke($consumer, $partitions, $cooperative);
    });

    $consumer = new RdKafka\KafkaConsumer($conf);
    $consumer->subscribe([$topicName]);

    $deadline = microtime(true) + 30;
    while ($consumer->getAssignment() === [] && microtime(true) < $deadline) {
        $consumer->consume(100);
    }

    if ($consumer->getAssignment() === []) {
        throw new RuntimeException('Timed out waiting for the assignment');
    }

    return $consumer;
}

function closeAndReport(RdKafka\KafkaConsumer $consumer): void
{
    try {
        $consumer->close();
        echo "  Closed\n";
    } catch (RuntimeException $e) {
        echo '  Closed, then threw: ', $e->getMessage(), "\n";
    }

    try {
        $consumer->getAssignment();
    } catch (Exception $e) {
        echo '  ', $e->getMessage(), "\n";
    }
}

foreach (['range' => 'eager', 'cooperative-sticky' => 'cooperative'] as $strategy => $protocol) {
    echo "$protocol revoke callback throws before acknowledging\n";
    closeAndReport(createAssignedConsumer($topicName, $strategy, function () {
        throw new RuntimeException('revoke callback');
    }));

    echo "$protocol revoke callback throws after acknowledging\n";
    closeAndReport(createAssignedConsumer($topicName, $strategy, function (RdKafka\KafkaConsumer $consumer, array $partitions, bool $cooperative) {
        release($consumer, $partitions, $cooperative);
        throw new RuntimeException('revoke callback');
    }));

    echo "$protocol revoke callback returns without acknowledging\n";
    closeAndReport(createAssignedConsumer($topicName, $strategy, function (RdKafka\KafkaConsumer $consumer) use ($topicName) {
        foreach ([
            fn () => $consumer->getConsumerQueue(),
            fn () => $consumer->splitPartitionQueue($topicName, 0),
            fn () => $consumer->newTopic($topicName),
        ] as $acquire) {
            try {
                $acquire();
            } catch (RdKafka\Exception $e) {
                echo '  ', $e->getMessage(), "\n";
            }
        }
    }));

    echo "$protocol revoke callback skipped after an earlier exception\n";
    $closing = false;
    $revoked = false;
    $consumer = createAssignedConsumer(
        $topicName,
        $strategy,
        function () use (&$revoked) {
            $revoked = true;
        },
        function () use (&$closing) {
            if ($closing) {
                $closing = false;
                throw new RuntimeException('log callback');
            }
        },
    );
    $closing = true;
    closeAndReport($consumer);
    var_dump($revoked);
}

?>
--EXPECT--
eager revoke callback throws before acknowledging
  Closed, then threw: revoke callback
  RdKafka\KafkaConsumer::__construct() has not been called, or RdKafka\KafkaConsumer::close() was already called
eager revoke callback throws after acknowledging
  Closed, then threw: revoke callback
  RdKafka\KafkaConsumer::__construct() has not been called, or RdKafka\KafkaConsumer::close() was already called
eager revoke callback returns without acknowledging
  RdKafka\KafkaConsumer is being closed
  RdKafka\KafkaConsumer is being closed
  RdKafka\KafkaConsumer is being closed
  Closed
  RdKafka\KafkaConsumer::__construct() has not been called, or RdKafka\KafkaConsumer::close() was already called
eager revoke callback skipped after an earlier exception
  Closed, then threw: log callback
  RdKafka\KafkaConsumer::__construct() has not been called, or RdKafka\KafkaConsumer::close() was already called
bool(false)
cooperative revoke callback throws before acknowledging
  Closed, then threw: revoke callback
  RdKafka\KafkaConsumer::__construct() has not been called, or RdKafka\KafkaConsumer::close() was already called
cooperative revoke callback throws after acknowledging
  Closed, then threw: revoke callback
  RdKafka\KafkaConsumer::__construct() has not been called, or RdKafka\KafkaConsumer::close() was already called
cooperative revoke callback returns without acknowledging
  RdKafka\KafkaConsumer is being closed
  RdKafka\KafkaConsumer is being closed
  RdKafka\KafkaConsumer is being closed
  Closed
  RdKafka\KafkaConsumer::__construct() has not been called, or RdKafka\KafkaConsumer::close() was already called
cooperative revoke callback skipped after an earlier exception
  Closed, then threw: log callback
  RdKafka\KafkaConsumer::__construct() has not been called, or RdKafka\KafkaConsumer::close() was already called
bool(false)
