--TEST--
KafkaConsumer destruction releases eager and cooperative assignments
--SKIPIF--
<?php
require __DIR__ . '/helpers/integration-tests-check.php';
--FILE--
<?php
require __DIR__ . '/helpers/integration-tests-check.php';

$topicName = sprintf('test_rdkafka_%s', uniqid());
$observerTopicName = sprintf('test_rdkafka_%s', uniqid());

$conf = new RdKafka\Conf();
$conf->set('metadata.broker.list', getenv('TEST_KAFKA_BROKERS'));
$producer = new RdKafka\Producer($conf);
$producer->newTopic($topicName)->produce(0, 0, 'message');
$producer->newTopic($observerTopicName)->produce(0, 0, 'message');

if ($producer->flush(10000) !== RD_KAFKA_RESP_ERR_NO_ERROR) {
    throw new RuntimeException('Timed out creating the test topics');
}

function createConsumer(string $topicName, string $strategy, string $groupId, int &$assignments = 0): RdKafka\KafkaConsumer
{
    $conf = new RdKafka\Conf();
    $conf->set('metadata.broker.list', getenv('TEST_KAFKA_BROKERS'));
    $conf->set('group.id', $groupId);
    $conf->set('partition.assignment.strategy', $strategy);
    $conf->set('log_level', '0');

    // The callback does not keep the consumer alive, so unset() destroys it
    $conf->setRebalanceCb(function (RdKafka\KafkaConsumer $consumer, int $err, array $partitions) use (&$assignments) {
        $cooperative = $consumer->getRebalanceProtocol() === 'COOPERATIVE';

        if ($err === RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS) {
            $assignments++;
            $cooperative ? $consumer->incrementalAssign($partitions) : $consumer->assign($partitions);
        } else {
            $cooperative ? $consumer->incrementalUnassign($partitions) : $consumer->assign(null);
        }
    });

    $consumer = new RdKafka\KafkaConsumer($conf);
    $consumer->subscribe([$topicName]);

    return $consumer;
}

function waitFor(callable $condition, callable $serve, string $description): void
{
    $deadline = microtime(true) + 30;
    while (!$condition()) {
        if (microtime(true) > $deadline) {
            throw new RuntimeException("Timed out waiting for $description");
        }

        $serve();
    }
}

foreach (['range' => 'eager', 'cooperative-sticky' => 'cooperative'] as $strategy => $protocol) {
    $consumer = createConsumer($topicName, $strategy, sprintf('test_rdkafka_group_%s', uniqid()));
    waitFor(fn () => $consumer->getAssignment() !== [], fn () => $consumer->consume(100), 'the assignment');
    unset($consumer);
    echo "Destroyed $protocol consumer with an assignment\n";

    // The observer shares the group but subscribes to another topic. Its
    // next assignment callback shows that the group has finished a
    // rebalance giving the unserved consumer its topic.
    $groupId = sprintf('test_rdkafka_group_%s', uniqid());
    $observerAssignments = 0;
    $observer = createConsumer($observerTopicName, $strategy, $groupId, $observerAssignments);
    waitFor(fn () => $observer->getAssignment() !== [], fn () => $observer->consume(100), 'the observer assignment');

    $assignmentsBeforeJoin = $observerAssignments;
    $consumer = createConsumer($topicName, $strategy, $groupId);
    waitFor(
        function () use (&$observerAssignments, $assignmentsBeforeJoin, $consumer) {
            return $observerAssignments > $assignmentsBeforeJoin && $consumer->getConsumerQueue()->getLength() > 0;
        },
        fn () => $observer->consume(100),
        'the pending assignment',
    );
    unset($consumer);
    echo "Destroyed $protocol consumer with a pending assignment\n";
    $observer->close();

    $consumer = createConsumer($topicName, $strategy, sprintf('test_rdkafka_group_%s', uniqid()));
    waitFor(fn () => $consumer->getAssignment() !== [], fn () => $consumer->consume(100), 'the assignment');
    $consumer->close();
    echo "Closed $protocol consumer with an assignment\n";
}

// Destroyed during request shutdown
$consumer = createConsumer($topicName, 'cooperative-sticky', sprintf('test_rdkafka_group_%s', uniqid()));
waitFor(fn () => $consumer->getAssignment() !== [], fn () => $consumer->consume(100), 'the assignment');

?>
--EXPECT--
Destroyed eager consumer with an assignment
Destroyed eager consumer with a pending assignment
Closed eager consumer with an assignment
Destroyed cooperative consumer with an assignment
Destroyed cooperative consumer with a pending assignment
Closed cooperative consumer with an assignment
