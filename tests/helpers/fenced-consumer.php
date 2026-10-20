<?php

/*
 * Returns a consumer that the broker fenced by admitting a second static
 * member with the same group.instance.id. The fenced consumer has a fatal
 * error, so closing it fails.
 */
function createFencedConsumer(string $brokers): RdKafka\KafkaConsumer
{
    $topicName = sprintf('test_rdkafka_%s', uniqid());
    $groupId = sprintf('test_rdkafka_group_%s', uniqid());

    $conf = new RdKafka\Conf();
    $conf->set('metadata.broker.list', $brokers);
    $producer = new RdKafka\Producer($conf);
    $producer->newTopic($topicName)->produce(0, 0, 'message');

    if ($producer->flush(10000) !== RD_KAFKA_RESP_ERR_NO_ERROR) {
        throw new RuntimeException('Timed out creating the test topic');
    }

    $createStaticMember = function () use ($brokers, $groupId, $topicName): RdKafka\KafkaConsumer {
        $conf = new RdKafka\Conf();
        $conf->set('metadata.broker.list', $brokers);
        $conf->set('group.id', $groupId);
        $conf->set('group.instance.id', 'static-member');
        $conf->set('log_level', '0');

        $consumer = new RdKafka\KafkaConsumer($conf);
        $consumer->subscribe([$topicName]);

        return $consumer;
    };

    $fenced = $createStaticMember();

    $deadline = microtime(true) + 30;
    while ($fenced->getAssignment() === []) {
        if (microtime(true) > $deadline) {
            throw new RuntimeException('Timed out waiting for the assignment');
        }

        $fenced->consume(100);
    }

    $replacement = $createStaticMember();

    while (true) {
        if (microtime(true) > $deadline) {
            throw new RuntimeException('Timed out waiting for the consumer to be fenced');
        }

        $replacement->consume(0);
        $message = $fenced->consume(100);

        if ($message !== null && $message->err === RD_KAFKA_RESP_ERR__FATAL) {
            $replacement->close();

            return $fenced;
        }
    }
}
