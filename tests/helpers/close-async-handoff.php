<?php

/*
 * Two consumers share two single-partition topics. One closes with
 * closeAsync() while both keep calling consume(), and the other must take
 * over its partition and consume new records from it.
 *
 * With $closeWhileClosing, the departing consumer instead calls close()
 * right after closeAsync(), while its queues, a notification registration
 * and a topic are still referenced.
 */
function testCloseAsyncHandoff(string $brokers, array $options, bool $closeWhileClosing = false): void
{
    $topicNames = [sprintf('test_rdkafka_%s', uniqid()), sprintf('test_rdkafka_%s', uniqid())];
    $groupId = sprintf('test_rdkafka_group_%s', uniqid());

    $conf = new RdKafka\Conf();
    $conf->set('metadata.broker.list', $brokers);
    $producer = new RdKafka\Producer($conf);

    $produce = function (string $topicName, string $payload) use ($producer): void {
        $producer->newTopic($topicName)->produce(0, 0, $payload);

        if ($producer->flush(10000) !== RD_KAFKA_RESP_ERR_NO_ERROR) {
            throw new RuntimeException("Timed out producing to $topicName");
        }
    };

    foreach ($topicNames as $topicName) {
        $produce($topicName, 'setup');
    }

    $createConsumer = function () use ($brokers, $groupId, $options, $topicNames): RdKafka\KafkaConsumer {
        $conf = new RdKafka\Conf();
        $conf->set('metadata.broker.list', $brokers);
        $conf->set('group.id', $groupId);
        $conf->set('auto.offset.reset', 'earliest');
        $conf->set('enable.auto.commit', 'false');
        $conf->set('enable.auto.offset.store', 'false');
        $conf->set('log_level', '0');

        foreach ($options as $name => $value) {
            $conf->set($name, $value);
        }

        $conf->setRebalanceCb(function (RdKafka\KafkaConsumer $consumer, int $err, array $partitions) {
            $cooperative = $consumer->getRebalanceProtocol() === 'COOPERATIVE';

            if ($err === RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS) {
                $cooperative ? $consumer->incrementalAssign($partitions) : $consumer->assign($partitions);
            } else {
                $cooperative ? $consumer->incrementalUnassign($partitions) : $consumer->assign(null);
            }
        });

        $consumer = new RdKafka\KafkaConsumer($conf);
        $consumer->subscribe($topicNames);

        return $consumer;
    };

    $assignment = fn (RdKafka\KafkaConsumer $consumer): array => array_map(
        fn (RdKafka\TopicPartition $partition) => $partition->getTopic(),
        $consumer->getAssignment(),
    );

    $received = ['departing' => [], 'remaining' => []];
    $consumers = ['departing' => $createConsumer(), 'remaining' => $createConsumer()];

    // Calls consume(0) on each consumer still being served until $done returns true
    $serve = function (callable $done, string $waitingFor) use (&$consumers, &$received): void {
        $deadline = microtime(true) + 30;

        while (!$done()) {
            if (microtime(true) > $deadline) {
                throw new RuntimeException("Timed out waiting for $waitingFor");
            }

            foreach ($consumers as $name => $consumer) {
                $message = $consumer->consume(0);

                if ($message === null || $message->err === RD_KAFKA_RESP_ERR__PARTITION_EOF) {
                    continue;
                }

                if ($message->err !== RD_KAFKA_RESP_ERR_NO_ERROR) {
                    throw new RuntimeException($message->errstr(), $message->err);
                }

                $received[$name][] = "{$message->topic_name}:{$message->payload}";
            }

            usleep(10000);
        }
    };

    $serve(function () use (&$consumers, $assignment, $topicNames) {
        $departing = $assignment($consumers['departing']);
        $remaining = $assignment($consumers['remaining']);
        $all = array_merge($departing, $remaining);
        sort($all);

        return count($departing) === 1 && count($remaining) === 1 && $all === $topicNames;
    }, 'one partition per consumer');
    echo "Assigned one partition to each consumer\n";

    [$transferred] = $assignment($consumers['departing']);
    [$retained] = $assignment($consumers['remaining']);
    $produce($transferred, 'before close');
    $produce($retained, 'before close');

    $serve(function () use (&$received, $transferred, $retained) {
        return in_array("$transferred:before close", $received['departing'], true)
            && in_array("$retained:before close", $received['remaining'], true);
    }, 'records before close');
    echo "Both consumers received their records\n";

    $departing = $consumers['departing'];

    if ($closeWhileClosing) {
        $consumerQueue = $departing->getConsumerQueue();
        $partitionQueue = $departing->splitPartitionQueue($transferred, 0);
        $topic = $departing->newTopic($transferred);

        // Notifications are not available on every platform
        $streams = [];
        if (method_exists($consumerQueue, 'ioEventEnable')) {
            $streams = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
            stream_set_blocking($streams[1], false);
            $consumerQueue->ioEventEnable($streams[1]);
        }

        $departing->closeAsync();
        $departing->close();
        unset($consumers['departing']);
        echo "Departing consumer closed while closing asynchronously\n";

        foreach ([
            fn () => $consumerQueue->getLength(),
            fn () => $partitionQueue->consume(0),
            fn () => $topic->getName(),
        ] as $call) {
            try {
                $call();
            } catch (Exception $e) {
                echo $e->getMessage(), "\n";
            }
        }

        foreach ($streams as $stream) {
            fclose($stream);
        }
    } else {
        $departing->closeAsync();
    }

    $serve(function () use (&$consumers, $departing, $assignment, $topicNames) {
        if (isset($consumers['departing']) && $departing->isClosed()) {
            // Stop serving the departing consumer once it has closed
            unset($consumers['departing']);
        }

        $remaining = $assignment($consumers['remaining']);
        sort($remaining);

        return !isset($consumers['departing']) && $remaining === $topicNames;
    }, 'the handoff');
    echo "Departing consumer closed and the other owns both partitions\n";

    // Commits are disabled, so earlier records can be consumed again
    $produce($transferred, 'after close');
    $serve(function () use (&$received, $transferred) {
        return in_array("$transferred:after close", $received['remaining'], true);
    }, 'the new record');
    echo "Remaining consumer received a new record from the transferred partition\n";

    if (!$closeWhileClosing) {
        $departing->close();
    }
    $consumers['remaining']->close();
    echo "Closed both consumers\n";
}
