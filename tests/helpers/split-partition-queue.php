<?php

/*
 * Splits one of two subscribed single-partition topics into its own queue
 * inside the assignment callback, before the assignment is acknowledged.
 * The split partition's records and EOF must arrive only on its queue, the
 * other partition's only on the consumer queue, and both stay committable.
 * A second group member then makes the group revoke the split partition and
 * assign it again.
 */
function testSplitPartitionQueueRouting(string $brokers, array $options): void
{
    $splitTopic = sprintf('test_rdkafka_%s', uniqid());
    $otherTopic = sprintf('test_rdkafka_%s', uniqid());
    $groupId = sprintf('test_rdkafka_group_%s', uniqid());

    $conf = new RdKafka\Conf();
    $conf->set('metadata.broker.list', $brokers);
    $producer = new RdKafka\Producer($conf);

    $produce = function (string $topicName, array $payloads) use ($producer): void {
        $topic = $producer->newTopic($topicName);

        foreach ($payloads as $payload) {
            $topic->produce(0, 0, $payload);
        }

        if ($producer->flush(10000) !== RD_KAFKA_RESP_ERR_NO_ERROR) {
            throw new RuntimeException("Timed out producing to $topicName");
        }
    };

    $produce($splitTopic, ['split 0', 'split 1', 'split 2']);
    $produce($otherTopic, ['other 0', 'other 1', 'other 2']);

    $createConf = function () use ($brokers, $groupId, $options): RdKafka\Conf {
        $conf = new RdKafka\Conf();
        $conf->set('metadata.broker.list', $brokers);
        $conf->set('group.id', $groupId);
        $conf->set('auto.offset.reset', 'earliest');
        $conf->set('enable.auto.commit', 'false');
        $conf->set('enable.partition.eof', 'true');
        $conf->set('log_level', '0');

        foreach ($options as $name => $value) {
            $conf->set($name, $value);
        }

        return $conf;
    };

    $acknowledge = function (RdKafka\KafkaConsumer $consumer, int $err, array $partitions): void {
        $cooperative = $consumer->getRebalanceProtocol() === 'COOPERATIVE';

        if ($err === RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS) {
            $cooperative ? $consumer->incrementalAssign($partitions) : $consumer->assign($partitions);
        } else {
            $cooperative ? $consumer->incrementalUnassign($partitions) : $consumer->assign(null);
        }
    };

    // Split queues are acquired on assignment and released on revocation
    $queues = [];
    $split = true;
    $splitPartitionEvents = [];
    $conf = $createConf();
    $conf->setRebalanceCb(function (RdKafka\KafkaConsumer $consumer, int $err, array $partitions) use (&$queues, &$split, &$splitPartitionEvents, $splitTopic, $acknowledge) {
        foreach ($partitions as $partition) {
            if ($partition->getTopic() !== $splitTopic) {
                continue;
            }

            $splitPartitionEvents[] = $err === RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS ? 'assign' : 'revoke';

            if ($err !== RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS) {
                unset($queues[$partition->getPartition()]);
            } elseif ($split) {
                $queues[$partition->getPartition()] = $consumer->splitPartitionQueue($splitTopic, $partition->getPartition());
            }
        }

        $acknowledge($consumer, $err, $partitions);
    });

    $consumer = new RdKafka\KafkaConsumer($conf);
    $consumer->subscribe([$splitTopic, $otherTopic]);

    $received = ['consumer queue' => [], 'partition queue' => []];
    $record = function (?RdKafka\Message $message, string $source) use (&$received): void {
        if ($message === null) {
            return;
        }

        if ($message->err === RD_KAFKA_RESP_ERR__PARTITION_EOF) {
            $received[$source][] = "EOF {$message->topic_name}";
        } elseif ($message->err === RD_KAFKA_RESP_ERR_NO_ERROR) {
            $received[$source][] = $message->payload;
        } else {
            throw new RuntimeException($message->errstr(), $message->err);
        }
    };

    // Serves the consumer queue, the current split queues, any extra queues
    // and other group members until $done returns true
    $serve = function (callable $done, string $waitingFor, array $extraQueues = [], array $members = []) use ($consumer, $record, &$queues): void {
        $deadline = microtime(true) + 30;

        while (!$done()) {
            if (microtime(true) > $deadline) {
                throw new RuntimeException("Timed out waiting for $waitingFor");
            }

            // The consumer queue also serves the rebalance callbacks
            $record($consumer->consume(10), 'consumer queue');

            foreach (array_merge($queues, $extraQueues) as $queue) {
                $record($queue->consume(0), 'partition queue');
            }

            foreach ($members as $member) {
                $message = $member->consume(0);

                if ($message !== null && !in_array($message->err, [RD_KAFKA_RESP_ERR_NO_ERROR, RD_KAFKA_RESP_ERR__PARTITION_EOF], true)) {
                    throw new RuntimeException($message->errstr(), $message->err);
                }
            }
        }
    };

    $serve(function () use (&$received) {
        return count($received['consumer queue']) >= 4 && count($received['partition queue']) >= 4;
    }, 'the records');

    $received['consumer queue'] = str_replace($otherTopic, 'other topic', $received['consumer queue']);
    $received['partition queue'] = str_replace($splitTopic, 'split topic', $received['partition queue']);

    foreach ($received as $source => $messages) {
        echo "$source: ", implode(', ', $messages), "\n";
    }

    // Consumed positions are stored for both queues and committed together
    $consumer->commit();
    $committed = $consumer->getCommittedOffsets([
        new RdKafka\TopicPartition($splitTopic, 0),
        new RdKafka\TopicPartition($otherTopic, 0),
    ], 10000);
    echo 'committed: ', $committed[0]->getOffset(), ', ', $committed[1]->getOffset(), "\n";

    // A member that subscribes only to the split topic makes the group revoke
    // the split partition. Depending on the protocol and assignor, the
    // partition then moves to the member or straight back; closing the member
    // leaves it assigned to the consumer again either way.
    $revokeAndReassignSplitPartition = function () use ($createConf, $acknowledge, $splitTopic, $consumer, $serve, &$splitPartitionEvents): void {
        $conf = $createConf();
        $conf->setRebalanceCb($acknowledge);
        $member = new RdKafka\KafkaConsumer($conf);
        $member->subscribe([$splitTopic]);

        // Wait until the rebalance has finished, with the split partition
        // revoked and then owned by one of the members. Closing the new
        // member in the middle of a rebalance can leave the group waiting.
        $events = count($splitPartitionEvents);
        $serve(function () use (&$splitPartitionEvents, $events, $member) {
            $since = array_slice($splitPartitionEvents, $events);
            $revoked = array_search('revoke', $since, true);

            return $revoked !== false
                && (in_array('assign', array_slice($since, $revoked), true) || $member->getAssignment() !== []);
        }, 'the split partition to be revoked', [], [$member]);

        $member->close();

        $serve(function () use ($consumer) {
            return count($consumer->getAssignment()) === 2;
        }, 'the split partition to be assigned again');
    };

    $expectSplitRecord = function (string $payload, array $extraQueues) use ($produce, $splitTopic, $serve, &$received): void {
        $received = ['consumer queue' => [], 'partition queue' => []];
        $produce($splitTopic, [$payload]);

        $serve(function () use (&$received, $payload) {
            return in_array($payload, $received['partition queue'], true);
        }, "the record $payload", $extraQueues);

        $unexpected = preg_grep('/^EOF /', $received['consumer queue'], PREG_GREP_INVERT);
        if ($unexpected !== []) {
            throw new RuntimeException('Unexpected records on the consumer queue: ' . implode(', ', $unexpected));
        }

        echo "partition queue after reassignment: $payload\n";
    };

    // Without splitting again, a queue kept across the revocation still
    // receives the partition's records: the split persists in librdkafka
    $split = false;
    $retained = $queues[0];
    $revokeAndReassignSplitPartition();
    $expectSplitRecord('split 3', [$retained]);
    unset($retained);

    // Normally a revoked partition's queue is released, and the partition
    // is split again when it is assigned
    $split = true;
    $revokeAndReassignSplitPartition();
    $expectSplitRecord('split 4', []);

    $queues = [];
    $consumer->close();
}
