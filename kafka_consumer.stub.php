<?php

/**
 * @generate-class-entries
 * @generate-function-entries
 * @generate-legacy-arginfo
 */

namespace RdKafka;

class KafkaConsumer
{
    private ?callable $error_cb;

    private ?callable $rebalance_cb;

    private ?callable $dr_msg_cb;

    public function __construct(Conf $conf) {}

    /** @tentative-return-type */
    public function assign(?array $topic_partitions = null): void {}

#ifdef HAS_RD_KAFKA_INCREMENTAL_ASSIGN
    /** @tentative-return-type */
    public function incrementalAssign(array $topic_partitions): void {}

    /** @tentative-return-type */
    public function incrementalUnassign(array $topic_partitions): void {}
#endif

    /** @tentative-return-type */
    public function getAssignment(): array {}

    /** @tentative-return-type */
    public function commit(Message|array|null $message_or_offsets = null): void {}

    /** @tentative-return-type */
    public function close(): void {}

    /** @tentative-return-type */
    public function commitAsync(Message|array|null $message_or_offsets = null): void {}

    /** @tentative-return-type */
    public function consume(int $timeout_ms): ?Message {}

    /** @tentative-return-type */
    public function subscribe(array $topics): void {}

    /** @tentative-return-type */
    public function getSubscription(): array {}

    /** @tentative-return-type */
    public function unsubscribe(): void {}

    /** @tentative-return-type */
    public function getMetadata(bool $all_topics, ?Topic $only_topic, int $timeout_ms): Metadata {}

    /** @tentative-return-type */
    public function getControllerId(int $timeout_ms): int {}

    /** @tentative-return-type */
    public function newTopic(string $topic_name, ?TopicConf $topic_conf = null): KafkaConsumerTopic {}

    /** @tentative-return-type */
    public function getCommittedOffsets(array $topic_partitions, int $timeout_ms): array {}

    /** @tentative-return-type */
    public function getOffsetPositions(array $topic_partitions): array {}

    /** @tentative-return-type */
    public function queryWatermarkOffsets(string $topic, int $partition, int &$low, int &$high, int $timeout_ms): void {}

    /** @tentative-return-type */
    public function offsetsForTimes(array $topic_partitions, int $timeout_ms): array {}

    /** @tentative-return-type */
    public function pausePartitions(array $topic_partitions): array {}

    /** @tentative-return-type */
    public function resumePartitions(array $topic_partitions): array {}

    /** @tentative-return-type */
    public function poll(int $timeout_ms): int {}

    /** @tentative-return-type */
    public function oauthbearerSetToken(string $token_value, int $lifetime_ms, string $principal_name, array $extensions = []): void {}

    /** @tentative-return-type */
    public function oauthbearerSetTokenFailure(string $error): void {}
}
