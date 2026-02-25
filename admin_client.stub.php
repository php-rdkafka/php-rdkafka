<?php

/**
 * @generate-class-entries
 * @generate-function-entries
 * @generate-legacy-arginfo
 */

namespace RdKafka\Admin;

class AdminClient
{
    private ?callable $error_cb;

    private ?callable $dr_msg_cb;

    public function __construct(\RdKafka\Conf $conf) {}

    /** @tentative-return-type */
    public function newAdminOptions(int $for_api): AdminOptions {}

    /**
     * @param NewTopic[] $new_topics
     * @tentative-return-type
     */
    public function createTopics(array $new_topics, ?AdminOptions $options = null): array {}

    /**
     * @param DeleteTopic[] $delete_topics
     * @tentative-return-type
     */
    public function deleteTopics(array $delete_topics, ?AdminOptions $options = null): array {}

    /**
     * @param NewPartitions[] $new_partitions
     * @tentative-return-type
     */
    public function createPartitions(array $new_partitions, ?AdminOptions $options = null): array {}

#ifdef HAS_RD_KAFKA_DESCRIBE_TOPICS
    /**
     * @param string[] $topics
     * @tentative-return-type
     */
    public function describeTopics(array $topics, ?AdminOptions $options = null): array {}
#endif

    /**
     * @param \RdKafka\TopicPartition[] $topic_partitions
     * @return \RdKafka\TopicPartition[]
     * @tentative-return-type
     */
    public function deleteRecords(array $topic_partitions, ?AdminOptions $options = null): array {}
}

class AdminOptions
{
    /** @tentative-return-type */
    public function setRequestTimeout(int $timeout_ms): void {}

    /** @tentative-return-type */
    public function setOperationTimeout(int $timeout_ms): void {}

    /** @tentative-return-type */
    public function setValidateOnly(bool $validate_only): void {}

    /** @tentative-return-type */
    public function setBrokerId(int $broker_id): void {}

#ifdef HAS_RD_KAFKA_DESCRIBE_TOPICS
    /** @tentative-return-type */
    public function setIncludeAuthorizedOperations(bool $include): void {}
#endif
}

class NewTopic
{
    public function __construct(string $topic, int $num_partitions, int $replication_factor) {}

    /** @tentative-return-type */
    public function setReplicaAssignment(int $partition, array $broker_ids): void {}

    /** @tentative-return-type */
    public function setConfig(string $name, string $value): void {}
}

class DeleteTopic
{
    public function __construct(string $topic) {}
}

class NewPartitions
{
    public function __construct(string $topic, int $new_total_count) {}

    /** @tentative-return-type */
    public function setReplicaAssignment(int $new_partition_index, array $broker_ids): void {}
}

class TopicResult
{
    public int $error;

    public ?string $error_string;

    public string $name;

    /** @tentative-return-type */
    public function getError(): int {}

    /** @tentative-return-type */
    public function getErrorString(): ?string {}

    /** @tentative-return-type */
    public function getName(): string {}
}

#ifdef HAS_RD_KAFKA_DESCRIBE_TOPICS
class Node
{
    public int $id;

    public string $host;

    public int $port;

    public ?string $rack;
}

class TopicPartitionInfo
{
    public int $partition;

    public ?Node $leader;

    /** @var Node[] */
    public array $isr;

    /** @var Node[] */
    public array $replicas;
}

class TopicDescription
{
    public string $name;

    public ?string $topic_id;

    public bool $is_internal;

    public int $error;

    public ?string $error_string;

    /** @var TopicPartitionInfo[] */
    public array $partitions;
}
#endif
