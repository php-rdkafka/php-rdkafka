<?php

/**
 * @generate-class-entries
 * @generate-function-entries
 * @generate-legacy-arginfo
 */

namespace RdKafka;

class TopicPartition
{
    public function __construct(string $topic, int $partition, int $offset = 0) {}

    /** @tentative-return-type */
    public function getTopic(): ?string {}

    /** @tentative-return-type */
    public function setTopic(string $topic_name): TopicPartition {}

    /** @tentative-return-type */
    public function getPartition(): int {}

    /** @tentative-return-type */
    public function setPartition(int $partition): TopicPartition {}

    /** @tentative-return-type */
    public function getOffset(): int {}

    /** @tentative-return-type */
    public function setOffset(int $offset): TopicPartition {}

    /** @tentative-return-type */
    public function getErr(): ?int {}

    /** @tentative-return-type */
    public function getMetadata(): ?string {}

    /** @tentative-return-type */
    public function setMetadata(?string $metadata): TopicPartition {}
}
