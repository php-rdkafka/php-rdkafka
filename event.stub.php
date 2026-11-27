<?php

/**
 * @generate-class-entries
 * @generate-function-entries
 * @generate-legacy-arginfo
 */

namespace RdKafka;

class Event
{
    /** @implementation-alias RdKafka::__construct */
    private function __construct() {}

    /** @tentative-return-type */
    public function getType(): int {}

    /** @tentative-return-type */
    public function getName(): string {}

    /** @tentative-return-type */
    public function getError(): int {}

    /** @tentative-return-type */
    public function getErrorString(): ?string {}

    /**
     * @return Admin\TopicResult[]
     * @tentative-return-type
     */
    public function getCreateTopicsResult(): array {}

    /**
     * @return Admin\TopicResult[]
     * @tentative-return-type
     */
    public function getDeleteTopicsResult(): array {}

    /**
     * @return Admin\TopicResult[]
     * @tentative-return-type
     */
    public function getCreatePartitionsResult(): array {}

#ifdef HAS_RD_KAFKA_DESCRIBE_TOPICS
    /**
     * @return Admin\TopicDescription[]
     * @tentative-return-type
     */
    public function getDescribeTopicsResult(): array {}
#endif

    /**
     * @return TopicPartition[]
     * @tentative-return-type
     */
    public function getDeleteRecordsResult(): array {}
}
