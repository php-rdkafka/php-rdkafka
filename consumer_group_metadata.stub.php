<?php

/**
 * @generate-class-entries
 * @generate-function-entries
 * @generate-legacy-arginfo
 */

namespace RdKafka;

final class ConsumerGroupMetadata
{
    public function __construct(string $group_id, int $generation_id = -1, string $member_id = "", ?string $group_instance_id = null) {}

#ifdef HAS_RD_KAFKA_CONSUMER_GROUP_METADATA_GETTERS
    /** @tentative-return-type */
    public function getGroupId(): string {}

    /** @tentative-return-type */
    public function getMemberId(): string {}

    /** @tentative-return-type */
    public function getGroupInstanceId(): ?string {}

    /** @tentative-return-type */
    public function getGenerationId(): int {}
#endif
}
