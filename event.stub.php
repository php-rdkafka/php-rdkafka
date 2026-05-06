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
}
