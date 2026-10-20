<?php

/**
 * @generate-class-entries
 * @generate-function-entries
 * @generate-legacy-arginfo
 */

namespace RdKafka;

class Queue
{
    /** @implementation-alias RdKafka::__construct */
    private  function __construct() {}

    /** @tentative-return-type */
    public function consume(int $timeout_ms): ?Message {}

    /** @tentative-return-type */
    public function getLength(): int {}

#ifndef PHP_WIN32
    /**
     * @param resource|null $stream
     * @tentative-return-type
     */
    public function ioEventEnable($stream, string $payload = "\x01"): void {}
#endif
}
