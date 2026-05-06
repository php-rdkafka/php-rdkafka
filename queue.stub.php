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
    public function poll(int $timeout_ms): ?Event {}
}
