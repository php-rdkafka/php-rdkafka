<?php

/**
 * @generate-class-entries
 * @generate-function-entries
 * @generate-legacy-arginfo
 */

namespace RdKafka;

class Message
{
    public int $err;

    public ?string $topic_name = null;

    public ?int $timestamp = null;

    public int $partition;

    public ?string $payload = null;

    public ?int $len = null;

    public ?string $key = null;

    public int $offset;

    public array $headers;

    public ?string $opaque = null;

    private ?array $native_headers = null;

    /** @tentative-return-type */
    public function errstr(): ?string {}

    /** @tentative-return-type */
    public function getHeaderPairs(): array {}
}
