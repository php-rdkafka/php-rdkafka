<?php

/**
 * @generate-class-entries
 * @generate-function-entries
 * @generate-legacy-arginfo
 */

namespace RdKafka;

class Conf
{
    public function __construct() {}

    /** @tentative-return-type */
    public function dump(): array {}

    /** @tentative-return-type */
    public function get(string $name): string {}

    /** @tentative-return-type */
    public function set(string $name, string $value): void {}

    /**
     * @tentative-return-type
     * @deprecated
     */
    public function setDefaultTopicConf(TopicConf $topic_conf): void {}

    /** @tentative-return-type */
    public function setErrorCb(callable $callback): void {}

    /** @tentative-return-type */
    public function setDrMsgCb(callable $callback): void {}

    /** @tentative-return-type */
    public function setStatsCb(callable $callback): void {}

    /** @tentative-return-type */
    public function setRebalanceCb(callable $callback): void {}

    /** @tentative-return-type */
    public function setConsumeCb(callable $callback): void {}

    /** @tentative-return-type */
    public function setOffsetCommitCb(callable $callback): void {}

    /** @tentative-return-type */
    public function setLogCb(callable $callback): void {}

    /** @tentative-return-type */
    public function setOauthbearerTokenRefreshCb(callable $callback): void {}
}

class TopicConf
{
    public function __construct() {}

    /**
     * @tentative-return-type
     * @implementation-alias RdKafka\Conf::dump
     */
    public function dump(): array {}

    /**
     * @tentative-return-type
     * @implementation-alias RdKafka\Conf::get
     */
    public function get(string $name): string {}

    /**
     * @tentative-return-type
     * @implementation-alias RdKafka\Conf::set
     */
    public function set(string $name, string $value): void {}

    /** @tentative-return-type */
    public function setPartitioner(int $partitioner): void {}
}
