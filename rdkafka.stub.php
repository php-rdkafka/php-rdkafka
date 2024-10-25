<?php

/**
 * @generate-class-entries
 * @generate-function-entries
 * @generate-legacy-arginfo
 */

namespace {
    abstract class RdKafka {
        private ?callable $error_cb;

        private ?callable $dr_cb;

        private function __construct() {}

        /** @tentative-return-type */
        public function addBrokers(string $broker_list): int {}

        /** @tentative-return-type */
        public function getMetadata(bool $all_topics, ?RdKafka\Topic $only_topic, int $timeout_ms): RdKafka\Metadata {}

#ifdef HAS_RD_KAFKA_CONTROLLERID
        /** @tentative-return-type */
        public function getControllerId(int $timeout_ms): int {}
#endif

        /** @tentative-return-type */
        public function getOutQLen(): int {}

        /**
         * @alias RdKafka::getMetadata
         * @deprecated
         * @tentative-return-type
         */
        public function metadata(bool $all_topics, ?RdKafka\Topic $only_topic, int $timeout_ms): RdKafka\Metadata {}

        /**
         * @deprecated
         * @tentative-return-type
         */
        public function setLogLevel(int $level): void {}

        /** @tentative-return-type */
        public function newTopic(string $topic_name, ?RdKafka\TopicConf $topic_conf = null): RdKafka\Topic {}

        /**
         * @alias RdKafka::getOutQLen
         * @deprecated
         * @tentative-return-type
         */
        public function outqLen(): int {}

        /** @tentative-return-type */
        public function poll(int $timeout_ms): int {}

        /** @tentative-return-type */
        public function flush(int $timeout_ms): int {}

#ifdef HAS_RD_KAFKA_PURGE
        /** @tentative-return-type */
        public function purge(int $purge_flags): int {}
#endif

        /**
         * @deprecated
         * @tentative-return-type
         */
        public function setLogger(int $logger): void {}

        /** @tentative-return-type */
        public function queryWatermarkOffsets(string $topic, int $partition, int &$low, int &$high, int $timeout_ms): void {}

        /** @tentative-return-type */
        public function offsetsForTimes(array $topic_partitions, int $timeout_ms): array {}

        /** @tentative-return-type */
        public function pausePartitions(array $topic_partitions): array {}

        /** @tentative-return-type */
        public function resumePartitions(array $topic_partitions): array {}
        
        #ifdef HAS_RD_KAFKA_OAUTHBEARER
        /** @tentative-return-type */
        public function oauthbearerSetToken(string $token_value, int|float|string $lifetime_ms, string $principal_name, array $extensions = []): void {}
        
        /** @tentative-return-type */
        public function oauthbearerSetTokenFailure(string $error): void {}
        #endif
    }
}

namespace RdKafka {
    class Exception extends \Exception {
    }

    class Consumer extends \RdKafka {
        public function __construct(?Conf $conf = null) {}

        /** @tentative-return-type */
        public function newQueue(): Queue {}
    }

    class Producer extends \RdKafka {
        public function __construct(?Conf $conf = null) {}

#ifdef HAS_RD_KAFKA_TRANSACTIONS
        /** @tentative-return-type */
        public function initTransactions(int $timeout_ms): void {}

        /** @tentative-return-type */
        public function beginTransaction(): void {}

        /** @tentative-return-type */
        public function commitTransaction(int $timeout_ms): void {}

        /** @tentative-return-type */
        public function abortTransaction(int $timeout_ms): void {}
#endif
    }
}
