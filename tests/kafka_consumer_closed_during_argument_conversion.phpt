--TEST--
KafkaConsumer methods fail safely when converting an argument closes the consumer
--FILE--
<?php

class ClosingValue
{
    public function __construct(private RdKafka\KafkaConsumer $consumer)
    {
    }

    public function __toString(): string
    {
        $this->consumer->close();

        return 'value';
    }
}

class ClosingMessage extends RdKafka\Message
{
    public function __construct(private RdKafka\KafkaConsumer $consumer)
    {
    }

    public function __get(string $name): mixed
    {
        if ($name === 'topic_name') {
            $this->consumer->close();

            // Built at runtime, so a copy that is not released is reported
            return str_repeat('topic', 2);
        }

        return 0;
    }
}

$conf = new RdKafka\Conf();
$conf->set('group.id', 'closed_during_argument_conversion');
$conf->set('security.protocol', 'SASL_PLAINTEXT');
$conf->set('sasl.mechanisms', 'OAUTHBEARER');
$conf->setLogCb(function () {});

$calls = [
    fn ($consumer) => $consumer->subscribe(['topic', new ClosingValue($consumer)]),
    function ($consumer) {
        set_error_handler(function () use ($consumer) {
            $consumer->close();

            return true;
        });

        try {
            $consumer->subscribe(['topic', []]);
        } finally {
            restore_error_handler();
        }
    },
    fn ($consumer) => $consumer->oauthbearerSetToken('token', (time() + 60) * 1000, 'principal', ['key' => new ClosingValue($consumer)]),
    function ($consumer) {
        $message = new ClosingMessage($consumer);
        unset($message->err, $message->topic_name, $message->partition, $message->offset);
        $consumer->commit($message);
    },
];

foreach ($calls as $call) {
    try {
        $call(new RdKafka\KafkaConsumer($conf));
    } catch (Exception $exception) {
        echo $exception->getMessage(), "\n";
    }
}
--EXPECT--
RdKafka\KafkaConsumer::__construct() has not been called, or RdKafka\KafkaConsumer::close() was already called
RdKafka\KafkaConsumer::__construct() has not been called, or RdKafka\KafkaConsumer::close() was already called
RdKafka\KafkaConsumer::__construct() has not been called, or RdKafka\KafkaConsumer::close() was already called
RdKafka\KafkaConsumer::__construct() has not been called, or RdKafka\KafkaConsumer::close() was already called
