--TEST--
KafkaConsumer::commit() keeps exceptions thrown while reading Message properties
--FILE--
<?php

class MagicMessage extends RdKafka\Message
{
    public function __construct(private string $failing, public Throwable $exception)
    {
    }

    public function __get(string $name): mixed
    {
        if ($name === $this->failing) {
            throw $this->exception;
        }

        return match ($name) {
            'topic_name' => str_repeat('topic', 2),
            'partition' => 0,
            'offset' => 0,
        };
    }
}

class InvalidErrorMessage extends RdKafka\Message
{
    public function __get(string $name): mixed
    {
        return ['invalid' => str_repeat('value', 2)];
    }
}

$conf = new RdKafka\Conf();
$conf->set('group.id', 'commit_message_properties');
$conf->setLogCb(function () {});

$consumer = new RdKafka\KafkaConsumer($conf);

foreach (['partition', 'offset'] as $failing) {
    $message = new MagicMessage($failing, new RuntimeException("$failing failed"));
    $message->err = RD_KAFKA_RESP_ERR_NO_ERROR;
    unset($message->topic_name, $message->partition, $message->offset);

    try {
        $consumer->commit($message);
    } catch (RuntimeException $exception) {
        printf("%s: %s\n", $exception->getMessage(), var_export($exception === $message->exception, true));
    }
}

$message = new InvalidErrorMessage();
unset($message->err);

try {
    $consumer->commit($message);
} catch (TypeError $exception) {
    echo get_class($exception), "\n";
}
--EXPECT--
partition failed: true
offset failed: true
TypeError
