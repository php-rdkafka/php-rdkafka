--TEST--
Message::errstr() reads err through __get() and references
--FILE--
<?php

class MagicMessage extends RdKafka\Message
{
    public function __construct(public mixed $value)
    {
    }

    public function __get(string $name): mixed
    {
        return $this->value;
    }
}

$message = new MagicMessage(RD_KAFKA_RESP_ERR__PARTITION_EOF);
unset($message->err);
var_dump($message->errstr() === rd_kafka_err2str(RD_KAFKA_RESP_ERR__PARTITION_EOF));

$message = new MagicMessage(['invalid' => str_repeat('value', 2)]);
unset($message->err);

try {
    $message->errstr();
} catch (TypeError $exception) {
    echo get_class($exception), "\n";
}

$message = new RdKafka\Message();
$message->err = RD_KAFKA_RESP_ERR__PARTITION_EOF;
$err = &$message->err;
var_dump($message->errstr() === rd_kafka_err2str(RD_KAFKA_RESP_ERR__PARTITION_EOF));
--EXPECT--
bool(true)
TypeError
bool(true)
