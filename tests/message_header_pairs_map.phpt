--TEST--
Message::getHeaderPairs() uses the headers map of messages not created by the extension
--FILE--
<?php

class StringableHeader
{
    public function __construct(private RdKafka\Message $message)
    {
    }

    public function __toString(): string
    {
        $this->message->headers = [];

        return 'converted';
    }
}

class ThrowingHeader
{
    public function __toString(): string
    {
        throw new RuntimeException('Conversion failed');
    }
}

$message = new RdKafka\Message();
var_dump($message->getHeaderPairs());

$message->headers = ['name' => 'value', 0 => 1, 'null' => null];
var_dump($message->getHeaderPairs() === [['name', 'value'], ['0', '1'], ['null', '']]);

$message->headers = ['object' => new StringableHeader($message), 'after' => 'value'];
var_dump($message->getHeaderPairs() === [['object', 'converted'], ['after', 'value']]);

$headers = ['before' => 'value', 'rejected' => new ThrowingHeader()];
$message->headers = $headers;

try {
    $message->getHeaderPairs();
} catch (RuntimeException $exception) {
    echo $exception->getMessage(), "\n";
}

var_dump($message->headers === $headers);

// Serialized before Message had its private header property.
$message = unserialize('O:15:"RdKafka\Message":2:{s:3:"err";i:0;s:7:"headers";a:1:{s:4:"name";s:5:"value";}}');
var_dump($message->getHeaderPairs() === [['name', 'value']]);

class MagicMessage extends RdKafka\Message
{
    public ?Throwable $exception = null;

    public function __get(string $name): mixed
    {
        if ($this->exception !== null) {
            throw $this->exception;
        }

        return ['magic' => str_repeat('value', 2)];
    }
}

$message = new MagicMessage();
unset($message->headers);
var_dump($message->getHeaderPairs() === [['magic', 'valuevalue']]);

$message->exception = new RuntimeException('Read failed');

try {
    $message->getHeaderPairs();
} catch (RuntimeException $exception) {
    var_dump($exception === $message->exception);
}
--EXPECT--
array(0) {
}
bool(true)
bool(true)
Conversion failed
bool(true)
bool(true)
bool(true)
bool(true)
