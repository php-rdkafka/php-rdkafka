--TEST--
KafkaConsumer::subscribe() converts topic names without changing the given array
--FILE--
<?php

class TopicName
{
    public function __construct(private string $name)
    {
    }

    public function __toString(): string
    {
        return $this->name;
    }
}

class FailingTopicName
{
    public function __construct(public Throwable $exception)
    {
    }

    public function __toString(): string
    {
        throw $this->exception;
    }
}

$conf = new RdKafka\Conf();
$conf->set('group.id', 'subscribe_topics');
$conf->setLogCb(function () {});

$consumer = new RdKafka\KafkaConsumer($conf);

$name = new TopicName('from_object');
$reference = 'from_reference';
$topics = [$name, 42, &$reference];

$consumer->subscribe($topics);

var_dump($topics[0] === $name, $topics[1], $reference);
var_dump($consumer->getSubscription());

$failing = new FailingTopicName(new RuntimeException('conversion failed'));

try {
    $consumer->subscribe(['before', $failing, 'after']);
} catch (RuntimeException $exception) {
    printf("%s: %s\n", $exception->getMessage(), var_export($exception === $failing->exception, true));
}

set_error_handler(function (int $errno, string $errstr) {
    throw new ErrorException($errstr, 0, $errno);
});

try {
    $consumer->subscribe(['before', []]);
} catch (ErrorException $exception) {
    echo $exception->getMessage(), "\n";
}

restore_error_handler();

try {
    $consumer->subscribe(["with\0null"]);
} catch (ValueError $exception) {
    echo $exception->getMessage(), "\n";
}

var_dump($consumer->getSubscription());

$consumer->subscribe(['valid']);
var_dump($consumer->getSubscription());
--EXPECT--
bool(true)
int(42)
string(14) "from_reference"
array(3) {
  [0]=>
  string(2) "42"
  [1]=>
  string(11) "from_object"
  [2]=>
  string(14) "from_reference"
}
conversion failed: true
Array to string conversion
RdKafka\KafkaConsumer::subscribe(): Argument #1 ($topics) must not contain any null bytes
array(3) {
  [0]=>
  string(2) "42"
  [1]=>
  string(11) "from_object"
  [2]=>
  string(14) "from_reference"
}
array(1) {
  [0]=>
  string(5) "valid"
}
