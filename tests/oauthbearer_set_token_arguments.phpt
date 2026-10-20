--TEST--
oauthbearerSetToken() rejects invalid arguments before setting the token
--FILE--
<?php

class FailingValue
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
$conf->set('group.id', 'oauthbearer_set_token_arguments');
$conf->set('security.protocol', 'SASL_PLAINTEXT');
$conf->set('sasl.mechanisms', 'OAUTHBEARER');
$conf->setLogCb(function () {});

// librdkafka rejects these expired lifetimes and reports the converted value.
$lifetimes = [1000, 1000.9, -1000.9, '1000', " \t1000", '-1000'];

$invalidLifetimes = ['', ' ', '10ms', "1000\0", '9223372036854775808', '-9223372036854775809', NAN, INF, -INF, 9223372036854775808.0, -9223372036854777856.0];

$unsupportedLifetimes = [null, true, [], new stdClass()];

foreach ([new RdKafka\Producer($conf), new RdKafka\KafkaConsumer($conf)] as $client) {
    $method = new ReflectionMethod($client, 'oauthbearerSetToken');
    printf("%s: %s\n", get_class($client), $method->getParameters()[1]->getType());

    foreach (array_merge($lifetimes, $invalidLifetimes, $unsupportedLifetimes) as $lifetime) {
        try {
            $client->oauthbearerSetToken('token', $lifetime, 'principal');
        } catch (Exception|TypeError $exception) {
            printf("%s: %s\n", get_class($exception), $exception->getMessage());
        }
    }

    $extensions = ['first' => 42, 'second' => new FailingValue(new RuntimeException('conversion failed'))];

    try {
        $client->oauthbearerSetToken('token', 1000, 'principal', $extensions);
    } catch (RuntimeException $exception) {
        printf("%s: %s\n", $exception->getMessage(), var_export($exception === $extensions['second']->exception, true));
    }

    var_dump($extensions['first']);

    set_error_handler(function (int $errno, string $errstr) {
        throw new ErrorException($errstr, 0, $errno);
    });

    try {
        $client->oauthbearerSetToken('token', 1000, 'principal', ['first' => 'value', 'second' => []]);
    } catch (ErrorException $exception) {
        echo $exception->getMessage(), "\n";
    }

    restore_error_handler();

    $client->oauthbearerSetToken('token', (time() + 60) * 1000, 'principal', ['first' => 'value']);
    echo "Token set\n";
}
--EXPECTF--
RdKafka\Producer: string|int|float
RdKafka\Exception: Must supply an unexpired token: now=%dms, exp=1000ms
RdKafka\Exception: Must supply an unexpired token: now=%dms, exp=1000ms
RdKafka\Exception: Must supply an unexpired token: now=%dms, exp=-1000ms
RdKafka\Exception: Must supply an unexpired token: now=%dms, exp=1000ms
RdKafka\Exception: Must supply an unexpired token: now=%dms, exp=1000ms
RdKafka\Exception: Must supply an unexpired token: now=%dms, exp=-1000ms
InvalidArgumentException: RdKafka::oauthbearerSetToken(): Argument #2 ($lifetime_ms) must be a valid integer
InvalidArgumentException: RdKafka::oauthbearerSetToken(): Argument #2 ($lifetime_ms) must be a valid integer
InvalidArgumentException: RdKafka::oauthbearerSetToken(): Argument #2 ($lifetime_ms) must be a valid integer
InvalidArgumentException: RdKafka::oauthbearerSetToken(): Argument #2 ($lifetime_ms) must be a valid integer
InvalidArgumentException: RdKafka::oauthbearerSetToken(): Argument #2 ($lifetime_ms) must be a valid integer
InvalidArgumentException: RdKafka::oauthbearerSetToken(): Argument #2 ($lifetime_ms) must be a valid integer
InvalidArgumentException: RdKafka::oauthbearerSetToken(): Argument #2 ($lifetime_ms) must be a valid integer
InvalidArgumentException: RdKafka::oauthbearerSetToken(): Argument #2 ($lifetime_ms) must be a valid integer
InvalidArgumentException: RdKafka::oauthbearerSetToken(): Argument #2 ($lifetime_ms) must be a valid integer
InvalidArgumentException: RdKafka::oauthbearerSetToken(): Argument #2 ($lifetime_ms) must be a valid integer
InvalidArgumentException: RdKafka::oauthbearerSetToken(): Argument #2 ($lifetime_ms) must be a valid integer
TypeError: RdKafka::oauthbearerSetToken(): Argument #2 ($lifetime_ms) must be of type int|float|string, null given
TypeError: RdKafka::oauthbearerSetToken(): Argument #2 ($lifetime_ms) must be of type int|float|string, bool given
TypeError: RdKafka::oauthbearerSetToken(): Argument #2 ($lifetime_ms) must be of type int|float|string, array given
TypeError: RdKafka::oauthbearerSetToken(): Argument #2 ($lifetime_ms) must be of type int|float|string, stdClass given
conversion failed: true
int(42)
Array to string conversion
Token set
RdKafka\KafkaConsumer: string|int|float
RdKafka\Exception: Must supply an unexpired token: now=%dms, exp=1000ms
RdKafka\Exception: Must supply an unexpired token: now=%dms, exp=1000ms
RdKafka\Exception: Must supply an unexpired token: now=%dms, exp=-1000ms
RdKafka\Exception: Must supply an unexpired token: now=%dms, exp=1000ms
RdKafka\Exception: Must supply an unexpired token: now=%dms, exp=1000ms
RdKafka\Exception: Must supply an unexpired token: now=%dms, exp=-1000ms
InvalidArgumentException: RdKafka\KafkaConsumer::oauthbearerSetToken(): Argument #2 ($lifetime_ms) must be a valid integer
InvalidArgumentException: RdKafka\KafkaConsumer::oauthbearerSetToken(): Argument #2 ($lifetime_ms) must be a valid integer
InvalidArgumentException: RdKafka\KafkaConsumer::oauthbearerSetToken(): Argument #2 ($lifetime_ms) must be a valid integer
InvalidArgumentException: RdKafka\KafkaConsumer::oauthbearerSetToken(): Argument #2 ($lifetime_ms) must be a valid integer
InvalidArgumentException: RdKafka\KafkaConsumer::oauthbearerSetToken(): Argument #2 ($lifetime_ms) must be a valid integer
InvalidArgumentException: RdKafka\KafkaConsumer::oauthbearerSetToken(): Argument #2 ($lifetime_ms) must be a valid integer
InvalidArgumentException: RdKafka\KafkaConsumer::oauthbearerSetToken(): Argument #2 ($lifetime_ms) must be a valid integer
InvalidArgumentException: RdKafka\KafkaConsumer::oauthbearerSetToken(): Argument #2 ($lifetime_ms) must be a valid integer
InvalidArgumentException: RdKafka\KafkaConsumer::oauthbearerSetToken(): Argument #2 ($lifetime_ms) must be a valid integer
InvalidArgumentException: RdKafka\KafkaConsumer::oauthbearerSetToken(): Argument #2 ($lifetime_ms) must be a valid integer
InvalidArgumentException: RdKafka\KafkaConsumer::oauthbearerSetToken(): Argument #2 ($lifetime_ms) must be a valid integer
TypeError: RdKafka\KafkaConsumer::oauthbearerSetToken(): Argument #2 ($lifetime_ms) must be of type int|float|string, null given
TypeError: RdKafka\KafkaConsumer::oauthbearerSetToken(): Argument #2 ($lifetime_ms) must be of type int|float|string, bool given
TypeError: RdKafka\KafkaConsumer::oauthbearerSetToken(): Argument #2 ($lifetime_ms) must be of type int|float|string, array given
TypeError: RdKafka\KafkaConsumer::oauthbearerSetToken(): Argument #2 ($lifetime_ms) must be of type int|float|string, stdClass given
conversion failed: true
int(42)
Array to string conversion
Token set
