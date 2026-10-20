--TEST--
KafkaConsumer::close() cannot be called from a callback
--FILE--
<?php

function closeFromCallback(RdKafka\KafkaConsumer $consumer, string $context): void
{
    try {
        $consumer->close();
    } catch (RdKafka\Exception $e) {
        if ($e->getCode() !== RD_KAFKA_RESP_ERR__STATE) {
            throw $e;
        }

        echo "$context: ", $e->getMessage(), "\n";
    }
}

final class CloseOnDestruct
{
    public function __construct(private RdKafka\KafkaConsumer $consumer)
    {
    }

    public function __destruct()
    {
        closeFromCallback($this->consumer, 'Return cleanup');
    }
}

$conf = new RdKafka\Conf();
$conf->set('group.id', 'close-in-callback');
$conf->set('statistics.interval.ms', '1');
$conf->set('log_level', '0');

$otherConf = new RdKafka\Conf();
$otherConf->set('group.id', 'close-other-from-callback');
$otherConf->set('log_level', '0');
$other = new RdKafka\KafkaConsumer($otherConf);

$calls = 0;
$conf->setStatsCb(function (RdKafka\KafkaConsumer $consumer) use (&$calls, $other): ?CloseOnDestruct {
    $calls++;

    if ($calls === 1) {
        $other->close();
        echo "Other consumer closed from callback\n";

        $consumer->consume(50);
        closeFromCallback($consumer, 'Outer callback');

        return new CloseOnDestruct($consumer);
    }

    if ($calls === 2) {
        closeFromCallback($consumer, 'Nested callback');
    }

    return null;
});

$consumer = new RdKafka\KafkaConsumer($conf);
$consumer->consume(100);

var_dump($calls >= 2);

$consumer->close();
echo "Closed after callback\n";

?>
--EXPECT--
Other consumer closed from callback
Nested callback: RdKafka\KafkaConsumer::close() cannot be called from a callback
Outer callback: RdKafka\KafkaConsumer::close() cannot be called from a callback
Return cleanup: RdKafka\KafkaConsumer::close() cannot be called from a callback
bool(true)
Closed after callback
