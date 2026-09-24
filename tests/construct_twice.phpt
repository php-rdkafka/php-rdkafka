--TEST--
Clients reject repeated construction but can retry a failed construction
--FILE--
<?php

/* Retries a failed construction once the configuration has been fixed */
class RetryingProducer extends RdKafka\Producer
{
    public function __construct(RdKafka\Conf $conf)
    {
        try {
            parent::__construct($conf);
        } catch (RdKafka\Exception $e) {
            echo "Producer construction failed\n";
            $conf->set('acks', 'all');
            parent::__construct($conf);
        }
    }
}

class RetryingKafkaConsumer extends RdKafka\KafkaConsumer
{
    public function __construct(RdKafka\Conf $conf)
    {
        try {
            parent::__construct($conf);
        } catch (RdKafka\Exception $e) {
            echo "KafkaConsumer construction failed\n";
            $conf->set('group.id', 'construct-twice');
            $conf->set('fetch.max.bytes', '2000000');
            parent::__construct($conf);
        }
    }
}

function createConf(): RdKafka\Conf
{
    $conf = new RdKafka\Conf();

    // Silences librdkafka's missing-broker warning.
    $conf->set('log_level', '0');
    $conf->set('group.id', 'construct-twice');
    $conf->setErrorCb(function () { });

    return $conf;
}

function constructAgain(object $client, RdKafka\Conf $conf): void
{
    try {
        $client->__construct($conf);
    } catch (RdKafka\Exception $e) {
        if ($e->getCode() !== RD_KAFKA_RESP_ERR__STATE) {
            throw $e;
        }

        echo $e->getMessage(), "\n";
    }
}

$producer = new RdKafka\Producer(createConf());
constructAgain($producer, createConf());
var_dump($producer->getOutQLen());

$consumer = new RdKafka\Consumer(createConf());
constructAgain($consumer, createConf());
var_dump($consumer->newQueue() instanceof RdKafka\Queue);

$kafkaConsumer = new RdKafka\KafkaConsumer(createConf());
constructAgain($kafkaConsumer, createConf());
var_dump($kafkaConsumer->getSubscription());

$kafkaConsumer->close();
constructAgain($kafkaConsumer, createConf());

// librdkafka rejects the configuration
$conf = createConf();
$conf->set('enable.idempotence', 'true');
$conf->set('acks', '1');
$producer = new RetryingProducer($conf);
var_dump($producer->getOutQLen());

// The group is missing
$conf = new RdKafka\Conf();
$conf->set('log_level', '0');
$conf->setErrorCb(function () { });
$kafkaConsumer = new RetryingKafkaConsumer($conf);
var_dump($kafkaConsumer->getSubscription());
$kafkaConsumer->close();

// librdkafka rejects the configuration
$conf = createConf();
$conf->set('message.max.bytes', '2000000');
$conf->set('fetch.max.bytes', '1000000');
$kafkaConsumer = new RetryingKafkaConsumer($conf);
var_dump($kafkaConsumer->getSubscription());

?>
--EXPECT--
RdKafka\Producer::__construct() has already been called
int(0)
RdKafka\Consumer::__construct() has already been called
bool(true)
RdKafka\KafkaConsumer::__construct() has already been called
array(0) {
}
RdKafka\KafkaConsumer::__construct() has already been called
Producer construction failed
int(0)
KafkaConsumer construction failed
array(0) {
}
KafkaConsumer construction failed
array(0) {
}
