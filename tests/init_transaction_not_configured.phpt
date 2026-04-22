--TEST--
initTransaction() not configured
--SKIPIF--
<?php
require __DIR__ . '/helpers/integration-tests-check.php';
if (!class_exists("RdKafka\\KafkaErrorException")) {
    echo "skip";
}
--FILE--
<?php
require __DIR__ . '/helpers/integration-tests-check.php';

$conf = new RdKafka\Conf();
$conf->set('metadata.broker.list', getenv('TEST_KAFKA_BROKERS'));

$producer = new RdKafka\Producer($conf);

try {
    $producer->initTransactions(10000);
} catch (RdKafka\KafkaErrorException $e) {
    echo $e->getMessage() . PHP_EOL;
    echo $e->getCode() . PHP_EOL;
    echo $e->getFile() . PHP_EOL;
    echo $e->getLine() . PHP_EOL;
}

--EXPECTF--
The Transactional API requires transactional.id to be configured (RD_KAFKA_RESP_ERR__NOT_CONFIGURED)
-145
%s/tests/init_transaction_not_configured.php
10
