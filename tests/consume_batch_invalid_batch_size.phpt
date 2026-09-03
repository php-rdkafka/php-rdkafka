--TEST--
RdKafka\ConsumerTopic::consumeBatch() rejects an out-of-range $batch_size
--FILE--
<?php

$conf = new RdKafka\Conf();
$conf->set('metadata.broker.list', '127.0.0.1:9092');

$consumer = new RdKafka\Consumer($conf);
$topic = $consumer->newTopic('test');

foreach ([0, -1, PHP_INT_MAX] as $batchSize) {
    try {
        $topic->consumeBatch(0, 0, $batchSize);
        echo "no exception for {$batchSize}\n";
    } catch (\InvalidArgumentException $e) {
        echo $e->getMessage(), "\n";
    }
}
--EXPECTF--
Out of range value '0' for batch_size
Out of range value '-1' for batch_size
Out of range value '%d' for batch_size
