--TEST--
RdKafka::setLogger() emits E_DEPRECATED
--FILE--
<?php
set_error_handler(function (int $errno, string $errstr): bool {
    if ($errno === E_DEPRECATED) {
        echo "Deprecated: $errstr\n";
    }
    return true;
}, E_DEPRECATED);

$conf = new RdKafka\Conf();
$conf->setLogCb(function () {});
$producer = new RdKafka\Producer($conf);
$producer->setLogger(RD_KAFKA_LOG_PRINT);

echo "OK\n";
--EXPECTF--
Deprecated: Method RdKafka::setLogger() is deprecated
OK
%A
