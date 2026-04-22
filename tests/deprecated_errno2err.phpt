--TEST--
rd_kafka_errno2err() emits E_DEPRECATED
--FILE--
<?php
set_error_handler(function (int $errno, string $errstr): bool {
    if ($errno === E_DEPRECATED) {
        echo "Deprecated: $errstr\n";
    }
    return true;
}, E_DEPRECATED);

$result = rd_kafka_errno2err(0);

echo "OK\n";
--EXPECT--
Deprecated: Function rd_kafka_errno2err() is deprecated
OK
