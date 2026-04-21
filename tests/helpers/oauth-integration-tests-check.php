<?php

if (file_exists(__DIR__ . '/../test_env.php')) {
    include __DIR__ . '/../test_env.php';
}

$broker = getenv('TEST_KAFKA_OAUTH_BROKERS');
if (!$broker) {
    die('skip due to missing TEST_KAFKA_OAUTH_BROKERS');
}

[$host, $port] = explode(':', $broker) + [1 => 9092];
$socket = @fsockopen($host, (int) $port, $errno, $errstr, 1);
if (!$socket) {
    die("skip OAuth broker not reachable at $broker");
}
fclose($socket);
