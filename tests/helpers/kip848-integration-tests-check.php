<?php

if (file_exists(__DIR__ . '/../test_env.php')) {
    include __DIR__ . '/../test_env.php';
}

// Once configured, the broker must support the consumer group protocol:
// connection and join errors fail the test instead of skipping it
if (!getenv('TEST_KAFKA_KIP848_BROKERS')) {
    die('skip due to missing TEST_KAFKA_KIP848_BROKERS');
}
