<?php

if (file_exists(__DIR__ . '/../test_env.php')) {
    include __DIR__ . '/../test_env.php';
}

if (getenv('TEST_KAFKA_BROKERS')) {
    return;
}

die('skip due to missing TEST_KAFKA_BROKERS environment & no test_env.php');
