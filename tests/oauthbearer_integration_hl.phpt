--TEST--
Oauthbearer
--SKIPIF--
<?php
require __DIR__ . '/helpers/oauth-integration-tests-check.php';
RD_KAFKA_VERSION >= 0x01010000 || die("skip librdkafka too old does not support oauthbearer");
--FILE--
<?php
require __DIR__ . '/helpers/oauth-integration-tests-check.php';

function generateJws($scope = 'required-scope', $expiresInSeconds = 60)
{
    $nowSeconds = floor(microtime(true));
    $expirySeconds = ($nowSeconds + $expiresInSeconds);
    $expiryMs = $expirySeconds * 1000;

    $principal = 'admin';
    $claimsJson = sprintf(
        '{"sub": "%s", "exp": %d, "iat": %d, "scope": "%s"}',
        $principal,
        $expirySeconds,
        $nowSeconds - 10,
        $scope
    );
    $headerJwsSegment = 'eyJhbGciOiJub25lIn0';
    $claimsJwsSegment = base64_encode($claimsJson);
    $claimsJwsSegment = rtrim(strtr($claimsJwsSegment, '+/', '-_'), '=');

    $jws = sprintf('%s.%s.', $headerJwsSegment, $claimsJwsSegment);

    return [
        'value' => $jws,
        'principal' => $principal,
        'expiryMs' => $expiryMs,
    ];
}

// Set up tests
$conf = new RdKafka\Conf();
$conf->set('metadata.broker.list', getenv('TEST_KAFKA_OAUTH_BROKERS'));
$conf->set('security.protocol', 'SASL_PLAINTEXT');
$conf->set('sasl.mechanisms', 'OAUTHBEARER');
$conf->set('sasl.oauthbearer.config', 'principal=admin');
$conf->setLogCb(function ($kafka, $level, $facility, $message) {});
$conf->setErrorCb(function ($producer, $err, $errstr) {
    printf("%s: %s\n", rd_kafka_err2str($err), $errstr);
});

// Test that refresh token with setting token accurately will succeed when getting metadata
$conf->setOauthbearerTokenRefreshCb(function ($producer) {
    echo "Refreshing token and succeeding\n";
    $token = generateJws();
    $producer->oauthbearerSetToken($token['value'], (string) $token['expiryMs'], $token['principal']);
});
$producer = new \RdKafka\Producer($conf);
$producer->poll(0);
$topicName = sprintf("test_rdkafka_%s", uniqid());
$topic = $producer->newTopic($topicName);

echo "Writing test data\n";
$topic->produce(RD_KAFKA_PARTITION_UA, 0, "Test");
$producer->poll(0);
$result = $producer->flush(10000);
echo RD_KAFKA_RESP_ERR_NO_ERROR === $result ? "Write successful\n" : "Write error\n";

$confConsumer = new RdKafka\Conf();
$confConsumer->set('metadata.broker.list', getenv('TEST_KAFKA_OAUTH_BROKERS'));
$confConsumer->set('security.protocol', 'SASL_PLAINTEXT');
$confConsumer->set('sasl.mechanisms', 'OAUTHBEARER');
$confConsumer->set('sasl.oauthbearer.config', 'principal=admin');
$confConsumer->set('group.id', 'test_group');
$confConsumer->set('auto.offset.reset', 'earliest');
$confConsumer->setLogCb(function ($kafka, $level, $facility, $message) {});
$confConsumer->setErrorCb(function ($producer, $err, $errstr) {
    printf("%s: %s\n", rd_kafka_err2str($err), $errstr);
});

// Test that refresh token with setting token accurately will succeed when consuming data
$confConsumer->setOauthbearerTokenRefreshCb(function ($consumer) {
    echo "Refreshing token and succeeding\n";
    $token = generateJws();
    $consumer->oauthbearerSetToken($token['value'], (string) $token['expiryMs'], $token['principal']);
});

$consumer = new \RdKafka\KafkaConsumer($confConsumer);
$consumer->subscribe([$topicName]);
echo "Reading data\n";
$message = $consumer->consume(10*1000);
echo ($message->err === RD_KAFKA_RESP_ERR_NO_ERROR) ? "Read successful\n" : "Read Error: " . $message->errstr() . "\n";
echo $message->payload . "\n";

// Test that refresh token with setting token failure will fail when trying to read data
$confConsumer->setOauthbearerTokenRefreshCb(function ($consumer) {
    echo "Setting token failure in refresh cb\n";
    $consumer->oauthbearerSetTokenFailure('Token failure before data consumption');
});
$confConsumer->set('group.id', 'test_group_fail');

$consumer = new \RdKafka\KafkaConsumer($confConsumer);
$consumer->subscribe([$topicName]);
echo "Reading data\n";

$message = $consumer->consume(10*1000);
echo $message->err === -185 ? "Received empty message when reading data after not setting or refreshing any token\n" :
    "FAIL: Did receive a message after not setting or refreshing any token\n";

// Test that metadata will be loaded before data consumption, under the condition that poll is called
$confConsumer->setOauthbearerTokenRefreshCb(function ($consumer) {
    echo "Refreshing token on poll and succeeding\n";
    $token = generateJws();
    $consumer->oauthbearerSetToken($token['value'], (string) $token['expiryMs'], $token['principal']);
});
$consumer = new \RdKafka\KafkaConsumer($confConsumer);
$consumerTopic = $consumer->newTopic($topicName);
$consumer->poll(0);

try {
    echo "Reading metadata\n";
    $consumer->getMetadata(false, $consumerTopic, 10*1000);
    echo "Metadata was fetched successfully after calling poll\n";
} catch (\RdKafka\Exception $e) {
    echo "FAIL: Caught exception when getting metadata after calling poll:\n";
    echo $e, "\n";
}

--EXPECT--
Refreshing token and succeeding
Writing test data
Write successful
Reading data
Refreshing token and succeeding
Read successful
Test
Reading data
Setting token failure in refresh cb
Local: Authentication failure: Failed to acquire SASL OAUTHBEARER token: Token failure before data consumption
Received empty message when reading data after not setting or refreshing any token
Refreshing token on poll and succeeding
Reading metadata
Metadata was fetched successfully after calling poll
