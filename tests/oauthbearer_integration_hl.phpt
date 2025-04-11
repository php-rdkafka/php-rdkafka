--TEST--
Oauthbearer
--SKIPIF--
<?php
require __DIR__ . '/integration-tests-check.php';
RD_KAFKA_VERSION >= 0x01010000 || die("skip librdkafka too old does not support oauthbearer");
--FILE--
<?php
require __DIR__ . '/integration-tests-check.php';

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
if (RD_KAFKA_VERSION >= 0x090000 && false !== getenv('TEST_KAFKA_BROKER_VERSION')) {
    $conf->set('broker.version.fallback', getenv('TEST_KAFKA_BROKER_VERSION'));
}
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

try {
    $producer->getMetadata(false, $topic, 10*1000);
    echo "Metadata retrieved successfully when refresh callback set token\n";
} catch (\RdKafka\Exception $e) {
    echo "FAIL: Caught exception when getting metadata after successfully refreshing any token:\n";
    printf("%s: %s\n", get_class($e), $e->getMessage());
}

echo "Writing test data\n";
$topic->produce(RD_KAFKA_PARTITION_UA, 0, "Test");
$producer->poll(0);
$result = $producer->flush(10000);
echo RD_KAFKA_RESP_ERR_NO_ERROR === $result ? "Write successful\n" : "Write error\n";

$confConsumer = new RdKafka\Conf();
if (RD_KAFKA_VERSION >= 0x090000 && false !== getenv('TEST_KAFKA_BROKER_VERSION')) {
    $confConsumer->set('broker.version.fallback', getenv('TEST_KAFKA_BROKER_VERSION'));
}
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

// Test that refresh token with setting token accurately will succeed when getting metadata
$confConsumer->setOauthbearerTokenRefreshCb(function ($consumer) {
    echo "Refreshing token and succeeding\n";
    $token = generateJws();
    $consumer->oauthbearerSetToken($token['value'], (string) $token['expiryMs'], $token['principal']);
});

$consumer = new \RdKafka\KafkaConsumer($confConsumer);
$consumer->subscribe([$topicName]);
echo "Reading data\n";
$message = $consumer->consume(5500);
echo ($message->err === RD_KAFKA_RESP_ERR_NO_ERROR) ? "Read successful\n" : "Read Error\n";
echo $message->payload . "\n";

//// Test that refresh token with setting token failure will fail when getting metadata
//$conf->setOauthbearerTokenRefreshCb(function ($producer) {
//    echo "Setting token failure in refresh cb\n";
//    $producer->oauthbearerSetTokenFailure('Token failure before getting metadata');
//    $producer->poll(0);
//});
//$producer = new \RdKafka\Producer($conf);
//$producer->poll(0);
//$topicName = sprintf("test_rdkafka_%s", uniqid());
//$topic = $producer->newTopic($topicName);
//try {
//    $producer->getMetadata(false, $topic, 10*1000);
//    echo "FAIL: Did not catch exception after not setting or refreshing any token\n";
//} catch (\RdKafka\Exception $e) {
//    echo "Caught exception when getting metadata after not setting or refreshing any token\n";
//}
//
//// Test that setting token without refreshing will get metadata successfully
//$conf->setOauthbearerTokenRefreshCb(function ($producer) {});
//$producer = new \RdKafka\Producer($conf);
//$token = generateJws();
//$producer->oauthbearerSetToken($token['value'], $token['expiryMs'], $token['principal']);
//$topicName = sprintf("test_rdkafka_%s", uniqid());
//$topic = $producer->newTopic($topicName);
//try {
//    $producer->getMetadata(false, $topic, 10*1000);
//    echo "Got metadata successfully\n";
//} catch (\RdKafka\Exception $e) {
//    echo "FAIL: Set token but still got exception \n";
//    exit;
//}
//
//// Test that token refresh is called after token expires
//$conf->setOauthbearerTokenRefreshCb(function ($producer) {
//    echo "Refreshing token\n";
//});
//$producer = new \RdKafka\Producer($conf);
//$token = generateJws('required-scope', 5);
//$producer->oauthbearerSetToken($token['value'], $token['expiryMs'], $token['principal']);
//$producer->poll(0);
//echo "Polled with refresh\n";
//sleep(1);
//$producer->poll(0);
//echo "Polled without refresh\n";
//sleep(4);
//$producer->poll(0);
//echo "Polled with refresh\n";
//
//// Test that tokens without required scope fail
//$producer = new \RdKafka\Producer($conf);
//$token = generateJws('not-required-scope');
//$producer->oauthbearerSetToken($token['value'], $token['expiryMs'], $token['principal']);
//$topicName = sprintf("test_rdkafka_%s", uniqid());
//$topic = $producer->newTopic($topicName);
//try {
//    $producer->getMetadata(false, $topic, 10*1000);
//    echo "FAIL: Exception not thrown as expected when using insufficient scope\n";
//    exit;
//} catch (\RdKafka\Exception $e) {
//    echo "Caught expected exception with insufficient_scope\n";
//}
//
//// Test that setting token with extensions succeeds
//$conf->setOauthbearerTokenRefreshCb(function ($producer) {});
//$producer = new \RdKafka\Producer($conf);
//$token = generateJws();
//$producer->oauthbearerSetToken($token['value'], $token['expiryMs'], $token['principal'], ['testExtensionKey' => 'Test extension value']);
//$producer->poll(0);

//Setting token failure in refresh cb
//Local: Authentication failure: Failed to acquire SASL OAUTHBEARER token: Token failure before getting metadata
//Caught exception when getting metadata after not setting or refreshing any token--EXPECT--
//Got metadata successfullyRefreshing token and succeeding
//Refreshing tokenMetadata retrieved successfully when refresh callback set token
//Polled with refreshWriting test data
//Polled without refreshWrite successful
//Refreshing token

--EXPECT--
Refreshing token and succeeding
Metadata retrieved successfully when refresh callback set token
Writing test data
Write successful
Reading data
Refreshing token and succeeding
Read successful
Test
