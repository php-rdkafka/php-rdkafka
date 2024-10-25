--TEST--
Oauthbearer
--SKIPIF--
<?php
require __DIR__ . '/integration-tests-check.php';
?>
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
if (false !== getenv('TEST_KAFKA_BROKER_VERSION')) {
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

// Test that refresh token with setting token failure will fail when getting metadata
$conf->setOauthbearerTokenRefreshCb(function ($producer) {
    echo "Setting token failure in refresh cb\n";
    $producer->oauthbearerSetTokenFailure('Token failure before getting metadata');
    $producer->poll(0);
});
$producer = new \RdKafka\Producer($conf);
$producer->poll(0);
$topicName = sprintf("test_rdkafka_%s", uniqid());
$topic = $producer->newTopic($topicName);
try {
    $producer->getMetadata(false, $topic, 10*1000);
    echo "FAIL: Did not catch exception after not setting or refreshing any token\n";
} catch (\RdKafka\Exception $e) {
    echo "Caught exception when getting metadata after not setting or refreshing any token\n";
}

// Test that setting token without refreshing will get metadata successfully
$conf->setOauthbearerTokenRefreshCb(function ($producer) {});
$producer = new \RdKafka\Producer($conf);
$token = generateJws();
$producer->oauthbearerSetToken($token['value'], $token['expiryMs'], $token['principal']);
$topicName = sprintf("test_rdkafka_%s", uniqid());
$topic = $producer->newTopic($topicName);
try {
    $producer->getMetadata(false, $topic, 10*1000);
    echo "Got metadata successfully\n";
} catch (\RdKafka\Exception $e) {
    echo "FAIL: Set token but still got exception \n";
    exit;
}

// Test that token refresh is called after token expires
$conf->setOauthbearerTokenRefreshCb(function ($producer) {
    echo "Refreshing token\n";
});
$producer = new \RdKafka\Producer($conf);
$token = generateJws('required-scope', 5);
$producer->oauthbearerSetToken($token['value'], $token['expiryMs'], $token['principal']);
$producer->poll(0);
echo "Polled with refresh\n";
sleep(1);
$producer->poll(0);
echo "Polled without refresh\n";
sleep(4);
$producer->poll(0);
echo "Polled with refresh\n";

// Test that tokens without required scope fail
$producer = new \RdKafka\Producer($conf);
$token = generateJws('not-required-scope');
$producer->oauthbearerSetToken($token['value'], $token['expiryMs'], $token['principal']);
$topicName = sprintf("test_rdkafka_%s", uniqid());
$topic = $producer->newTopic($topicName);
try {
    $producer->getMetadata(false, $topic, 10*1000);
    echo "FAIL: Exception not thrown as expected when using insufficient scope\n";
    exit;
} catch (\RdKafka\Exception $e) {
    echo "Caught expected exception with insufficient_scope\n";
}

// Test that setting token with extensions succeeds
$conf->setOauthbearerTokenRefreshCb(function ($producer) {});
$producer = new \RdKafka\Producer($conf);
$token = generateJws();
$producer->oauthbearerSetToken($token['value'], $token['expiryMs'], $token['principal'], ['testExtensionKey' => 'Test extension value']);
$producer->poll(0);

--EXPECT--
Refreshing token and succeeding
Metadata retrieved successfully when refresh callback set token
Setting token failure in refresh cb
Local: Authentication failure: Failed to acquire SASL OAUTHBEARER token: Token failure before getting metadata
Caught exception when getting metadata after not setting or refreshing any token
Got metadata successfully
Refreshing token
Polled with refresh
Polled without refresh
Refreshing token
Polled with refresh
Caught expected exception with insufficient_scope
