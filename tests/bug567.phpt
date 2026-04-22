--TEST--
Bug #567
--SKIPIF--
<?php
RD_KAFKA_VERSION >= 0x01010000 || die("skip librdkafka too old does not support oauthbearer");
--FILE--
<?php

$conf = new RdKafka\Conf();
$conf->set('metadata.broker.list', 'foobar');
$conf->set('security.protocol', 'SASL_PLAINTEXT');
$conf->set('sasl.mechanisms', 'OAUTHBEARER');
$conf->setOauthbearerTokenRefreshCb(function ($producer) {
    echo "oauthbearer token refresh callback successfully called\n";
});
$producer = new \RdKafka\Producer($conf);
$producer->poll(0);
echo "producer polled successfully\n";

--EXPECT--
oauthbearer token refresh callback successfully called
producer polled successfully
