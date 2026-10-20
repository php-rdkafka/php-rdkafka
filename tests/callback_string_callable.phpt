--TEST--
Callbacks can be given as function names
--FILE--
<?php

function log_cb($kafka, $level, $facility, $message)
{
    $GLOBALS['logged'] = true;
}

$conf = new RdKafka\Conf();
$conf->set('debug', 'generic');

foreach (['setErrorCb', 'setDrMsgCb', 'setStatsCb', 'setRebalanceCb', 'setConsumeCb', 'setOffsetCommitCb', 'setOauthbearerTokenRefreshCb'] as $setter) {
    $conf->$setter('var_dump');
    $conf->$setter('var_dump');
}

$conf->setLogCb('log_cb');
$conf->setLogCb('log_cb');

$producer = new RdKafka\Producer($conf);
unset($conf);

$logged = false;
$producer->poll(0);
var_dump($logged);

$conf = new RdKafka\Conf();
$conf->setLogCb('log_cb');

$consumer = new RdKafka\Consumer($conf);
$topic = $consumer->newTopic('callback_string_callable');
var_dump($topic->consumeCallback(0, 0, 'var_dump'));
--EXPECT--
bool(true)
int(-1)
