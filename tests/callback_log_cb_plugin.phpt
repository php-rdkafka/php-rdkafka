--TEST--
Conf::setLogCb() ignores plugin loading logs
--SKIPIF--
<?php
$features = explode(',', (new RdKafka\Conf())->get('builtin.features'));
if (!in_array('plugins', $features)) die('skip librdkafka built without plugin support');
--FILE--
<?php

$conf = new RdKafka\Conf();
$conf->set('debug', 'plugin');
$conf->set('log_level', '7');
$conf->setLogCb(function ($kafka, $level, $facility, $message) {
    echo "$facility: $message\n";
});

try {
    $conf->set('plugin.library.paths', '/nonexistent/plugin');
} catch (RdKafka\Exception $e) {
    printf("Caught a %s: %s\n", get_class($e), $e->getMessage());
}
--EXPECTF--
Caught a RdKafka\Exception: %s (plugin /nonexistent/plugin)
