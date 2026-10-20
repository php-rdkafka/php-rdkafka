--TEST--
RdKafka\Queue::ioEventEnable() rejects encrypted streams
--SKIPIF--
<?php
if (!method_exists(RdKafka\Queue::class, 'ioEventEnable')) {
    die('skip RdKafka\Queue::ioEventEnable() is not available on this platform');
}
if (!extension_loaded('openssl')) {
    die('skip requires the openssl extension');
}
--FILE--
<?php

// Raw notification bytes would corrupt the TLS stream, so it is rejected
$key = openssl_pkey_new(['private_key_type' => OPENSSL_KEYTYPE_EC, 'curve_name' => 'prime256v1']);
$certificate = openssl_csr_sign(openssl_csr_new(['commonName' => 'localhost'], $key), null, $key, 1);
openssl_x509_export($certificate, $certificatePem);
openssl_pkey_export($key, $keyPem);
$pemFile = tempnam(sys_get_temp_dir(), 'rdkafka-tls');
file_put_contents($pemFile, $certificatePem . $keyPem);

$listener = stream_socket_server('tcp://127.0.0.1:0', $errno, $errstr, STREAM_SERVER_BIND | STREAM_SERVER_LISTEN,
    stream_context_create(['ssl' => ['local_cert' => $pemFile]]));
$client = stream_socket_client('tcp://' . stream_socket_get_name($listener, false), $errno, $errstr, 5, STREAM_CLIENT_CONNECT,
    stream_context_create(['ssl' => ['verify_peer' => false, 'verify_peer_name' => false]]));
$server = stream_socket_accept($listener, 5);
stream_set_blocking($server, false);
stream_set_blocking($client, false);

$serverReady = $clientReady = false;
$deadline = microtime(true) + 5;
while (!($serverReady && $clientReady) && microtime(true) < $deadline) {
    $serverReady = $serverReady || stream_socket_enable_crypto($server, true, STREAM_CRYPTO_METHOD_TLS_SERVER) === true;
    $clientReady = $clientReady || stream_socket_enable_crypto($client, true, STREAM_CRYPTO_METHOD_TLS_CLIENT) === true;
}
unlink($pemFile);
var_dump($serverReady && $clientReady);

$conf = new RdKafka\Conf();
$conf->set('log_level', '0');
$producer = new RdKafka\Producer($conf);

try {
    $producer->getMainQueue()->ioEventEnable($client);
} catch (InvalidArgumentException $e) {
    echo $e->getMessage(), "\n";
}

?>
--EXPECT--
bool(true)
The stream must be a plain socket or pipe
