--TEST--
RdKafka\Queue notification descriptors are owned by the queue and released with it
--SKIPIF--
<?php
if (!method_exists(RdKafka\Queue::class, 'ioEventEnable')) {
    die('skip RdKafka\Queue::ioEventEnable() is not available on this platform');
}
if (!is_dir('/proc/self/fdinfo')) {
    die('skip requires /proc/self/fdinfo');
}
--FILE--
<?php

function notificationPair(): array
{
    [$read, $write] = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
    stream_set_blocking($read, false);
    stream_set_blocking($write, false);

    return [$read, $write];
}

function readNotification($read): string
{
    $readable = [$read];
    $writable = [];
    $exceptional = [];

    if (stream_select($readable, $writable, $exceptional, 0) !== 1) {
        return '';
    }

    return fread($read, 1024);
}

function queueDeliveryReport(RdKafka\Producer $producer): void
{
    // Purging a queued message enqueues its delivery report, and writes any
    // notification, before purge() returns. No broker is needed.
    $producer->newTopic('queue-io-event')->produce(RD_KAFKA_PARTITION_UA, 0, 'message');
    $producer->purge(RD_KAFKA_PURGE_F_QUEUE);
}

function descriptors(): array
{
    // Skips the descriptor scandir() used, which is closed by now
    return array_filter(scandir('/proc/self/fd'), fn ($descriptor) => is_numeric($descriptor) && isOpen($descriptor));
}

/* Registers the stream and returns the descriptor the queue added */
function register(RdKafka\Queue $queue, $stream): string
{
    $before = descriptors();
    $queue->ioEventEnable($stream);
    $added = array_values(array_diff(descriptors(), $before));

    if (count($added) !== 1) {
        throw new RuntimeException('Expected one new descriptor, got ' . count($added));
    }

    return $added[0];
}

function isOpen(string $descriptor): bool
{
    clearstatcache();

    return is_link("/proc/self/fd/$descriptor");
}

$conf = new RdKafka\Conf();
$conf->set('log_level', '0');
$conf->set('group.id', 'queue-io-event-lifetime');

$producer = new RdKafka\Producer($conf);
$queue = $producer->getMainQueue();
[$read, $write] = notificationPair();

echo "Owns a close-on-exec duplicate\n";
$descriptor = register($queue, $write);
preg_match('/^flags:\s+([0-7]+)$/m', file_get_contents("/proc/self/fdinfo/$descriptor"), $matches);
var_dump((octdec($matches[1]) & 02000000) !== 0);

echo "Replacing the stream releases the previous duplicate\n";
[$otherRead, $otherWrite] = notificationPair();
$otherDescriptor = register($queue, $otherWrite);
var_dump(isOpen($descriptor));
queueDeliveryReport($producer);
var_dump(readNotification($read), bin2hex(readNotification($otherRead)));
$producer->poll(0);

echo "Disabling releases the duplicate and can be repeated\n";
$queue->ioEventEnable(null);
$queue->ioEventEnable(null);
var_dump(isOpen($otherDescriptor));

echo "Closing the stream does not redirect notifications\n";
register($queue, $write);
fclose($write);
[$reusedRead, $reusedWrite] = notificationPair();
queueDeliveryReport($producer);
var_dump(bin2hex(readNotification($read)), readNotification($reusedRead), readNotification($reusedWrite));
$producer->poll(0);

echo "Disabling with a pending notification\n";
queueDeliveryReport($producer);
$queue->ioEventEnable(null);
var_dump(bin2hex(readNotification($read)));
$producer->poll(0);

echo "Releasing the queue releases the duplicate\n";
$descriptor = register($queue, $otherWrite);
unset($queue);
var_dump(isOpen($descriptor));
queueDeliveryReport($producer);
var_dump(readNotification($otherRead));
$producer->poll(0);

echo "Closing the client releases the duplicate\n";
$kafkaConsumer = new RdKafka\KafkaConsumer($conf);
$consumerQueue = $kafkaConsumer->getConsumerQueue();
$descriptor = register($consumerQueue, $otherWrite);
$kafkaConsumer->close();
var_dump(isOpen($descriptor));

?>
--EXPECT--
Owns a close-on-exec duplicate
bool(true)
Replacing the stream releases the previous duplicate
bool(false)
string(0) ""
string(2) "01"
Disabling releases the duplicate and can be repeated
bool(false)
Closing the stream does not redirect notifications
string(2) "01"
string(0) ""
string(0) ""
Disabling with a pending notification
string(2) "01"
Releasing the queue releases the duplicate
bool(false)
string(0) ""
Closing the client releases the duplicate
bool(false)
