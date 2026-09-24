--TEST--
RdKafka\Queue::ioEventEnable() validates the stream and notifies it
--SKIPIF--
<?php
if (!method_exists(RdKafka\Queue::class, 'ioEventEnable')) {
    die('skip RdKafka\Queue::ioEventEnable() is not available on this platform');
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

function expectException(callable $callback): void
{
    try {
        $callback();
        echo "No exception\n";
    } catch (Throwable $e) {
        echo get_class($e), ': ', $e->getMessage(), "\n";
    }
}

/* A write filter that records whether it was ever run */
class RecordingFilter extends php_user_filter
{
    public static bool $ran = false;

    public function filter($in, $out, &$consumed, bool $closing): int
    {
        self::$ran = true;

        while ($bucket = stream_bucket_make_writeable($in)) {
            $consumed += $bucket->datalen;
            stream_bucket_append($out, $bucket);
        }

        return PSFS_PASS_ON;
    }
}

class UserStream
{
    public $context;

    public function stream_open(string $path, string $mode, int $options, ?string &$opened_path): bool
    {
        return true;
    }

    public function stream_cast(int $cast_as)
    {
        return $GLOBALS['userStreamTarget'];
    }
}

$conf = new RdKafka\Conf();
$conf->set('log_level', '0');

$reports = 0;
$conf->setDrMsgCb(function () use (&$reports) {
    $reports++;
});

$producer = new RdKafka\Producer($conf);
$queue = $producer->getMainQueue();

echo "Notifies a valid stream\n";
[$read, $write] = notificationPair();
$queue->ioEventEnable($write);
queueDeliveryReport($producer);
var_dump(bin2hex(readNotification($read)));
$producer->poll(0);
var_dump($reports);

echo "Uses the payload\n";
$queue->ioEventEnable($write, 'ready');
queueDeliveryReport($producer);
var_dump(readNotification($read));
$producer->poll(0);

echo "Rejects invalid streams\n";
[$blockingRead, $blockingWrite] = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
expectException(fn () => $queue->ioEventEnable($blockingWrite));

$file = tmpfile();
expectException(fn () => $queue->ioEventEnable($file));

$memory = fopen('php://memory', 'w+');
expectException(fn () => $queue->ioEventEnable($memory));

$pipe = popen('exit 0', 'r');
stream_set_blocking($pipe, false);
expectException(fn () => $queue->ioEventEnable($pipe));
pclose($pipe);

// Rejecting a filtered stream must not flush it through its filters
stream_filter_register('queue-io-event.recording', RecordingFilter::class);
[$filteredRead, $filteredWrite] = notificationPair();
stream_filter_append($filteredWrite, 'queue-io-event.recording', STREAM_FILTER_WRITE);
expectException(fn () => $queue->ioEventEnable($filteredWrite));
var_dump(RecordingFilter::$ran);

$filteredPipe = popen('cat > /dev/null', 'w');
stream_set_blocking($filteredPipe, false);
stream_filter_append($filteredPipe, 'queue-io-event.recording', STREAM_FILTER_WRITE);
expectException(fn () => $queue->ioEventEnable($filteredPipe));
var_dump(RecordingFilter::$ran);
pclose($filteredPipe);

[$userRead, $userStreamTarget] = notificationPair();
stream_wrapper_register('queueioevent', UserStream::class);
$userStream = fopen('queueioevent://notifications', 'w');
expectException(fn () => $queue->ioEventEnable($userStream));

expectException(fn () => $queue->ioEventEnable(stream_context_create()));

[$closedRead, $closedWrite] = notificationPair();
fclose($closedWrite);
expectException(fn () => $queue->ioEventEnable($closedWrite));

expectException(fn () => $queue->ioEventEnable($write, ''));

echo "A rejected stream keeps the previous registration\n";
queueDeliveryReport($producer);
var_dump(readNotification($read));
$producer->poll(0);

echo "Disabling ignores the payload and can be repeated\n";
$queue->ioEventEnable(null, '');
$queue->ioEventEnable(null);
queueDeliveryReport($producer);
var_dump(readNotification($read));
$producer->poll(0);

var_dump($reports);

?>
--EXPECT--
Notifies a valid stream
string(2) "01"
int(1)
Uses the payload
string(5) "ready"
Rejects invalid streams
InvalidArgumentException: The stream must be non-blocking
InvalidArgumentException: The stream must be a plain socket or pipe
InvalidArgumentException: The stream must be a plain socket or pipe
InvalidArgumentException: The stream must be writable
InvalidArgumentException: The stream must be a plain socket or pipe
bool(false)
InvalidArgumentException: The stream must be a plain socket or pipe
bool(false)
InvalidArgumentException: The stream must be a plain socket or pipe
TypeError: RdKafka\Queue::ioEventEnable(): supplied resource is not a valid stream resource
TypeError: RdKafka\Queue::ioEventEnable(): supplied resource is not a valid stream resource
InvalidArgumentException: The notification payload must not be empty
A rejected stream keeps the previous registration
string(5) "ready"
Disabling ignores the payload and can be repeated
string(0) ""
int(4)
