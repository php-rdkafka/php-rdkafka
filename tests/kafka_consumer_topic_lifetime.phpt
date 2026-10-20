--TEST--
KafkaConsumerTopic keeps its parent alive and is invalidated on close
--FILE--
<?php

function createConsumer(string $groupId): RdKafka\KafkaConsumer
{
    $conf = new RdKafka\Conf();
    $conf->set('group.id', $groupId);

    // Silences librdkafka's missing-broker warning.
    $conf->set('log_level', '0');

    return new RdKafka\KafkaConsumer($conf);
}

$consumer = createConsumer('topic-parent-lifetime');
$topic = $consumer->newTopic('topic-parent-lifetime');
$reference = WeakReference::create($consumer);

unset($consumer);

echo "Topic keeps parent alive\n";
var_dump($reference->get() instanceof RdKafka\KafkaConsumer);

unset($topic);

echo "Releasing topic releases parent\n";
var_dump($reference->get() === null);

$consumer = createConsumer('topic-close-lifetime');
$topic = $consumer->newTopic('topic-close-lifetime');
$consumer->close();

echo "Topic is invalidated on close\n";
try {
    $topic->getName();
} catch (Exception $e) {
    echo get_class($e), "\n";
    echo $e->getMessage(), "\n";
}

?>
--EXPECT--
Topic keeps parent alive
bool(true)
Releasing topic releases parent
bool(true)
Topic is invalidated on close
Exception
RdKafka\Topic is not initialized or its client has been closed
