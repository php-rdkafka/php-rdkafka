--TEST--
Invalid topic partition list arguments throw a TypeError
--FILE--
<?php

$conf = new RdKafka\Conf();
$conf->set('group.id', 'topic_partition_list_argument');
$conf->setLogCb(function () {});

$consumer = new RdKafka\KafkaConsumer($conf);
$lowLevelConsumer = new RdKafka\Consumer($conf);
$producer = new RdKafka\Producer($conf);

$valid = new RdKafka\TopicPartition('topic_partition_list_argument', 0);

$calls = [
    fn ($topicPartitions) => $consumer->assign($topicPartitions),
    fn ($topicPartitions) => $consumer->commit($topicPartitions),
    fn ($topicPartitions) => $lowLevelConsumer->pausePartitions($topicPartitions),
    fn ($topicPartitions) => $producer->sendOffsetsToTransaction($topicPartitions, new RdKafka\ConsumerGroupMetadata('group'), 0),
];

foreach ($calls as $call) {
    foreach ([[$valid, 'invalid'], [$valid, new stdClass()]] as $topicPartitions) {
        try {
            $call($topicPartitions);
        } catch (TypeError $exception) {
            echo $exception->getMessage(), "\n";
        }
    }
}

foreach (['invalid', new stdClass()] as $argument) {
    try {
        $consumer->commit($argument);
    } catch (TypeError $exception) {
        echo $exception->getMessage(), "\n";
    }
}

$invalid = 'invalid';

try {
    $consumer->assign([&$invalid]);
} catch (TypeError $exception) {
    echo $exception->getMessage(), "\n";
}

$topicPartitions = [$valid];

foreach ($topicPartitions as &$topicPartition) {
}

$consumer->assign($topicPartitions);
var_dump(count($consumer->getAssignment()));
--EXPECT--
RdKafka\KafkaConsumer::assign(): Argument #1 ($topic_partitions) must be an array of RdKafka\TopicPartition, at least one element is a(n) string
RdKafka\KafkaConsumer::assign(): Argument #1 ($topic_partitions) must be an array of RdKafka\TopicPartition, at least one element is a(n) stdClass
RdKafka\KafkaConsumer::commit(): Argument #1 ($message_or_offsets) must be an array of RdKafka\TopicPartition, at least one element is a(n) string
RdKafka\KafkaConsumer::commit(): Argument #1 ($message_or_offsets) must be an array of RdKafka\TopicPartition, at least one element is a(n) stdClass
RdKafka::pausePartitions(): Argument #1 ($topic_partitions) must be an array of RdKafka\TopicPartition, at least one element is a(n) string
RdKafka::pausePartitions(): Argument #1 ($topic_partitions) must be an array of RdKafka\TopicPartition, at least one element is a(n) stdClass
RdKafka\Producer::sendOffsetsToTransaction(): Argument #1 ($offsets) must be an array of RdKafka\TopicPartition, at least one element is a(n) string
RdKafka\Producer::sendOffsetsToTransaction(): Argument #1 ($offsets) must be an array of RdKafka\TopicPartition, at least one element is a(n) stdClass
RdKafka\KafkaConsumer::commit(): Argument #1 ($message_or_offsets) must be of type RdKafka\Message|array|null, string given
RdKafka\KafkaConsumer::commit(): Argument #1 ($message_or_offsets) must be of type RdKafka\Message|array|null, stdClass given
RdKafka\KafkaConsumer::assign(): Argument #1 ($topic_partitions) must be an array of RdKafka\TopicPartition, at least one element is a(n) string
int(1)
