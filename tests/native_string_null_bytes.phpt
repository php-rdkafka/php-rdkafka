--TEST--
Names, configuration and credentials containing null bytes are rejected
--FILE--
<?php

class ExtensionValue
{
    public function __toString(): string
    {
        return "extension\0value";
    }
}

$conf = new RdKafka\Conf();
$conf->set('group.id', 'native_string_null_bytes');
$conf->set('security.protocol', 'SASL_PLAINTEXT');
$conf->set('sasl.mechanisms', 'OAUTHBEARER');
$conf->setLogCb(function () {});

$topicConf = new RdKafka\TopicConf();
$topicConf->set('auto.offset.reset', 'earliest');

$producer = new RdKafka\Producer($conf);
$lowLevelConsumer = new RdKafka\Consumer($conf);
$consumer = new RdKafka\KafkaConsumer($conf);
$topicPartition = new RdKafka\TopicPartition('valid', 0);

$message = new RdKafka\Message();
$message->err = RD_KAFKA_RESP_ERR_NO_ERROR;
$message->topic_name = "topic\0name";
$message->partition = 0;
$message->offset = 0;

$calls = [
    fn () => $producer->newTopic("topic\0name"),
    fn () => $lowLevelConsumer->newTopic("topic\0name"),
    fn () => $consumer->newTopic("topic\0name"),
    fn () => new RdKafka\TopicPartition("topic\0name", 0),
    fn () => $topicPartition->setTopic("topic\0name"),
    fn () => $producer->queryWatermarkOffsets("topic\0name", 0, $low, $high, 0),
    fn () => $consumer->queryWatermarkOffsets("topic\0name", 0, $low, $high, 0),
    fn () => $consumer->commit($message),
    fn () => $conf->set("group.id\0", 'other'),
    fn () => $conf->set('group.id', "other\0group"),
    fn () => $conf->get("group.id\0"),
    fn () => $topicConf->set('auto.offset.reset', "latest\0"),
    fn () => $topicConf->get("auto.offset.reset\0"),
    fn () => $producer->addBrokers("localhost:9092\0,other:9092"),
    fn () => new RdKafka\ConsumerGroupMetadata("group\0id"),
    fn () => new RdKafka\ConsumerGroupMetadata('group', 1, "member\0id"),
    fn () => new RdKafka\ConsumerGroupMetadata('group', 1, 'member', "instance\0id"),
];

foreach ([$producer, $consumer] as $client) {
    $calls[] = fn () => $client->oauthbearerSetToken("token\0value", 1000, 'principal');
    $calls[] = fn () => $client->oauthbearerSetToken('token', 1000, "principal\0name");
    $calls[] = fn () => $client->oauthbearerSetToken('token', 1000, 'principal', ['first' => 'value', "second\0key" => 'value']);
    $calls[] = fn () => $client->oauthbearerSetToken('token', 1000, 'principal', ['first' => 'value', 'second' => "extension\0value"]);
    $calls[] = fn () => $client->oauthbearerSetToken('token', 1000, 'principal', ['first' => 'value', 'second' => new ExtensionValue()]);
}

foreach ($calls as $call) {
    try {
        $call();
    } catch (ValueError|RdKafka\Exception $exception) {
        printf("%s: %s\n", get_class($exception), $exception->getMessage());
    }
}

var_dump($conf->get('group.id'));
var_dump($topicConf->get('auto.offset.reset'));
var_dump($producer->newTopic('valid')->getName());
var_dump($consumer->newTopic('valid')->getName());
var_dump($topicPartition->getTopic());
--EXPECT--
ValueError: RdKafka::newTopic(): Argument #1 ($topic_name) must not contain any null bytes
ValueError: RdKafka::newTopic(): Argument #1 ($topic_name) must not contain any null bytes
ValueError: RdKafka\KafkaConsumer::newTopic(): Argument #1 ($topic_name) must not contain any null bytes
ValueError: RdKafka\TopicPartition::__construct(): Argument #1 ($topic) must not contain any null bytes
ValueError: RdKafka\TopicPartition::setTopic(): Argument #1 ($topic_name) must not contain any null bytes
ValueError: RdKafka::queryWatermarkOffsets(): Argument #1 ($topic) must not contain any null bytes
ValueError: RdKafka\KafkaConsumer::queryWatermarkOffsets(): Argument #1 ($topic) must not contain any null bytes
RdKafka\Exception: Invalid argument: Specified Message's topic_name must not contain any null bytes
ValueError: RdKafka\Conf::set(): Argument #1 ($name) must not contain any null bytes
ValueError: RdKafka\Conf::set(): Argument #2 ($value) must not contain any null bytes
ValueError: RdKafka\Conf::get(): Argument #1 ($name) must not contain any null bytes
ValueError: RdKafka\TopicConf::set(): Argument #2 ($value) must not contain any null bytes
ValueError: RdKafka\TopicConf::get(): Argument #1 ($name) must not contain any null bytes
ValueError: RdKafka::addBrokers(): Argument #1 ($broker_list) must not contain any null bytes
ValueError: RdKafka\ConsumerGroupMetadata::__construct(): Argument #1 ($group_id) must not contain any null bytes
ValueError: RdKafka\ConsumerGroupMetadata::__construct(): Argument #3 ($member_id) must not contain any null bytes
ValueError: RdKafka\ConsumerGroupMetadata::__construct(): Argument #4 ($group_instance_id) must not contain any null bytes
ValueError: RdKafka::oauthbearerSetToken(): Argument #1 ($token_value) must not contain any null bytes
ValueError: RdKafka::oauthbearerSetToken(): Argument #3 ($principal_name) must not contain any null bytes
ValueError: RdKafka::oauthbearerSetToken(): Argument #4 ($extensions) must not contain any null bytes
ValueError: RdKafka::oauthbearerSetToken(): Argument #4 ($extensions) must not contain any null bytes
ValueError: RdKafka::oauthbearerSetToken(): Argument #4 ($extensions) must not contain any null bytes
ValueError: RdKafka\KafkaConsumer::oauthbearerSetToken(): Argument #1 ($token_value) must not contain any null bytes
ValueError: RdKafka\KafkaConsumer::oauthbearerSetToken(): Argument #3 ($principal_name) must not contain any null bytes
ValueError: RdKafka\KafkaConsumer::oauthbearerSetToken(): Argument #4 ($extensions) must not contain any null bytes
ValueError: RdKafka\KafkaConsumer::oauthbearerSetToken(): Argument #4 ($extensions) must not contain any null bytes
ValueError: RdKafka\KafkaConsumer::oauthbearerSetToken(): Argument #4 ($extensions) must not contain any null bytes
string(24) "native_string_null_bytes"
string(8) "smallest"
string(5) "valid"
string(5) "valid"
string(5) "valid"
