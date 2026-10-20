# PHP Kafka client - php-rdkafka

[![Join the chat at https://gitter.im/arnaud-lb/php-rdkafka](https://badges.gitter.im/arnaud-lb/php-rdkafka.svg)](https://gitter.im/arnaud-lb/php-rdkafka?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

[![Supported librdkafka versions: >= 1.6.0](https://img.shields.io/badge/librdkafka-%3E%3D%201.6.0-blue.svg)](https://github.com/confluentinc/librdkafka/releases) [![Supported Kafka versions: >= 0.8](https://img.shields.io/badge/kafka-%3E%3D%200.8-blue.svg)](https://github.com/edenhill/librdkafka/blob/master/INTRODUCTION.md#broker-version-compatibility) ![Supported PHP versions: >= 8.1](https://img.shields.io/badge/php-%3E%3D%208.1-blue.svg)

PHP-rdkafka is a **stable**, **production-ready**, and **fast** Kafka client for PHP based on [librdkafka](https://github.com/edenhill/librdkafka).

Current version supports PHP >= 8.1.0, librdkafka >= 1.6.0, Kafka >= 0.8. Version [6.x](https://github.com/arnaud-lb/php-rdkafka/tree/6.x) supports PHP 7.x..8.x, librdkafka 0.11..2.x. Older versions support PHP 5.

The goal of the extension is to be a low-level un-opinionated librdkafka binding focused on production and long term support.

The high level and low level *consumers*, *producer*, and *metadata* APIs are supported.

Documentation is available [here](https://arnaud-lb.github.io/php-rdkafka/phpdoc/book.rdkafka.html).

## Table of Contents

1. [Installation](#installation)
2. [Examples](#examples)
3. [Usage](#usage)
   * [Producing](#producing)
   * [High-level consuming](#high-level-consuming)
   * [Low-level consuming (legacy)](#low-level-consuming-legacy)
   * [Low-level consuming from multiple topics / partitions (legacy)](#low-level-consuming-from-multiple-topics--partitions-legacy)
   * [Using stored offsets](#using-stored-offsets)
   * [Interesting configuration parameters](#interesting-configuration-parameters)
     * [queued.max.messages.kbytes](#queuedmaxmessageskbytes)
     * [topic.metadata.refresh.sparse and topic.metadata.refresh.interval.ms](#topicmetadatarefreshsparse-and-topicmetadatarefreshintervalms)
     * [internal.termination.signal](#internalterminationsignal)
4. [Documentation](#documentation)
5. [Credits](#credits)
6. [License](#license)

## Installation

https://arnaud-lb.github.io/php-rdkafka-doc/phpdoc/rdkafka.setup.html

## Supported Versions

7.x requires **PHP >= 8.1** and **librdkafka >= 1.6.0** (enforced at `configure` time; a build against an older librdkafka fails with `librdkafka version 1.6.0 or greater required`).

The test matrix builds and tests against librdkafka v1.6.1, v1.9.2, v2.6.0, and v2.14.1 (the v1.6.1 job exercises the floor), plus a non-blocking experimental build against librdkafka `master`. librdkafka 1.5.3, the previous floor, was the last release in the 1.5.x line (released 2020-12-09); librdkafka does not maintain long-term branches per minor version, so it received zero fixes after development moved on to 1.6.0.

### What your distro likely ships

If you're relying on your OS's packaged `librdkafka` rather than building it yourself or installing from Confluent's repositories, check the version against the floor above. As of this writing:

| Distro | librdkafka | Meets the 1.6.0 floor? |
|---|---|---|
| Ubuntu 22.04 (Jammy) | 1.8.0 | Yes |
| Ubuntu 24.04 (Noble) | 2.3.0 | Yes |
| Ubuntu 26.04 (Resolute) | 2.13.0 | Yes |
| Debian 11 (Bullseye) | 1.6.0 | Yes |
| Debian 12 (Bookworm) | 2.0.2 | Yes |
| Debian 13 (Trixie) | 2.8.0 | Yes |
| RHEL 8 family (AlmaLinux/Rocky/CentOS Stream 8) | 1.6.1 | Yes |
| RHEL 9 family (AlmaLinux/Rocky/CentOS Stream 9) | 1.6.1 | Yes |
| CentOS Stream 10 | 2.1.1 | Yes |
| Fedora (39–44) | 2.1.1 – 2.14.1 | Yes |
| Ubuntu 20.04 (Focal) | 1.2.1 | No |
| Amazon Linux 2 | 0.11.4 | No |
| CentOS 7 | 0.11.4 | No |
| openSUSE Leap 15.x | 0.11.6 | No |

Source: [repology.org](https://repology.org/project/librdkafka/versions). If your distro ships a version below the floor, you'll need to install librdkafka from [Confluent's repositories](https://github.com/confluentinc/librdkafka#installation), build it from source, or use a container image with a newer version.

Debian 11's stock package (1.6.0) sits exactly at the `configure`-enforced floor, one patch release below the v1.6.1 CI actually builds and tests. Debian 11's own free LTS support ended 2026-08-31; further updates require Debian's paid ELTS program.

## Examples

https://arnaud-lb.github.io/php-rdkafka-doc/phpdoc/rdkafka.examples.html

## Usage

Configuration parameters used below can be found in [Librdkafka Configuration reference](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md)

### Producing

#### Creating a producer
For producing, we first need to create a producer, and to add brokers (Kafka
servers) to it:

``` php
<?php
$conf = new RdKafka\Conf();
$conf->set('log_level', (string) LOG_DEBUG);
$conf->set('debug', 'all');
$rk = new RdKafka\Producer($conf);
$rk->addBrokers("10.0.0.1:9092,10.0.0.2:9092");
```

#### Producing messages

> **Warning** Make sure that your producer follows proper shutdown (see below) to not lose messages.  

Next, we create a topic instance from the producer:
``` php
<?php

$topic = $rk->newTopic("test");
```

From there, we can produce as much messages as we want, using the produce
method:
``` php
<?php

$topic->produce(RD_KAFKA_PARTITION_UA, 0, "Message payload");
```
The first argument is the partition. RD_KAFKA_PARTITION_UA stands for
*unassigned*, and lets librdkafka choose the partition.  
The second argument are message flags and should be either 0  
or `RD_KAFKA_MSG_F_BLOCK` to block produce on full queue.
The message payload can be anything.

#### Proper shutdown

This should be done prior to destroying a producer instance  
to make sure all queued and in-flight produce requests are completed  
before terminating. Use a reasonable value for `$timeout_ms`.  

> **Warning** Not calling flush can lead to message loss!

```php
$rk->flush($timeout_ms);
```

In case you don't care about sending messages that haven't been sent yet,
you can use `purge()` before calling `flush()`:

```php
// Forget messages that are not fully sent yet
$rk->purge(RD_KAFKA_PURGE_F_QUEUE);

$rk->flush($timeout_ms);
```

### High-level consuming

The RdKafka\KafkaConsumer class supports automatic partition assignment/revocation. See the example [here](https://arnaud-lb.github.io/php-rdkafka-doc/phpdoc/rdkafka.examples.html#example-1).

### Low-level consuming (legacy)

> **Note** The low-level consumer is a legacy API, please prefer using the high-level consumer

We first need to create a low level consumer, and to add brokers (Kafka
servers) to it:

``` php
<?php
$conf = new RdKafka\Conf();
$conf->set('log_level', (string) LOG_DEBUG);
$conf->set('debug', 'all');
$rk = new RdKafka\Consumer($conf);
$rk->addBrokers("10.0.0.1,10.0.0.2");
```

Next, create a topic instance by calling the `newTopic()` method, and start
consuming on partition 0:

``` php
<?php

$topic = $rk->newTopic("test");

// The first argument is the partition to consume from.
// The second argument is the offset at which to start consumption. Valid values
// are: RD_KAFKA_OFFSET_BEGINNING, RD_KAFKA_OFFSET_END, RD_KAFKA_OFFSET_STORED.
$topic->consumeStart(0, RD_KAFKA_OFFSET_BEGINNING);
```

Next, retrieve the consumed messages:

``` php
<?php

while (true) {
    // The first argument is the partition (again).
    // The second argument is the timeout.
    $msg = $topic->consume(0, 1000);
    if (null === $msg || $msg->err === RD_KAFKA_RESP_ERR__PARTITION_EOF) {
        // Constant check required by librdkafka 0.11.6. Newer librdkafka versions will return NULL instead.
        continue;
    } elseif ($msg->err) {
        echo $msg->errstr(), "\n";
        break;
    } else {
        echo $msg->payload, "\n";
    }
}
```

### Low-level consuming from multiple topics / partitions (legacy)

> **Note** The low-level consumer is a legacy API, please prefer using the high-level consumer

Consuming from multiple topics and/or partitions can be done by telling
librdkafka to forward all messages from these topics/partitions to an internal
queue, and then consuming from this queue:

Creating the queue:

``` php
<?php
$queue = $rk->newQueue();
```

Adding topic partitions to the queue:

``` php
<?php

$topic1 = $rk->newTopic("topic1");
$topic1->consumeQueueStart(0, RD_KAFKA_OFFSET_BEGINNING, $queue);
$topic1->consumeQueueStart(1, RD_KAFKA_OFFSET_BEGINNING, $queue);

$topic2 = $rk->newTopic("topic2");
$topic2->consumeQueueStart(0, RD_KAFKA_OFFSET_BEGINNING, $queue);
```

Next, retrieve the consumed messages from the queue:

``` php
<?php

while (true) {
    // The only argument is the timeout.
    $msg = $queue->consume(1000);
    if (null === $msg || $msg->err === RD_KAFKA_RESP_ERR__PARTITION_EOF) {
        // Constant check required by librdkafka 0.11.6. Newer librdkafka versions will return NULL instead.
        continue;
    } elseif ($msg->err) {
        echo $msg->errstr(), "\n";
        break;
    } else {
        echo $msg->payload, "\n";
    }
}
```

### Using stored offsets

#### Broker (default)
librdkafka per default stores offsets on the broker.

#### File offsets (deprecated)
If you're using local file for offset storage, then by default the file is created in the current directory, with a
name based on the topic and the partition. The directory can be changed by setting the ``offset.store.path``
[configuration property](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md).

### Consumer settings

#### Low-level consumer: auto commit settings
To manually control the offset, set `enable.auto.offset.store` to `false`.  
The settings `auto.commit.interval.ms` and `auto.commit.enable` will control  
if the stored offsets will be auto committed to the broker and in which interval.

#### High-level consumer: auto commit settings
To manually control the offset, set `enable.auto.commit` to `false`.

#### High level consumer: max.poll.interval.ms
Maximum allowed time between calls to consume messages for high-level consumers.  
If this interval is exceeded the consumer is considered failed and the group will  
rebalance in order to reassign the partitions to another consumer group member.

#### Consumer group id (general)
`group.id` is responsible for setting your consumer group ID and it should be unique (and should
not change). Kafka uses it to recognize applications and store offsets for them.

``` php
<?php

$topicConf = new RdKafka\TopicConf();
$topicConf->set("auto.commit.interval.ms", 1e3);

$topic = $rk->newTopic("test", $topicConf);

$topic->consumeStart(0, RD_KAFKA_OFFSET_STORED);
```

### Interesting configuration parameters

[Librdkafka Configuration reference](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md)

#### queued.max.messages.kbytes

librdkafka will buffer up to 1GB of messages for each consumed partition by default. You can lower memory usage by reducing the value of the ``queued.max.messages.kbytes`` parameter on your consumers.

### topic.metadata.refresh.sparse and topic.metadata.refresh.interval.ms

Each consumer and producer instance will fetch topics metadata at an interval defined by the ``topic.metadata.refresh.interval.ms`` parameter. Depending on your librdkafka version, the parameter defaults to 10 seconds, or 600 seconds.

librdkafka fetches the metadata for all topics of the cluster by default. Setting ``topic.metadata.refresh.sparse`` to the string ``"true"`` makes sure that librdkafka fetches only the topics he uses.

Setting ``topic.metadata.refresh.sparse`` to ``"true"``, and ``topic.metadata.refresh.interval.ms`` to 600 seconds (plus some jitter) can reduce the bandwidth a lot, depending on the number of consumers and topics.

### internal.termination.signal

This setting allows librdkafka threads to terminate as soon as librdkafka is done with them. This effectively allows your PHP processes / requests to terminate quickly.

When enabling this, you have to mask the signal like this:

``` php
<?php
// once
pcntl_sigprocmask(SIG_BLOCK, array(SIGIO));
// any time
$conf->set('internal.termination.signal', SIGIO);
```

### socket.blocking.max.ms (librdkafka < 1.0.0)

> Maximum time a broker socket operation may block. A lower value improves responsiveness at the expense of slightly higher CPU usage.

Reducing the value of this setting improves shutdown speed. The value defines the maximum time librdkafka will block in one iteration of a read loop. This also defines how often the main librdkafka thread will check for termination.

### queue.buffering.max.ms

This defines the maximum and default time librdkafka will wait before sending a batch of messages. Reducing this setting to e.g. 1ms ensures that messages are sent ASAP, instead of being batched.

This has been seen to reduce the shutdown time of the rdkafka instance, and of the PHP process / request.

## Performance / Low-latency settings

Here is a configuration optimized for low latency. This allows a PHP process / request to send messages ASAP and to terminate quickly.

``` php
<?php

$conf = new \RdKafka\Conf();
$conf->set('socket.timeout.ms', 50); // or socket.blocking.max.ms, depending on librdkafka version
if (function_exists('pcntl_sigprocmask')) {
    pcntl_sigprocmask(SIG_BLOCK, array(SIGIO));
    $conf->set('internal.termination.signal', SIGIO);
} else {
    $conf->set('queue.buffering.max.ms', 1);
}

$producer = new \RdKafka\Producer($conf);
$consumer = new \RdKafka\Consumer($conf);
```

It is advised to call poll at regular intervals to serve callbacks. In `php-rdkafka:3.x`  
poll was also called during shutdown, so not calling it in regular intervals might  
lead to a slightly longer shutdown. The example below polls until there are no more events in the queue:

```
$producer->produce(...);
while ($producer->getOutQLen() > 0) {
    $producer->poll(1);
}
```

## Known Issues

### Topic handles are not freed until the client is destroyed

`newTopic()` (on `Producer`, `Consumer`, and `KafkaConsumer`) creates a new
`rd_kafka_topic_t` handle for each distinct topic name. Calling `unset()` on
the returned `RdKafka\Topic` releases php-rdkafka's own reference, but
librdkafka topic objects are refcounted internally as well as by the
application, and librdkafka does not release its internal reference until the
owning client handle itself is destroyed:

> topic objects are refcounted (both internally and for the app) the topic
> object might not actually be destroyed by this call, but the application
> must consider the object destroyed.

From the [`rd_kafka_topic_destroy()`](https://github.com/confluentinc/librdkafka/blob/master/src/rdkafka.h) header comment.

In practice this means a long-lived `Producer`/`Consumer`/`KafkaConsumer` that
calls `newTopic()` with many distinct, one-off topic names will see memory
grow roughly linearly with the number of distinct names, and that memory is
only reclaimed when the client itself is destroyed — not when the `Topic`
object is unset, and not on any timer (this is unrelated to
`metadata.max.age.ms`, which only governs the separate protocol-level
metadata cache).

Calling `newTopic()` repeatedly with the *same* name is fine and does not
leak: librdkafka returns the existing cached handle rather than creating a
new one. **Workaround:** if your application produces to or consumes from a
bounded, known set of topics, create each `Topic` object once and reuse it
rather than calling `newTopic()` per operation.

## Documentation

https://arnaud-lb.github.io/php-rdkafka-doc/phpdoc/book.rdkafka.html  
The source of the documentation can be found [here](https://github.com/arnaud-lb/php-rdkafka-doc)

## Asking for Help

If the documentation is not enough, feel free to ask a questions on the php-rdkafka channels on [Gitter](https://gitter.im/arnaud-lb/php-rdkafka) or [Google Groups](https://groups.google.com/forum/#!forum/php-rdkafka).

## Stubs

Because your IDE is not able to auto discover php-rdkadka api you can consider usage of external package providing a set of stubs for php-rdkafka classes, functions and constants: [kwn/php-rdkafka-stubs](https://github.com/kwn/php-rdkafka-stubs)

## Contributing

If you would like to contribute, thank you :)

Before you start, please take a look at the [CONTRIBUTING document](https://github.com/arnaud-lb/php-rdkafka/blob/master/CONTRIBUTING.md) to see how to get your changes merged in.

## Credits

Documentation copied from [librdkafka](https://github.com/edenhill/librdkafka).

Authors: see [contributors](https://github.com/arnaud-lb/php-rdkafka/graphs/contributors).

## License

php-rdkafka is released under the [MIT](https://github.com/arnaud-lb/php-rdkafka/blob/master/LICENSE) license.
