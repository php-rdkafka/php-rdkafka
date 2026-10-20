# Upgrading from 6.x to 7.x

## Summary of changes

**Minimum requirements raised.** PHP 7.x is no longer supported; PHP 8.1 or later is required. librdkafka 1.6.0 or later is required (previously 1.0.0).

**Compile-time feature flags removed.** Several methods were previously compiled in only when the installed librdkafka was new enough to support them (guarded by `#ifdef HAS_RD_KAFKA_OAUTHBEARER`, `HAS_RD_KAFKA_TRANSACTIONS`, `HAS_RD_KAFKA_PURGE`, `HAS_RD_KAFKA_CONTROLLERID`, `HAVE_RD_KAFKA_MESSAGE_HEADERS`, `HAS_RD_KAFKA_INCREMENTAL_ASSIGN`). Because the minimum librdkafka is now 1.6.0, which provides all of these features, the guards have been removed and the methods are always available.

**New methods on `KafkaConsumer`.** The high-level consumer gained `poll()`, `oauthbearerSetToken()`, `oauthbearerSetTokenFailure()`, `getRebalanceProtocol()`, and `getConsumerGroupMetadata()`, and now supports SASL/SSL OAUTHBEARER authentication end-to-end.

**Internal fixes.** A missing `zend_restore_error_handling()` call in the KafkaConsumer error path was corrected. Several internal type mismatches were fixed.

**Callback cycles are now collected.** Keep a reference to clients while using them and flush producers before releasing them. A callback that captures its client no longer keeps an otherwise unreachable client alive until request shutdown.

**`KafkaConsumer::close()` rejected inside callbacks.** The method now throws `RdKafka\Exception` when called from one of the consumer's callbacks. Close the consumer after the method that invoked the callback returns.

**Callback exceptions stop consumption and polling.** When a callback throws, the method that called it now returns before handing out another message or calling another callback. Messages and delivery reports that were not handed out stay queued for the next call. A consumer's error callback is the exception and keeps the previous behavior.

**PHP 7 compatibility shims removed.** Internal compatibility code for PHP 7 has been cleaned up; this has no effect on behaviour for PHP 8 users.

---

## User-impacting changes

### `KafkaConsumer::consume()` returns `null` on timeout

Previously, `KafkaConsumer::consume()` returned a `Message` object with `err` set to `RD_KAFKA_RESP_ERR__TIMED_OUT` when no message arrived within the timeout window. It now returns `null` in that case, matching librdkafka's own behavior.

**Before:**
```php
$msg = $consumer->consume(1000);
if ($msg->err === RD_KAFKA_RESP_ERR__TIMED_OUT) {
    // no message
}
```

**After:**
```php
$msg = $consumer->consume(1000);
if ($msg === null) {
    // no message
}
```

`RD_KAFKA_RESP_ERR__TIMED_OUT` on a returned `Message` now means an actual timeout error originating from librdkafka, not a poll window expiry.

### `KafkaConsumer::close()` cannot be called from a callback

Calling `KafkaConsumer::close()` from a consumer callback now throws `RdKafka\Exception` with code `RD_KAFKA_RESP_ERR__STATE`. Record that shutdown was requested in the callback, then close the consumer after the method that invoked the callback returns.

### `KafkaConsumer::close()` reports close errors

`close()` now throws `RdKafka\Exception` for a native close error instead of ignoring it. Handle this exception where your application closes consumers. Once closing starts, the consumer is released even if `close()` throws; do not retry it on that object. Calls rejected inside a callback or during another `close()` do not start closing the consumer.

### Callback exceptions stop consumption and polling

After catching a callback exception, call `poll()`, `consume()` or `consumeCallback()` again if processing is to continue. Events that were not handed out now stay queued for that next call. A consumer's error callback keeps the previous dispatch behavior. This does not recover messages already collected by `ConsumerTopic::consumeBatch()` before an exception.

### Clients cannot be constructed twice

Calling `__construct()` again on an `RdKafka\Producer`, `RdKafka\Consumer` or `RdKafka\KafkaConsumer` that was constructed successfully now throws `RdKafka\Exception` with code `RD_KAFKA_RESP_ERR__STATE`, including after `KafkaConsumer::close()`. Create a new object instead.

### Conf::dump() does not include topic-level properties

`Conf::set()` accepts both global and topic-level properties, silently routing topic-level properties to an embedded `default_topic_conf`. However, `Conf::dump()` only returns global configuration properties.

**If you were using Conf::dump() for debugging or testing** and expecting to see topic-level properties (like `auto.offset.reset`, `compression.codec`, `auto.commit.enable`, etc.), you must now explicitly access them via `getDefaultTopicConf()`.

```php
// Before (incorrect assumption):
$conf = new RdKafka\Conf;
$conf->set('group.id', 'my-group');           // global property
$conf->set('auto.offset.reset', 'earliest');  // topic-level property

$dump = $conf->dump();
// Incorrectly assumed 'auto.offset.reset' would appear here

// After (correct usage):
$conf = new RdKafka\Conf;
$conf->set('group.id', 'my-group');           // global property
$conf->set('auto.offset.reset', 'earliest');  // topic-level property

// Get global properties:
$globalProps = $conf->dump();
echo $globalProps['group.id'];  // 'my-group'

// Get topic-level properties:
$topicConf = $conf->getDefaultTopicConf();

if ($topicConf !== null) {
    $topicProps = $topicConf->dump();
    echo $topicProps['auto.offset.reset'];  // 'earliest'

    // Or get a single topic-level property:
    $value = $topicConf->get('auto.offset.reset');  // 'earliest'
}

// Alternatively, use Conf::get() for global properties and Conf::getDefaultTopicConf()->get() for topic-level properties:
$groupId = $conf->get('group.id');                                          // global
$autoOffsetReset = $conf->getDefaultTopicConf()->get('auto.offset.reset');  // topic-level
```

Note: This behavior existed in php-rdkafka 6.x but was not well-documented. Tests or debugging code that relied on dump() containing topic-level properties will need to be updated.
Refer to librdkafka CONFIGURATION.md (https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md) to determine which properties are topic-level (marked with * in the C/P column) vs global.

### PHP 8.1 now required

php-rdkafka 7.x requires PHP 8.1 or later. PHP 7.x is no longer supported.

### librdkafka 1.6.0 now required

librdkafka 1.6.0 is the new minimum. Versions older than 1.6.0 are not supported.

### Previously conditional methods are now always available

The following methods were only compiled in when the build-time librdkafka was sufficiently new. They are now unconditionally available (librdkafka 1.6.0 supports all of them):

| Class | Method |
|-------|--------|
| `RdKafka\Conf` | `setOauthbearerTokenRefreshCb()` |
| `RdKafka\Producer` | `purge()` |
| `RdKafka\Producer` | `initTransactions()`, `beginTransaction()`, `commitTransaction()`, `abortTransaction()` |
| `RdKafka\Producer` | `oauthbearerSetToken()`, `oauthbearerSetTokenFailure()` |
| `RdKafka\Producer` | `getControllerId()` |
| `RdKafka\KafkaConsumer` | `getControllerId()` |
| `RdKafka\KafkaConsumer` | `incrementalAssign()`, `incrementalUnassign()` |
| `RdKafka\ProducerTopic` | `producev()` |

If your code checked `method_exists()` before calling any of these, those guards can be removed.

### New methods on `KafkaConsumer`

`RdKafka\KafkaConsumer` gained five new methods:

```php
KafkaConsumer::poll(int $timeout_ms): int
KafkaConsumer::oauthbearerSetToken(string $token_value, int $lifetime_ms, string $principal_name, array $extensions = []): void
KafkaConsumer::oauthbearerSetTokenFailure(string $error): void
KafkaConsumer::getRebalanceProtocol(): string
KafkaConsumer::getConsumerGroupMetadata(): ConsumerGroupMetadata
```

`poll()` allows the high-level consumer to service callbacks (including the OAUTHBEARER token refresh callback) without consuming a message. This is the same method that exists on the low-level `RdKafka\Consumer`.

### New `ConsumerGroupMetadata` API

`RdKafka\ConsumerGroupMetadata` can be constructed with full group metadata on every supported librdkafka version. Its getter methods require librdkafka 2.8.0 and throw `RdKafka\Exception` on older versions.

### `RdKafka::setLogger()` and `rd_kafka_errno2err()` are deprecated

Both `RdKafka::setLogger()` and `rd_kafka_errno2err()` are deprecated in librdkafka and will be removed in a future version. Calling them now emits an `E_DEPRECATED` notice.

- Replace `$producer->setLogger(RD_KAFKA_LOG_PRINT)` (and similar) with `$conf->setLogCb(callable $callback)` set before constructing the producer or consumer.
- Replace `rd_kafka_errno2err($errno)` with `rd_kafka_last_error()`, which returns the last error code set by librdkafka directly without requiring an errno argument.

### SASL/SSL OAUTHBEARER support for `KafkaConsumer`

`RdKafka\KafkaConsumer` now fully supports OAUTHBEARER authentication, including over SASL_SSL. Set `setOauthbearerTokenRefreshCb()` on the `Conf` and use the new `oauthbearerSetToken()` / `oauthbearerSetTokenFailure()` methods on the consumer instance to provide tokens.
