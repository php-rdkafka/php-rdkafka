# Upgrading from 6.x to 7.x

## Summary of changes

**Minimum requirements raised.** PHP 7.x is no longer supported; PHP 8.1 or later is required. librdkafka 1.5.3 or later is required (previously 1.0.0).

**Compile-time feature flags removed.** Several methods were previously compiled in only when the installed librdkafka was new enough to support them (guarded by `#ifdef HAS_RD_KAFKA_OAUTHBEARER`, `HAS_RD_KAFKA_TRANSACTIONS`, `HAS_RD_KAFKA_PURGE`, `HAS_RD_KAFKA_CONTROLLERID`, `HAVE_RD_KAFKA_MESSAGE_HEADERS`). Because the minimum librdkafka is now 1.5.3, which provides all of these features, the guards have been removed and the methods are always available.

**New methods on `KafkaConsumer`.** The high-level consumer gained `poll()`, `oauthbearerSetToken()`, and `oauthbearerSetTokenFailure()`, and now supports SASL/SSL OAUTHBEARER authentication end-to-end.

**Internal fixes.** A missing `zend_restore_error_handling()` call in the KafkaConsumer error path was corrected. Several internal type mismatches were fixed.

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

### PHP 8.1 now required

php-rdkafka 7.x requires PHP 8.1 or later. PHP 7.x is no longer supported.

### librdkafka 1.5.3 now required

librdkafka 1.5.3 is the new minimum. Versions older than 1.5.3 are not supported.

### Previously conditional methods are now always available

The following methods were only compiled in when the build-time librdkafka was sufficiently new. They are now unconditionally available (librdkafka 1.5.3 supports all of them):

| Class | Method |
|-------|--------|
| `RdKafka\Conf` | `setOauthbearerTokenRefreshCb()` |
| `RdKafka\Producer` | `purge()` |
| `RdKafka\Producer` | `initTransactions()`, `beginTransaction()`, `commitTransaction()`, `abortTransaction()` |
| `RdKafka\Producer` | `oauthbearerSetToken()`, `oauthbearerSetTokenFailure()` |
| `RdKafka\Producer` | `getControllerId()` |
| `RdKafka\KafkaConsumer` | `getControllerId()` |
| `RdKafka\ProducerTopic` | `producev()` |

If your code checked `method_exists()` before calling any of these, those guards can be removed.

### New methods on `KafkaConsumer`

`RdKafka\KafkaConsumer` gained three new methods:

```php
KafkaConsumer::poll(int $timeout_ms): int
KafkaConsumer::oauthbearerSetToken(string $token_value, int $lifetime_ms, string $principal_name, array $extensions = []): void
KafkaConsumer::oauthbearerSetTokenFailure(string $error): void
```

`poll()` allows the high-level consumer to service callbacks (including the OAUTHBEARER token refresh callback) without consuming a message. This is the same method that exists on the low-level `RdKafka\Consumer`.

### SASL/SSL OAUTHBEARER support for `KafkaConsumer`

`RdKafka\KafkaConsumer` now fully supports OAUTHBEARER authentication, including over SASL_SSL. Set `setOauthbearerTokenRefreshCb()` on the `Conf` and use the new `oauthbearerSetToken()` / `oauthbearerSetTokenFailure()` methods on the consumer instance to provide tokens.
