/*
  +----------------------------------------------------------------------+
  | php-rdkafka                                                          |
  +----------------------------------------------------------------------+
  | Copyright (c) 2016 Arnaud Le Blanc                                   |
  +----------------------------------------------------------------------+
  | This source file is subject to version 3.01 of the PHP license,      |
  | that is bundled with this package in the file LICENSE, and is        |
  | available through the world-wide-web at the following url:           |
  | http://www.php.net/license/3_01.txt                                  |
  | If you did not receive a copy of the PHP license and are unable to   |
  | obtain it through the world-wide-web, please send a note to          |
  | license@php.net so we can mail you a copy immediately.               |
  +----------------------------------------------------------------------+
  | Author: Arnaud Le Blanc <arnaud.lb@gmail.com>                        |
  +----------------------------------------------------------------------+
*/

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "php.h"
#include "php_rdkafka.h"
#include "php_rdkafka_priv.h"
#include "librdkafka/rdkafka.h"
#include "ext/spl/spl_iterators.h"
#include "Zend/zend_interfaces.h"
#include "Zend/zend_exceptions.h"
#include "ext/spl/spl_exceptions.h"
#include "topic.h"
#include "queue.h"
#include "message.h"
#include "topic_arginfo.h"

static zend_object_handlers object_handlers;
zend_class_entry * ce_kafka_consumer_topic;
zend_class_entry * ce_kafka_kafka_consumer_topic;
zend_class_entry * ce_kafka_producer_topic;
zend_class_entry * ce_kafka_topic;

typedef struct _php_callback {
    zend_fcall_info fci;
    zend_fcall_info_cache fcc;
} php_callback;

static void kafka_topic_free(zend_object *object) /* {{{ */
{
    kafka_topic_object *intern = php_kafka_from_obj(kafka_topic_object, object);

    if (Z_TYPE(intern->zrk) != IS_UNDEF && intern->rkt) {
        kafka_object *kafka_intern = get_kafka_object(&intern->zrk);
        if (kafka_intern) {
            zend_hash_index_del(&kafka_intern->topics, (zend_ulong)intern);
        }
    } else if (intern->rkt) {
        rd_kafka_topic_destroy(intern->rkt);
    }

    zend_object_std_dtor(&intern->std);
}
/* }}} */

static zend_object *kafka_topic_new(zend_class_entry *class_type) /* {{{ */
{
    zend_object* retval;
    kafka_topic_object *intern;

    intern = zend_object_alloc(sizeof(*intern), class_type);
    zend_object_std_init(&intern->std, class_type);
    object_properties_init(&intern->std, class_type);

    retval = &intern->std;
    retval->handlers = &object_handlers;

    return retval;
}
/* }}} */


static void consume_callback(rd_kafka_message_t *msg, void *opaque)
{
    php_callback *cb = (php_callback*) opaque;
    zval args[1];

    if (!opaque) {
        return;
    }

    if (!cb) {
        return;
    }

    ZVAL_NULL(&args[0]);

    kafka_message_new(&args[0], msg, NULL);

    rdkafka_call_function(&cb->fci, &cb->fcc, NULL, 1, args);

    zval_ptr_dtor(&args[0]);
}

kafka_topic_object * get_kafka_topic_object(zval *zrkt)
{
    kafka_topic_object *orkt = Z_RDKAFKA_P(kafka_topic_object, zrkt);

    if (!orkt->rkt) {
        zend_throw_exception_ex(NULL, 0, "RdKafka\\Topic::__construct() has not been called");
        return NULL;
    }

    return orkt;
}

/* {{{ proto RdKafka\ConsumerTopic::consumeCallback([int $partition, int timeout_ms, mixed $callback]) */
PHP_METHOD(RdKafka_ConsumerTopic, consumeCallback)
{
    php_callback cb;
    zend_long partition;
    zend_long timeout_ms;
    long result;
    kafka_topic_object *intern;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "llf", &partition, &timeout_ms, &cb.fci, &cb.fcc) == FAILURE) {
        return;
    }

    if (partition < 0 || partition > 0x7FFFFFFF) {
        zend_throw_exception_ex(spl_ce_InvalidArgumentException, 0, "Out of range value '" ZEND_LONG_FMT "' for $partition", partition);
        return;
    }

    intern = get_kafka_topic_object(getThis());
    if (!intern) {
        return;
    }

    Z_ADDREF_P(&cb.fci.function_name);

    result = rd_kafka_consume_callback(intern->rkt, partition, timeout_ms, consume_callback, &cb);

    zval_ptr_dtor(&cb.fci.function_name);

    RETURN_LONG(result);
}
/* }}} */

/* {{{ proto void RdKafka\ConsumerTopic::consumeQueueStart(int $partition, int $offset, RdKafka\Queue $queue)
 * Same as consumeStart(), but re-routes incoming messages to the provided queue */
PHP_METHOD(RdKafka_ConsumerTopic, consumeQueueStart)
{
    zval *zrkqu;
    kafka_topic_object *intern;
    kafka_queue_object *queue_intern;
    zend_long partition;
    zend_long offset;
    int ret;
    rd_kafka_resp_err_t err;
    kafka_object *kafka_intern;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "llO", &partition, &offset, &zrkqu, ce_kafka_queue) == FAILURE) {
        return;
    }

    if (partition != RD_KAFKA_PARTITION_UA && (partition < 0 || partition > 0x7FFFFFFF)) {
        zend_throw_exception_ex(spl_ce_InvalidArgumentException, 0, "Out of range value '" ZEND_LONG_FMT "' for $partition", partition);
        return;
    }

    intern = get_kafka_topic_object(getThis());
    if (!intern) {
        return;
    }

    queue_intern = get_kafka_queue_object(zrkqu);
    if (!queue_intern) {
        return;
    }

    kafka_intern = get_kafka_object(&intern->zrk);
    if (!kafka_intern) {
        return;
    }

    if (is_consuming_toppar(kafka_intern, intern->rkt, partition)) {
        zend_throw_exception_ex(
            ce_kafka_exception,
            0,
            "%s:" ZEND_LONG_FMT " is already being consumed by the same Consumer instance",
            rd_kafka_topic_name(intern->rkt),
            partition
        );
        return;
    }

    ret = rd_kafka_consume_start_queue(intern->rkt, partition, offset, queue_intern->rkqu);

    if (ret == -1) {
        err = rd_kafka_last_error();
        zend_throw_exception(ce_kafka_exception, rd_kafka_err2str(err), err);
        return;
    }

    add_consuming_toppar(kafka_intern, intern->rkt, partition);
}
/* }}} */

/* {{{ proto void RdKafka\ConsumerTopic::consumeStart(int partition, int offset)
   Start consuming messages */
PHP_METHOD(RdKafka_ConsumerTopic, consumeStart)
{
    kafka_topic_object *intern;
    zend_long partition;
    zend_long offset;
    int ret;
    rd_kafka_resp_err_t err;
    kafka_object *kafka_intern;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "ll", &partition, &offset) == FAILURE) {
        return;
    }

    if (partition != RD_KAFKA_PARTITION_UA && (partition < 0 || partition > 0x7FFFFFFF)) {
        zend_throw_exception_ex(spl_ce_InvalidArgumentException, 0, "Out of range value '" ZEND_LONG_FMT "' for $partition", partition);
        return;
    }

    intern = get_kafka_topic_object(getThis());
    if (!intern) {
        return;
    }

    kafka_intern = get_kafka_object(&intern->zrk);
    if (!kafka_intern) {
        return;
    }

    if (is_consuming_toppar(kafka_intern, intern->rkt, partition)) {
        zend_throw_exception_ex(
            ce_kafka_exception,
            0,
            "%s:" ZEND_LONG_FMT " is already being consumed by the same Consumer instance",
            rd_kafka_topic_name(intern->rkt),
            partition
        );
        return;
    }

    ret = rd_kafka_consume_start(intern->rkt, partition, offset);

    if (ret == -1) {
        err = rd_kafka_last_error();
        zend_throw_exception(ce_kafka_exception, rd_kafka_err2str(err), err);
        return;
    }

    add_consuming_toppar(kafka_intern, intern->rkt, partition);
}
/* }}} */

/* {{{ proto void RdKafka\ConsumerTopic::consumeStop(int partition)
   Stop consuming messages */
PHP_METHOD(RdKafka_ConsumerTopic, consumeStop)
{
    kafka_topic_object *intern;
    zend_long partition;
    int ret;
    rd_kafka_resp_err_t err;
    kafka_object *kafka_intern;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "l", &partition) == FAILURE) {
        return;
    }

    if (partition != RD_KAFKA_PARTITION_UA && (partition < 0 || partition > 0x7FFFFFFF)) {
        zend_throw_exception_ex(spl_ce_InvalidArgumentException, 0, "Out of range value '" ZEND_LONG_FMT "' for $partition", partition);
        return;
    }

    intern = get_kafka_topic_object(getThis());
    if (!intern) {
        return;
    }

    kafka_intern = get_kafka_object(&intern->zrk);
    if (!kafka_intern) {
        return;
    }

    ret = rd_kafka_consume_stop(intern->rkt, partition);

    if (ret == -1) {
        err = rd_kafka_last_error();
        zend_throw_exception(ce_kafka_exception, rd_kafka_err2str(err), err);
        return;
    }

    del_consuming_toppar(kafka_intern, intern->rkt, partition);
}
/* }}} */

/* {{{ proto RdKafka\Message RdKafka\ConsumerTopic::consume(int $partition, int timeout_ms)
   Consume a single message from partition */
PHP_METHOD(RdKafka_ConsumerTopic, consume)
{
    kafka_topic_object *intern;
    zend_long partition;
    zend_long timeout_ms;
    rd_kafka_message_t *message;
    rd_kafka_resp_err_t err;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "ll", &partition, &timeout_ms) == FAILURE) {
        return;
    }

    if (partition != RD_KAFKA_PARTITION_UA && (partition < 0 || partition > 0x7FFFFFFF)) {
        zend_throw_exception_ex(spl_ce_InvalidArgumentException, 0, "Out of range value '" ZEND_LONG_FMT "' for $partition", partition);
        return;
    }

    intern = get_kafka_topic_object(getThis());
    if (!intern) {
        return;
    }

    message = rd_kafka_consume(intern->rkt, partition, timeout_ms);

    if (!message) {
        err = rd_kafka_last_error();
        if (err == RD_KAFKA_RESP_ERR__TIMED_OUT) {
            return;
        }
        zend_throw_exception(ce_kafka_exception, rd_kafka_err2str(err), err);
        return;
    }

    kafka_message_new(return_value, message, NULL);

    rd_kafka_message_destroy(message);
}
/* }}} */

/* {{{ proto RdKafka\Message RdKafka\ConsumerTopic::consumeBatch(int $partition, int $timeout_ms, int $batch_size)
   Consume a batch of messages from a partition */
PHP_METHOD(RdKafka_ConsumerTopic, consumeBatch)
{
    kafka_topic_object *intern;
    zend_long partition, timeout_ms, batch_size;
    long result, i;
    rd_kafka_message_t **rkmessages;
    rd_kafka_resp_err_t err;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "lll", &partition, &timeout_ms, &batch_size) == FAILURE) {
        return;
    }

    if (batch_size <= 0 || (size_t) batch_size > SIZE_MAX / sizeof(*rkmessages)) {
        zend_throw_exception_ex(spl_ce_InvalidArgumentException, 0, "Out of range value '" ZEND_LONG_FMT "' for batch_size", batch_size);
        return;
    }

    if (partition != RD_KAFKA_PARTITION_UA && (partition < 0 || partition > 0x7FFFFFFF)) {
        zend_throw_exception_ex(spl_ce_InvalidArgumentException, 0, "Out of range value '" ZEND_LONG_FMT "' for $partition", partition);
        return;
    }

    intern = get_kafka_topic_object(getThis());
    if (!intern) {
        return;
    }

    rkmessages = safe_emalloc(batch_size, sizeof(*rkmessages), 0);

    result = rd_kafka_consume_batch(intern->rkt, partition, timeout_ms, rkmessages, batch_size);

    if (result == -1) {
        efree(rkmessages);
        err = rd_kafka_last_error();
        zend_throw_exception(ce_kafka_exception, rd_kafka_err2str(err), err);
        return;
    }

    if (result >= 0) {
        kafka_message_list_to_array(return_value, rkmessages, result);
        for (i = 0; i < result; ++i) {
            rd_kafka_message_destroy(rkmessages[i]);
        }
    }

    efree(rkmessages);
}
/* }}} */

/* {{{ proto void RdKafka\ConsumerTopic::offsetStore(int partition, int offset) */
PHP_METHOD(RdKafka_ConsumerTopic, offsetStore)
{
    kafka_topic_object *intern;
    zend_long partition;
    zend_long offset;
    rd_kafka_resp_err_t err;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "ll", &partition, &offset) == FAILURE) {
        return;
    }

    if (partition < 0 || partition > 0x7FFFFFFF) {
        zend_throw_exception_ex(spl_ce_InvalidArgumentException, 0, "Out of range value '" ZEND_LONG_FMT "' for $partition", partition);
        return;
    }

    intern = get_kafka_topic_object(getThis());
    if (!intern) {
        return;
    }

    err = rd_kafka_offset_store(intern->rkt, partition, offset);

    if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        zend_throw_exception(ce_kafka_exception, rd_kafka_err2str(err), err);
        return;
    }
}
/* }}} */

/* {{{ proto void RdKafka\ProducerTopic::produce(int $partition, int $msgflags[, string $payload[, string $key[, string $msg_opaque]]])
   Produce and send a single message to broker. */
PHP_METHOD(RdKafka_ProducerTopic, produce)
{
    zend_long partition;
    zend_long msgflags;
    char *payload = NULL;
    size_t payload_len = 0;
    char *key = NULL;
    size_t key_len = 0;
    zend_string *opaque = NULL;
    int ret;
    rd_kafka_resp_err_t err;
    kafka_topic_object *intern;

    ZEND_PARSE_PARAMETERS_START(2, 5)
        Z_PARAM_LONG(partition)
        Z_PARAM_LONG(msgflags)
        Z_PARAM_OPTIONAL
        Z_PARAM_STRING_OR_NULL(payload, payload_len)
        Z_PARAM_STRING_OR_NULL(key, key_len)
        Z_PARAM_STR_OR_NULL(opaque)
    ZEND_PARSE_PARAMETERS_END();

    if (partition != RD_KAFKA_PARTITION_UA && (partition < 0 || partition > 0x7FFFFFFF)) {
        zend_throw_exception_ex(spl_ce_InvalidArgumentException, 0, "Out of range value '" ZEND_LONG_FMT "' for $partition", partition);
        return;
    }

    if (msgflags != 0 && msgflags != RD_KAFKA_MSG_F_BLOCK) {
        zend_throw_exception_ex(spl_ce_InvalidArgumentException, 0, "Invalid value '" ZEND_LONG_FMT "' for $msgflags", msgflags);
        return;
    }

    intern = get_kafka_topic_object(getThis());

    if (opaque != NULL) {
        zend_string_addref(opaque);
    }

    ret = rd_kafka_produce(intern->rkt, partition, msgflags | RD_KAFKA_MSG_F_COPY, payload, payload_len, key, key_len, opaque);

    if (ret == -1) {
        if (opaque != NULL) {
            zend_string_release(opaque);
        }
        err = rd_kafka_last_error();
        zend_throw_exception(ce_kafka_exception, rd_kafka_err2str(err), err);
        return;
    }
}
/* }}} */

/* {{{ proto void RdKafka\ProducerTopic::producev(int $partition, int $msgflags[, string $payload[, string $key[, array $headers[, int $timestamp_ms[, string msg_opaque]]]]])
   Produce and send a single message to broker (with headers possibility and timestamp). */
PHP_METHOD(RdKafka_ProducerTopic, producev)
{
    zend_long partition;
    zend_long msgflags;
    char *payload = NULL;
    size_t payload_len = 0;
    char *key = NULL;
    size_t key_len = 0;
    rd_kafka_resp_err_t err;
    kafka_topic_object *intern;
    kafka_object *kafka_intern;
    HashTable *headersParam = NULL;
    HashPosition headersParamPos;
    char *header_key;
    zval *header_value;
    rd_kafka_headers_t *headers;
    zend_long timestamp_ms = 0;
    zend_bool timestamp_ms_is_null = 0;
    zend_string *opaque = NULL;

    ZEND_PARSE_PARAMETERS_START(2, 7)
        Z_PARAM_LONG(partition)
        Z_PARAM_LONG(msgflags)
        Z_PARAM_OPTIONAL
        Z_PARAM_STRING_OR_NULL(payload, payload_len)
        Z_PARAM_STRING_OR_NULL(key, key_len)
        Z_PARAM_ARRAY_HT_OR_NULL(headersParam)
        Z_PARAM_LONG_OR_NULL(timestamp_ms, timestamp_ms_is_null)
        Z_PARAM_STR_OR_NULL(opaque)
    ZEND_PARSE_PARAMETERS_END();

    if (partition != RD_KAFKA_PARTITION_UA && (partition < 0 || partition > 0x7FFFFFFF)) {
        zend_throw_exception_ex(spl_ce_InvalidArgumentException, 0, "Out of range value '" ZEND_LONG_FMT "' for $partition", partition);
        return;
    }

    if (msgflags != 0 && msgflags != RD_KAFKA_MSG_F_BLOCK) {
        zend_throw_exception_ex(spl_ce_InvalidArgumentException, 0, "Invalid value '" ZEND_LONG_FMT "' for $msgflags", msgflags);
        return;
    }

    if (timestamp_ms_is_null == 1) {
        timestamp_ms = 0;
    }

    intern = get_kafka_topic_object(getThis());

    if (opaque != NULL) {
        zend_string_addref(opaque);
    }

    if (headersParam != NULL && zend_hash_num_elements(headersParam) > 0) {
        headers = rd_kafka_headers_new(zend_hash_num_elements(headersParam));
        for (zend_hash_internal_pointer_reset_ex(headersParam, &headersParamPos);
                (header_value = zend_hash_get_current_data_ex(headersParam, &headersParamPos)) != NULL &&
                (header_key = rdkafka_hash_get_current_key_ex(headersParam, &headersParamPos)) != NULL;
                zend_hash_move_forward_ex(headersParam, &headersParamPos)) {
            convert_to_string_ex(header_value);
            rd_kafka_header_add(
                headers,
                header_key,
                -1, // Auto detect header title length
                Z_STRVAL_P(header_value),
                Z_STRLEN_P(header_value)
            );
        }
    } else {
        headers = rd_kafka_headers_new(0);
    }

    kafka_intern = get_kafka_object(&intern->zrk);
    if (!kafka_intern) {
        return;
    }

    err = rd_kafka_producev(
            kafka_intern->rk,
            RD_KAFKA_V_RKT(intern->rkt),
            RD_KAFKA_V_PARTITION(partition),
            RD_KAFKA_V_MSGFLAGS(msgflags | RD_KAFKA_MSG_F_COPY),
            RD_KAFKA_V_VALUE(payload, payload_len),
            RD_KAFKA_V_KEY(key, key_len),
            RD_KAFKA_V_TIMESTAMP(timestamp_ms),
            RD_KAFKA_V_HEADERS(headers),
            RD_KAFKA_V_OPAQUE(opaque),
            RD_KAFKA_V_END
    );

    if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        rd_kafka_headers_destroy(headers);
        if (opaque != NULL) {
            zend_string_release(opaque);
        }
        zend_throw_exception(ce_kafka_exception, rd_kafka_err2str(err), err);
        return;
    }
}
/* }}} */

/* {{{ proto string RdKafka\Topic::getName() */
PHP_METHOD(RdKafka_Topic, getName)
{
    kafka_topic_object *intern;

    if (zend_parse_parameters_none() == FAILURE) {
        return;
    }

    intern = get_kafka_topic_object(getThis());
    if (!intern) {
        return;
    }

    RETURN_STRING(rd_kafka_topic_name(intern->rkt));
}
/* }}} */

void kafka_topic_minit(INIT_FUNC_ARGS) { /* {{{ */

    memcpy(&object_handlers, zend_get_std_object_handlers(), sizeof(zend_object_handlers));
    object_handlers.clone_obj = NULL;
    object_handlers.free_obj = kafka_topic_free;
    object_handlers.offset = XtOffsetOf(kafka_topic_object, std);

    ce_kafka_topic = register_class_RdKafka_Topic();
    ce_kafka_topic->create_object = kafka_topic_new;

    ce_kafka_consumer_topic = register_class_RdKafka_ConsumerTopic(ce_kafka_topic);

    ce_kafka_kafka_consumer_topic = register_class_RdKafka_KafkaConsumerTopic(ce_kafka_topic);

    ce_kafka_producer_topic = register_class_RdKafka_ProducerTopic(ce_kafka_topic);
} /* }}} */
