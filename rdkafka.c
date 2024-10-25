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

/* $Id$ */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "php.h"
#include "php_ini.h"
#include "ext/standard/info.h"
#include "php_rdkafka.h"
#include "php_rdkafka_priv.h"
#include "librdkafka/rdkafka.h"
#include "Zend/zend_exceptions.h"
#include "ext/spl/spl_exceptions.h"
#include "metadata.h"
#include "conf.h"
#include "topic.h"
#include "queue.h"
#include "message.h"
#include "kafka_consumer.h"
#include "topic_partition.h"
#include "rdkafka_arginfo.h"
#include "fun_arginfo.h"
#include "kafka_error_exception.h"

#if PHP_VERSION_ID < 80100
#   error "PHP version 8.1.0 or greater required"
#endif

#if RD_KAFKA_VERSION < 0x010503ff
#	error librdkafka version 1.5.3 or greater required
#endif

enum {
   RD_KAFKA_LOG_PRINT = 100
   , RD_KAFKA_LOG_SYSLOG = 101
   , RD_KAFKA_LOG_SYSLOG_PRINT = 102
};

typedef struct _toppar {
    rd_kafka_topic_t    *rkt;
    int32_t             partition;
} toppar;

static zend_object_handlers kafka_object_handlers;
zend_object_handlers kafka_default_object_handlers;

static zend_class_entry * ce_kafka;
static zend_class_entry * ce_kafka_consumer;
zend_class_entry * ce_kafka_exception;
static zend_class_entry * ce_kafka_producer;

static void stop_consuming_toppar_pp(toppar ** tp) {
    rd_kafka_consume_stop((*tp)->rkt, (*tp)->partition);
}

static void stop_consuming(kafka_object * intern) {
    zend_hash_apply(&intern->consuming, (apply_func_t)stop_consuming_toppar_pp);
}

static void kafka_free(zend_object *object) /* {{{ */
{
    kafka_object *intern = php_kafka_from_obj(kafka_object, object);

    kafka_conf_callbacks_dtor(&intern->cbs);

    if (intern->rk) {
        if (intern->type == RD_KAFKA_CONSUMER) {
            stop_consuming(intern);
            zend_hash_destroy(&intern->consuming);
            zend_hash_destroy(&intern->queues);
        } else if (intern->type == RD_KAFKA_PRODUCER) {
            // Force internal delivery callbacks for queued messages, as we rely
            // on these to free msg_opaques
            rd_kafka_purge(intern->rk, RD_KAFKA_PURGE_F_QUEUE | RD_KAFKA_PURGE_F_INFLIGHT);
            rd_kafka_flush(intern->rk, 0);
        }
        zend_hash_destroy(&intern->topics);

        rd_kafka_destroy(intern->rk);
        intern->rk = NULL;
    }

    zend_object_std_dtor(&intern->std);
}
/* }}} */

static void toppar_pp_dtor(toppar ** tp) {
    efree(*tp);
}

static void kafka_queue_object_pre_free(kafka_queue_object ** pp) {
    kafka_queue_object *intern = *pp;
    rd_kafka_queue_destroy(intern->rkqu);
    intern->rkqu = NULL;
    zval_ptr_dtor(&intern->zrk);
}

static void kafka_topic_object_pre_free(kafka_topic_object ** pp) {
    kafka_topic_object *intern = *pp;
    rd_kafka_topic_destroy(intern->rkt);
    intern->rkt = NULL;
    zval_ptr_dtor(&intern->zrk);
}

static void kafka_init(zval *this_ptr, rd_kafka_type_t type, zval *zconf) /* {{{ */
{
    char errstr[512];
    rd_kafka_t *rk;
    kafka_object *intern;
    kafka_conf_object *conf_intern;
    rd_kafka_conf_t *conf = NULL;

    intern = Z_RDKAFKA_P(kafka_object, this_ptr);
    intern->type = type;

    if (zconf) {
        conf_intern = get_kafka_conf_object(zconf);
        if (conf_intern) {
            conf = rd_kafka_conf_dup(conf_intern->u.conf);
            kafka_conf_callbacks_copy(&intern->cbs, &conf_intern->cbs);
        }
    }

    if (conf == NULL) {
        conf = rd_kafka_conf_new();
    }

    intern->cbs.zrk = *this_ptr;
    rd_kafka_conf_set_opaque(conf, &intern->cbs);
    if (type == RD_KAFKA_PRODUCER) {
        rd_kafka_conf_set_dr_msg_cb(conf, kafka_conf_dr_msg_cb);
    }

    rk = rd_kafka_new(type, conf, errstr, sizeof(errstr));

    if (rk == NULL) {
        zend_throw_exception(ce_kafka_exception, errstr, 0);
        return;
    }

    if (intern->cbs.log) {
        rd_kafka_set_log_queue(rk, NULL);
    }

    intern->rk = rk;

    if (type == RD_KAFKA_CONSUMER) {
        zend_hash_init(&intern->consuming, 0, NULL, (dtor_func_t)toppar_pp_dtor, 0);
        zend_hash_init(&intern->queues, 0, NULL, (dtor_func_t)kafka_queue_object_pre_free, 0);
    }

    zend_hash_init(&intern->topics, 0, NULL, (dtor_func_t)kafka_topic_object_pre_free, 0);
}
/* }}} */

static zend_object *kafka_new(zend_class_entry *class_type) /* {{{ */
{
    zend_object* retval;
    kafka_object *intern;

    intern = zend_object_alloc(sizeof(*intern), class_type);
    zend_object_std_init(&intern->std, class_type);
    object_properties_init(&intern->std, class_type);

    retval = &intern->std;
    retval->handlers = &kafka_object_handlers;

    return retval;
}
/* }}} */

kafka_object * get_kafka_object(zval *zrk)
{
    kafka_object *ork = Z_RDKAFKA_P(kafka_object, zrk);

    if (!ork->rk) {
        zend_throw_exception_ex(NULL, 0, "RdKafka\\Kafka::__construct() has not been called");
        return NULL;
    }

    return ork;
}

static void kafka_log_syslog_print(const rd_kafka_t *rk, int level, const char *fac, const char *buf) {
    rd_kafka_log_print(rk, level, fac, buf);
#ifndef _MSC_VER
    rd_kafka_log_syslog(rk, level, fac, buf);
#endif
}

void add_consuming_toppar(kafka_object * intern, rd_kafka_topic_t * rkt, int32_t partition) {
    char *key = NULL;
    int key_len;
    const char *topic_name = rd_kafka_topic_name(rkt);
    toppar *tp;

    tp = emalloc(sizeof(*tp));
    tp->rkt = rkt;
    tp->partition = partition;

    key_len = spprintf(&key, 0, "%s:%d", topic_name, partition);

    zend_hash_str_add_ptr(&intern->consuming, key, key_len+1, tp);

    efree(key);
}

void del_consuming_toppar(kafka_object * intern, rd_kafka_topic_t * rkt, int32_t partition) {
    char *key = NULL;
    int key_len;
    const char *topic_name = rd_kafka_topic_name(rkt);

    key_len = spprintf(&key, 0, "%s:%d", topic_name, partition);

    zend_hash_str_del(&intern->consuming, key, key_len+1);

    efree(key);
}

int is_consuming_toppar(kafka_object * intern, rd_kafka_topic_t * rkt, int32_t partition) {
    char *key = NULL;
    int key_len;
    const char *topic_name = rd_kafka_topic_name(rkt);
    int ret;

    key_len = spprintf(&key, 0, "%s:%d", topic_name, partition);

    ret = zend_hash_str_exists(&intern->consuming, key, key_len+1);

    efree(key);

    return ret;
}

/* {{{ private constructor */
PHP_METHOD(RdKafka, __construct)
{
    zend_throw_exception(NULL, "Private constructor", 0);
    return;
}
/* }}} */

/* {{{ proto RdKafka\Consumer::__construct([RdKafka\Conf $conf]) */
PHP_METHOD(RdKafka_Consumer, __construct)
{
    zval *zconf = NULL;
    zend_error_handling error_handling;

    zend_replace_error_handling(EH_THROW, spl_ce_InvalidArgumentException, &error_handling);

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "|O!", &zconf, ce_kafka_conf) == FAILURE) {
        zend_restore_error_handling(&error_handling);
        return;
    }

    kafka_init(getThis(), RD_KAFKA_CONSUMER, zconf);

    zend_restore_error_handling(&error_handling);
}
/* }}} */

/* {{{ proto RdKafka\Queue RdKafka\Consumer::newQueue()
   Returns a RdKafka\Queue object */
PHP_METHOD(RdKafka_Consumer, newQueue)
{
    rd_kafka_queue_t *rkqu;
    kafka_object *intern;
    kafka_queue_object *queue_intern;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "") == FAILURE) {
        return;
    }

    intern = get_kafka_object(getThis());
    if (!intern) {
        return;
    }

    rkqu = rd_kafka_queue_new(intern->rk);

    if (!rkqu) {
        return;
    }

    if (object_init_ex(return_value, ce_kafka_queue) != SUCCESS) {
        return;
    }

    queue_intern = Z_RDKAFKA_P(kafka_queue_object, return_value);
    if (!queue_intern) {
        return;
    }

    queue_intern->rkqu = rkqu;

    // Keep a reference to the parent Kafka object, attempts to ensure that
    // the Queue object is destroyed before the Kafka object.
    // This avoids rd_kafka_destroy() hanging.
    queue_intern->zrk = *getThis();

    Z_ADDREF_P(&queue_intern->zrk);

    zend_hash_index_add_ptr(&intern->queues, (zend_ulong)queue_intern, queue_intern);
}
/* }}} */

/* {{{ proto int RdKafka::addBrokers(string $brokerList)
   Returns the number of brokers successfully added */
PHP_METHOD(RdKafka, addBrokers)
{
    char *broker_list;
    size_t broker_list_len;
    kafka_object *intern;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "s", &broker_list, &broker_list_len) == FAILURE) {
        return;
    }

    intern = get_kafka_object(getThis());
    if (!intern) {
        return;
    }

    RETURN_LONG(rd_kafka_brokers_add(intern->rk, broker_list));
}
/* }}} */

/* {{{ proto RdKafka\Metadata RdKafka::getMetadata(bool $all_topics, RdKafka\Topic $only_topic, int $timeout_ms)
   Request Metadata from broker */
PHP_METHOD(RdKafka, getMetadata)
{
    zend_bool all_topics;
    zval *only_zrkt;
    zend_long timeout_ms;
    rd_kafka_resp_err_t err;
    kafka_object *intern;
    const rd_kafka_metadata_t *metadata;
    kafka_topic_object *only_orkt = NULL;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "bO!l", &all_topics, &only_zrkt, ce_kafka_topic, &timeout_ms) == FAILURE) {
        return;
    }

    intern = get_kafka_object(getThis());
    if (!intern) {
        return;
    }

    if (only_zrkt) {
        only_orkt = get_kafka_topic_object(only_zrkt);
        if (!only_orkt) {
            return;
        }
    }

    err = rd_kafka_metadata(intern->rk, all_topics, only_orkt ? only_orkt->rkt : NULL, &metadata, timeout_ms);

    if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        zend_throw_exception(ce_kafka_exception, rd_kafka_err2str(err), err);
        return;
    }

    kafka_metadata_init(return_value, metadata);
}
/* }}} */

/* {{{ proto int RdKafka::getControllerId(int $timeout_ms)
   Returns the current ControllerId (controller broker id) as reported in broker metadata */
PHP_METHOD(RdKafka, getControllerId)
{
    kafka_object *intern;
    zend_long timeout;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "l", &timeout) == FAILURE) {
        return;
    }

    intern = get_kafka_object(getThis());
    if (!intern) {
        return;
    }

    RETURN_LONG(rd_kafka_controllerid(intern->rk, timeout));
}
/* }}} */

/* {{{ proto void RdKafka::setLogLevel(int $level)
   Specifies the maximum logging level produced by internal kafka logging and debugging */
PHP_METHOD(RdKafka, setLogLevel)
{
    kafka_object *intern;
    zend_long level;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "l", &level) == FAILURE) {
        return;
    }

    intern = get_kafka_object(getThis());
    if (!intern) {
        return;
    }

    rd_kafka_set_log_level(intern->rk, level);
}
/* }}} */

/* {{{ proto void RdKafka::oauthbearerSetToken(string $token_value, int $lifetime_ms, string $principal_name, array $extensions = [])
 * Set SASL/OAUTHBEARER token and metadata
 *
 * The SASL/OAUTHBEARER token refresh callback or event handler should cause
 * this method to be invoked upon success, via
 * $kafka->oauthbearerSetToken(). The extension keys must not include the
 * reserved key "`auth`", and all extension keys and values must conform to the
 * required format as per https://tools.ietf.org/html/rfc7628#section-3.1:
 *
 * key            = 1*(ALPHA)
 * value          = *(VCHAR / SP / HTAB / CR / LF )
*/
PHP_METHOD(RdKafka, oauthbearerSetToken)
{
    kafka_object *intern;
    char *token_value;
    size_t token_value_len;
    zend_long lifetime_ms;
    char *principal_name;
    size_t principal_len;
    HashTable *extensions_hash = NULL;
    
    char errstr[512];
    rd_kafka_resp_err_t ret = 0;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "sls|h", &token_value, &token_value_len, &lifetime_ms, &principal_name, &principal_len, &extensions_hash) == FAILURE) {
        return;
    }

    intern = get_kafka_object(getThis());
    if (!intern) {
        return;
    }    

    errstr[0] = '\0';

    int extensions_size = 0;
    char **extensions = NULL;

    if (extensions_hash != NULL) {
        extensions_size = zend_hash_num_elements(extensions_hash) * 2;
        extensions = safe_emalloc((extensions_size * 2), sizeof(char *), 0);

        int pos = 0;
        zend_ulong num_key;
        zend_string *extension_key_str;
        zval *extension_zval;
        ZEND_HASH_FOREACH_KEY_VAL(extensions_hash, num_key, extension_key_str, extension_zval) {
            if (!extension_key_str) {
                extension_key_str = zend_long_to_str(num_key);
                extensions[pos++] = estrdup(ZSTR_VAL(extension_key_str));
                zend_string_release(extension_key_str);
            } else {
                extensions[pos++] = estrdup(ZSTR_VAL(extension_key_str));
            }

            zend_string *tmp_extension_val_str;
            zend_string *extension_val_str = zval_get_tmp_string(extension_zval, &tmp_extension_val_str);
            extensions[pos++] = estrdup(ZSTR_VAL(extension_val_str));
            if (tmp_extension_val_str) {
                zend_string_release(tmp_extension_val_str);
            }
        } ZEND_HASH_FOREACH_END();
    }    

    ret = rd_kafka_oauthbearer_set_token(
        intern->rk,
        token_value,
        lifetime_ms,
        principal_name,
        (const char **)extensions,
        extensions_size,
        errstr,
        sizeof(errstr));

    if (extensions != NULL) {
        for (int i = 0; i < extensions_size; i++) {
            efree(extensions[i]);
        }
        efree(extensions);
    }
    
    switch (ret) {
        case RD_KAFKA_RESP_ERR__INVALID_ARG:
            zend_throw_exception(ce_kafka_exception, errstr, RD_KAFKA_RESP_ERR__INVALID_ARG);
            return;
        case RD_KAFKA_RESP_ERR__NOT_IMPLEMENTED:
            zend_throw_exception(ce_kafka_exception, errstr, RD_KAFKA_RESP_ERR__NOT_IMPLEMENTED);
            return;
        case RD_KAFKA_RESP_ERR__STATE:
            zend_throw_exception(ce_kafka_exception, errstr, RD_KAFKA_RESP_ERR__STATE);
            return;
        case RD_KAFKA_RESP_ERR_NO_ERROR:
            break;
        default:
            return;
    }
}
/* }}} */

/* {{{ proto void RdKafka::oauthbearerSetTokenFailure(string $error)
 The SASL/OAUTHBEARER token refresh callback or event handler should cause
 this method to be invoked upon failure, via
 rd_kafka_oauthbearer_set_token_failure().
*/
PHP_METHOD(RdKafka, oauthbearerSetTokenFailure)
{
    kafka_object *intern;
    const char *errstr;
    size_t errstr_len;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "s", &errstr, &errstr_len) == FAILURE) {
        return;
    }

    intern = get_kafka_object(getThis());
    if (!intern) {
        return;
    }    

    rd_kafka_resp_err_t ret = rd_kafka_oauthbearer_set_token_failure(intern->rk, errstr);

    switch (ret) {
        case RD_KAFKA_RESP_ERR__INVALID_ARG:
            zend_throw_exception(ce_kafka_exception, NULL, RD_KAFKA_RESP_ERR__INVALID_ARG);
            return;
        case RD_KAFKA_RESP_ERR__STATE:
            zend_throw_exception(ce_kafka_exception, NULL, RD_KAFKA_RESP_ERR__STATE);
            return;
        case RD_KAFKA_RESP_ERR_NO_ERROR:
            break;
        default:
            return;
    }
}
/* }}} */

/* {{{ proto RdKafka\Topic RdKafka::newTopic(string $topic)
   Returns an RdKafka\Topic object */
PHP_METHOD(RdKafka, newTopic)
{
    char *topic;
    size_t topic_len;
    rd_kafka_topic_t *rkt;
    kafka_object *intern;
    kafka_topic_object *topic_intern;
    zend_class_entry *topic_type;
    zval *zconf = NULL;
    rd_kafka_topic_conf_t *conf = NULL;
    kafka_conf_object *conf_intern;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "s|O!", &topic, &topic_len, &zconf, ce_kafka_topic_conf) == FAILURE) {
        return;
    }

    intern = get_kafka_object(getThis());
    if (!intern) {
        return;
    }

    if (zconf) {
        conf_intern = get_kafka_conf_object(zconf);
        if (conf_intern) {
            conf = rd_kafka_topic_conf_dup(conf_intern->u.topic_conf);
        }
    }

    rkt = rd_kafka_topic_new(intern->rk, topic, conf);

    if (!rkt) {
        return;
    }

    switch (intern->type) {
        case RD_KAFKA_CONSUMER:
            topic_type = ce_kafka_consumer_topic;
            break;
        case RD_KAFKA_PRODUCER:
            topic_type = ce_kafka_producer_topic;
            break;
        default:
            return;
    }

    if (object_init_ex(return_value, topic_type) != SUCCESS) {
        return;
    }

    topic_intern = Z_RDKAFKA_P(kafka_topic_object, return_value);
    if (!topic_intern) {
        return;
    }

    topic_intern->rkt = rkt;
    topic_intern->zrk = *getThis();

    Z_ADDREF_P(&topic_intern->zrk);

    zend_hash_index_add_ptr(&intern->topics, (zend_ulong)topic_intern, topic_intern);
}
/* }}} */

/* {{{ proto int RdKafka::getOutQLen()
   Returns the current out queue length */
PHP_METHOD(RdKafka, getOutQLen)
{
    kafka_object *intern;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "") == FAILURE) {
        return;
    }

    intern = get_kafka_object(getThis());
    if (!intern) {
        return;
    }

    RETURN_LONG(rd_kafka_outq_len(intern->rk));
}
/* }}} */

/* {{{ proto int RdKafka::poll(int $timeout_ms)
   Polls the provided kafka handle for events */
PHP_METHOD(RdKafka, poll)
{
    kafka_object *intern;
    zend_long timeout;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "l", &timeout) == FAILURE) {
        return;
    }

    intern = get_kafka_object(getThis());
    if (!intern) {
        return;
    }

    RETURN_LONG(rd_kafka_poll(intern->rk, timeout));
}
/* }}} */

/* {{{ proto int RdKafka::flush(int $timeout_ms)
   Wait until all outstanding produce requests, et.al, are completed. */
PHP_METHOD(RdKafka, flush)
{
    kafka_object *intern;
    zend_long timeout;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "l", &timeout) == FAILURE) {
        return;
    }

    intern = get_kafka_object(getThis());
    if (!intern) {
        return;
    }

    RETURN_LONG(rd_kafka_flush(intern->rk, timeout));
}
/* }}} */

/* {{{ proto int RdKafka::purge(int $purge_flags)
   Purge messages that are in queue or in flight */
PHP_METHOD(RdKafka, purge)
{
    kafka_object *intern;
    zend_long purge_flags;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "l", &purge_flags) == FAILURE) {
        return;
    }

    intern = get_kafka_object(getThis());
    if (!intern) {
        return;
    }

    RETURN_LONG(rd_kafka_purge(intern->rk, purge_flags));
}
/* }}} */

/* {{{ proto void RdKafka::queryWatermarkOffsets(string $topic, int $partition, int &$low, int &$high, int $timeout_ms)
   Query broker for low (oldest/beginning) or high (newest/end) offsets for partition */
PHP_METHOD(RdKafka, queryWatermarkOffsets)
{
    kafka_object *intern;
    char *topic;
    size_t topic_length;
    int64_t low, high;
    zend_long partition, timeout;
    zval *lowResult, *highResult;
    rd_kafka_resp_err_t err;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "slzzl", &topic, &topic_length, &partition, &lowResult, &highResult, &timeout) == FAILURE) {
        return;
    }

    ZVAL_DEREF(lowResult);
    ZVAL_DEREF(highResult);

    intern = get_kafka_object(getThis());
    if (!intern) {
        return;
    }

    err = rd_kafka_query_watermark_offsets(intern->rk, topic, partition, &low, &high, timeout);

    if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        zend_throw_exception(ce_kafka_exception, rd_kafka_err2str(err), err);
        return;
    }

    ZVAL_LONG(lowResult, (zend_long) low);
    ZVAL_LONG(highResult, (zend_long) high);
}
/* }}} */

/* {{{ proto void RdKafka::offsetsForTimes(array $topicPartitions, int $timeout_ms)
   Look up the offsets for the given partitions by timestamp. */
PHP_METHOD(RdKafka, offsetsForTimes)
{
    HashTable *htopars = NULL;
    kafka_object *intern;
    rd_kafka_topic_partition_list_t *topicPartitions;
    zend_long timeout_ms;
    rd_kafka_resp_err_t err;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "hl", &htopars, &timeout_ms) == FAILURE) {
        return;
    }

    intern = get_kafka_object(getThis());
    if (!intern) {
        return;
    }

    topicPartitions = array_arg_to_kafka_topic_partition_list(1, htopars);
    if (!topicPartitions) {
        return;
    }

    err = rd_kafka_offsets_for_times(intern->rk, topicPartitions, timeout_ms);

    if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        rd_kafka_topic_partition_list_destroy(topicPartitions);
        zend_throw_exception(ce_kafka_exception, rd_kafka_err2str(err), err);
        return;
    }
    kafka_topic_partition_list_to_array(return_value, topicPartitions);
    rd_kafka_topic_partition_list_destroy(topicPartitions);
}
/* }}} */

/* {{{ proto void RdKafka::setLogger(mixed $logger)
   Sets the log callback */
PHP_METHOD(RdKafka, setLogger)
{
    kafka_object *intern;
    zend_long id;
    void (*logger) (const rd_kafka_t * rk, int level, const char *fac, const char *buf);

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "l", &id) == FAILURE) {
        return;
    }

    intern = get_kafka_object(getThis());
    if (!intern) {
        return;
    }

    switch (id) {
        case RD_KAFKA_LOG_PRINT:
            logger = rd_kafka_log_print;
            break;
#ifndef _MSC_VER
        case RD_KAFKA_LOG_SYSLOG:
            logger = rd_kafka_log_syslog;
            break;
#endif
        case RD_KAFKA_LOG_SYSLOG_PRINT:
            logger = kafka_log_syslog_print;
            break;
        default:
            zend_throw_exception_ex(NULL, 0, "Invalid logger");
            return;
    }

    rd_kafka_set_logger(intern->rk, logger);
}
/* }}} */

/* {{{ proto RdKafka\TopicPartition[] RdKafka::pausePatitions(RdKafka\TopicPartition[] $topicPartitions)
   Pause producing or consumption for the provided list of partitions. */
PHP_METHOD(RdKafka, pausePartitions)
{
    HashTable *htopars;
    rd_kafka_topic_partition_list_t *topars;
    rd_kafka_resp_err_t err;
    kafka_object *intern;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "h", &htopars) == FAILURE) {
        return;
    }

    intern = get_kafka_object(getThis());
    if (!intern) {
        return;
    }

    topars = array_arg_to_kafka_topic_partition_list(1, htopars);
    if (!topars) {
        return;
    }

    err = rd_kafka_pause_partitions(intern->rk, topars);

    if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        rd_kafka_topic_partition_list_destroy(topars);
        zend_throw_exception(ce_kafka_exception, rd_kafka_err2str(err), err);
        return;
    }

    kafka_topic_partition_list_to_array(return_value, topars);
    rd_kafka_topic_partition_list_destroy(topars);
}
/* }}} */

/* {{{ proto RdKafka\TopicPartition[] RdKafka::resumePatitions(RdKafka\TopicPartition[] $topicPartitions)
   Resume producing consumption for the provided list of partitions. */
PHP_METHOD(RdKafka, resumePartitions)
{
    HashTable *htopars;
    rd_kafka_topic_partition_list_t *topars;
    rd_kafka_resp_err_t err;
    kafka_object *intern;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "h", &htopars) == FAILURE) {
        return;
    }

    intern = get_kafka_object(getThis());
    if (!intern) {
        return;
    }

    topars = array_arg_to_kafka_topic_partition_list(1, htopars);
    if (!topars) {
        return;
    }

    err = rd_kafka_pause_partitions(intern->rk, topars);

    if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        rd_kafka_topic_partition_list_destroy(topars);
        zend_throw_exception(ce_kafka_exception, rd_kafka_err2str(err), err);
        return;
    }

    kafka_topic_partition_list_to_array(return_value, topars);
    rd_kafka_topic_partition_list_destroy(topars);
}
/* }}} */

/* {{{ proto RdKafka\Producer::__construct([RdKafka\Conf $conf]) */
PHP_METHOD(RdKafka_Producer, __construct)
{
    zval *zconf = NULL;
    zend_error_handling error_handling;

    zend_replace_error_handling(EH_THROW, spl_ce_InvalidArgumentException, &error_handling);

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "|O!", &zconf, ce_kafka_conf) == FAILURE) {
        zend_restore_error_handling(&error_handling);
        return;
    }

    kafka_init(getThis(), RD_KAFKA_PRODUCER, zconf);

    zend_restore_error_handling(&error_handling);
}
/* }}} */

/* {{{ proto int RdKafka\Producer::initTransactions(int timeout_ms)
   Initializes transactions, needs to be done before producing and starting a transaction */
PHP_METHOD(RdKafka_Producer, initTransactions)
{
    kafka_object *intern;
    zend_long timeout_ms;
    const rd_kafka_error_t *error;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "l", &timeout_ms) == FAILURE) {
        return;
    }

    intern = get_kafka_object(getThis());
    if (!intern) {
        return;
    }

    error = rd_kafka_init_transactions(intern->rk, timeout_ms);

    if (NULL == error) {
        return;
    }

    create_kafka_error(return_value, error);
    zend_throw_exception_object(return_value);
}
/* }}} */

/* {{{ proto int RdKafka\Producer::beginTransaction()
   Start a transaction */
PHP_METHOD(RdKafka_Producer, beginTransaction)
{
    kafka_object *intern;
    const rd_kafka_error_t *error;

    intern = get_kafka_object(getThis());
    if (!intern) {
        return;
    }

    error = rd_kafka_begin_transaction(intern->rk);

    if (NULL == error) {
        return;
    }

    create_kafka_error(return_value, error);
    zend_throw_exception_object(return_value);
}
/* }}} */

/* {{{ proto int RdKafka\Producer::commitTransaction(int timeout_ms)
   Commit a transaction */
PHP_METHOD(RdKafka_Producer, commitTransaction)
{
    kafka_object *intern;
    zend_long timeout_ms;
    const rd_kafka_error_t *error;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "l", &timeout_ms) == FAILURE) {
        return;
    }

    intern = get_kafka_object(getThis());
    if (!intern) {
        return;
    }

    error = rd_kafka_commit_transaction(intern->rk, timeout_ms);

    if (NULL == error) {
        return;
    }

    create_kafka_error(return_value, error);
    zend_throw_exception_object(return_value);
}
/* }}} */

/* {{{ proto int RdKafka\Producer::abortTransaction(int timeout_ms)
   Commit a transaction */
PHP_METHOD(RdKafka_Producer, abortTransaction)
{
    kafka_object *intern;
    zend_long timeout_ms;
    const rd_kafka_error_t *error;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "l", &timeout_ms) == FAILURE) {
        return;
    }

    intern = get_kafka_object(getThis());
    if (!intern) {
        return;
    }

    error = rd_kafka_abort_transaction(intern->rk, timeout_ms);

    if (NULL == error) {
        return;
    }

    create_kafka_error(return_value, error);
    zend_throw_exception_object(return_value);
}
/* }}} */

#define COPY_CONSTANT(name) \
    REGISTER_LONG_CONSTANT(#name, name, CONST_CS | CONST_PERSISTENT)

void register_err_constants(INIT_FUNC_ARGS) /* {{{ */
{
    const struct rd_kafka_err_desc *errdescs;
    size_t cnt;
    size_t i;
    char buf[128];

    rd_kafka_get_err_descs(&errdescs, &cnt);

    for (i = 0; i < cnt; i++) {
        const struct rd_kafka_err_desc *desc = &errdescs[i];
        int len;

        if (!desc->name) {
            continue;
        }

        len = snprintf(buf, sizeof(buf), "RD_KAFKA_RESP_ERR_%s", desc->name);
        if ((size_t)len >= sizeof(buf)) {
            len = sizeof(buf)-1;
        }

        zend_register_long_constant(buf, len, desc->code, CONST_CS | CONST_PERSISTENT, module_number);
    }
} /* }}} */

/* {{{ PHP_MINIT_FUNCTION
 */
PHP_MINIT_FUNCTION(rdkafka)
{
    COPY_CONSTANT(RD_KAFKA_CONSUMER);
    COPY_CONSTANT(RD_KAFKA_OFFSET_BEGINNING);
    COPY_CONSTANT(RD_KAFKA_OFFSET_END);
    COPY_CONSTANT(RD_KAFKA_OFFSET_STORED);
    COPY_CONSTANT(RD_KAFKA_PARTITION_UA);
    COPY_CONSTANT(RD_KAFKA_PRODUCER);
    COPY_CONSTANT(RD_KAFKA_MSG_F_BLOCK);
    COPY_CONSTANT(RD_KAFKA_PURGE_F_QUEUE);
    COPY_CONSTANT(RD_KAFKA_PURGE_F_INFLIGHT);
    COPY_CONSTANT(RD_KAFKA_PURGE_F_NON_BLOCKING);
    REGISTER_LONG_CONSTANT("RD_KAFKA_VERSION", rd_kafka_version(), CONST_CS | CONST_PERSISTENT);
    REGISTER_LONG_CONSTANT("RD_KAFKA_BUILD_VERSION", RD_KAFKA_VERSION, CONST_CS | CONST_PERSISTENT);

    register_err_constants(INIT_FUNC_ARGS_PASSTHRU);

    COPY_CONSTANT(RD_KAFKA_CONF_UNKNOWN);
    COPY_CONSTANT(RD_KAFKA_CONF_INVALID);
    COPY_CONSTANT(RD_KAFKA_CONF_OK);

    REGISTER_LONG_CONSTANT("RD_KAFKA_MSG_PARTITIONER_RANDOM", MSG_PARTITIONER_RANDOM, CONST_CS | CONST_PERSISTENT);
    REGISTER_LONG_CONSTANT("RD_KAFKA_MSG_PARTITIONER_CONSISTENT", MSG_PARTITIONER_CONSISTENT, CONST_CS | CONST_PERSISTENT);
    REGISTER_LONG_CONSTANT("RD_KAFKA_MSG_PARTITIONER_CONSISTENT_RANDOM", MSG_PARTITIONER_CONSISTENT_RANDOM, CONST_CS | CONST_PERSISTENT);
    REGISTER_LONG_CONSTANT("RD_KAFKA_MSG_PARTITIONER_MURMUR2", MSG_PARTITIONER_MURMUR2, CONST_CS | CONST_PERSISTENT);
    REGISTER_LONG_CONSTANT("RD_KAFKA_MSG_PARTITIONER_MURMUR2_RANDOM", MSG_PARTITIONER_MURMUR2_RANDOM, CONST_CS | CONST_PERSISTENT);

    REGISTER_LONG_CONSTANT("RD_KAFKA_LOG_PRINT", RD_KAFKA_LOG_PRINT, CONST_CS | CONST_PERSISTENT);
    REGISTER_LONG_CONSTANT("RD_KAFKA_LOG_SYSLOG", RD_KAFKA_LOG_SYSLOG, CONST_CS | CONST_PERSISTENT);
    REGISTER_LONG_CONSTANT("RD_KAFKA_LOG_SYSLOG_PRINT", RD_KAFKA_LOG_SYSLOG_PRINT, CONST_CS | CONST_PERSISTENT);

    memcpy(&kafka_default_object_handlers, zend_get_std_object_handlers(), sizeof(zend_object_handlers));
    kafka_default_object_handlers.clone_obj = NULL;

	kafka_object_handlers = kafka_default_object_handlers;
    kafka_object_handlers.free_obj = kafka_free;
    kafka_object_handlers.offset = XtOffsetOf(kafka_object, std);

    ce_kafka = register_class_RdKafka();
    ce_kafka->create_object = kafka_new;

    ce_kafka_consumer = register_class_RdKafka_Consumer(ce_kafka);

    ce_kafka_producer = register_class_RdKafka_Producer(ce_kafka);

    ce_kafka_exception = register_class_RdKafka_Exception(zend_ce_exception);

    kafka_conf_minit(INIT_FUNC_ARGS_PASSTHRU);
    kafka_error_minit();
    kafka_kafka_consumer_minit(INIT_FUNC_ARGS_PASSTHRU);
    kafka_message_minit(INIT_FUNC_ARGS_PASSTHRU);
    kafka_metadata_minit(INIT_FUNC_ARGS_PASSTHRU);
    kafka_metadata_topic_partition_minit(INIT_FUNC_ARGS_PASSTHRU);
    kafka_queue_minit(INIT_FUNC_ARGS_PASSTHRU);
    kafka_topic_minit(INIT_FUNC_ARGS_PASSTHRU);

    return SUCCESS;
}
/* }}} */

/* {{{ PHP_MINFO_FUNCTION
 */
PHP_MINFO_FUNCTION(rdkafka)
{
    char *rd_kafka_version;

    php_info_print_table_start();
    php_info_print_table_row(2, "rdkafka support", "enabled");

    php_info_print_table_row(2, "version", PHP_RDKAFKA_VERSION);
    php_info_print_table_row(2, "build date", __DATE__ " " __TIME__);

    spprintf(
        &rd_kafka_version,
        0,
        "%u.%u.%u.%u",
        (RD_KAFKA_VERSION & 0xFF000000) >> 24,
        (RD_KAFKA_VERSION & 0x00FF0000) >> 16,
        (RD_KAFKA_VERSION & 0x0000FF00) >> 8,
        (RD_KAFKA_VERSION & 0x000000FF)
    );

    php_info_print_table_row(2, "librdkafka version (runtime)", rd_kafka_version_str());
    php_info_print_table_row(2, "librdkafka version (build)", rd_kafka_version);


    efree(rd_kafka_version);

    php_info_print_table_end();
}
/* }}} */

/* {{{ rdkafka_module_entry
 */
zend_module_entry rdkafka_module_entry = {
    STANDARD_MODULE_HEADER,
    "rdkafka",
    ext_functions,
    PHP_MINIT(rdkafka),
    NULL,
    NULL,
    NULL,
    PHP_MINFO(rdkafka),
    PHP_RDKAFKA_VERSION,
    STANDARD_MODULE_PROPERTIES
};
/* }}} */

#ifdef COMPILE_DL_RDKAFKA
ZEND_GET_MODULE(rdkafka)
#endif
