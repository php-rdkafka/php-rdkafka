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
#include "php_ini.h"
#include "ext/standard/info.h"
#include "php_rdkafka.h"
#include "php_rdkafka_priv.h"
#include "librdkafka/rdkafka.h"
#include "Zend/zend_exceptions.h"
#include "ext/spl/spl_exceptions.h"
#include "conf.h"
#include "topic_partition.h"
#include "message.h"
#if PHP_VERSION_ID < 80000
#include "conf_legacy_arginfo.h"
#else
#include "conf_arginfo.h"
#endif

zend_class_entry * ce_kafka_conf;
zend_class_entry * ce_kafka_topic_conf;

static zend_object_handlers handlers;

static void kafka_conf_callback_dtor(kafka_conf_callback *cb) /* {{{ */
{
    if (cb) {
        zval_ptr_dtor(&cb->fci.function_name);
        efree(cb);
    }
} /* }}} */

void kafka_conf_callbacks_dtor(kafka_conf_callbacks *cbs) /* {{{ */
{
    kafka_conf_callback_dtor(cbs->error);
    cbs->error = NULL;
    kafka_conf_callback_dtor(cbs->rebalance);
    cbs->rebalance = NULL;
    kafka_conf_callback_dtor(cbs->dr_msg);
    cbs->dr_msg = NULL;
    kafka_conf_callback_dtor(cbs->stats);
    cbs->stats = NULL;
    kafka_conf_callback_dtor(cbs->consume);
    cbs->consume = NULL;
    kafka_conf_callback_dtor(cbs->offset_commit);
    cbs->offset_commit = NULL;
    kafka_conf_callback_dtor(cbs->log);
    cbs->log = NULL;
    kafka_conf_callback_dtor(cbs->oauthbearer_token_refresh);
    cbs->oauthbearer_token_refresh = NULL;
} /* }}} */

static void kafka_conf_callback_copy(kafka_conf_callback **to, kafka_conf_callback *from) /* {{{ */
{
    if (from) {
        *to = emalloc(sizeof(**to));
        **to = *from;
        zval_copy_ctor(&(*to)->fci.function_name);
    }
} /* }}} */

void kafka_conf_callbacks_copy(kafka_conf_callbacks *to, kafka_conf_callbacks *from) /* {{{ */
{
    kafka_conf_callback_copy(&to->oauthbearer_token_refresh, from->oauthbearer_token_refresh);
    kafka_conf_callback_copy(&to->error, from->error);
    kafka_conf_callback_copy(&to->rebalance, from->rebalance);
    kafka_conf_callback_copy(&to->dr_msg, from->dr_msg);
    kafka_conf_callback_copy(&to->stats, from->stats);
    kafka_conf_callback_copy(&to->consume, from->consume);
    kafka_conf_callback_copy(&to->offset_commit, from->offset_commit);
    kafka_conf_callback_copy(&to->log, from->log);
} /* }}} */

static void kafka_conf_free(zend_object *object) /* {{{ */
{
    kafka_conf_object *intern = php_kafka_from_obj(kafka_conf_object, object);

    switch (intern->type) {
        case KAFKA_CONF:
            if (intern->u.conf) {
                rd_kafka_conf_destroy(intern->u.conf);
            }
            kafka_conf_callbacks_dtor(&intern->cbs);
            break;
        case KAFKA_TOPIC_CONF:
            if (intern->u.topic_conf) {
                rd_kafka_topic_conf_destroy(intern->u.topic_conf);
            }
            break;
    }

    zend_object_std_dtor(&intern->std);
}
/* }}} */

static zend_object *kafka_conf_new(zend_class_entry *class_type) /* {{{ */
{
    zend_object* retval;
    kafka_conf_object *intern;

    intern = zend_object_alloc(sizeof(*intern), class_type);
    zend_object_std_init(&intern->std, class_type);
    object_properties_init(&intern->std, class_type);

    retval = &intern->std;
    retval->handlers = &handlers;

    return retval;
}
/* }}} */

kafka_conf_object * get_kafka_conf_object(zval *zconf)
{
    kafka_conf_object *oconf = Z_RDKAFKA_P(kafka_conf_object, zconf);

    if (!oconf->type) {
        zend_throw_exception_ex(NULL, 0, "RdKafka\\Conf::__construct() has not been called");
        return NULL;
    }

    return oconf;
}

static void kafka_conf_error_cb(rd_kafka_t *rk, int err, const char *reason, void *opaque)
{
    kafka_conf_callbacks *cbs = (kafka_conf_callbacks*) opaque;
    zval args[3];

    if (!opaque) {
        return;
    }

    if (!cbs->error) {
        return;
    }

    ZVAL_NULL(&args[0]);
    ZVAL_NULL(&args[1]);
    ZVAL_NULL(&args[2]);

    ZVAL_ZVAL(&args[0], &cbs->zrk, 1, 0);
    ZVAL_LONG(&args[1], err);
    ZVAL_STRING(&args[2], reason);

    rdkafka_call_function(&cbs->error->fci, &cbs->error->fcc, NULL, 3, args);

    zval_ptr_dtor(&args[0]);
    zval_ptr_dtor(&args[1]);
    zval_ptr_dtor(&args[2]);
}

void kafka_conf_dr_msg_cb(rd_kafka_t *rk, const rd_kafka_message_t *msg, void *opaque)
{
    kafka_conf_callbacks *cbs = (kafka_conf_callbacks*) opaque;
    zend_string *msg_opaque = msg->_private;
    zval args[2];

    if (cbs != NULL && cbs->dr_msg) {
        ZVAL_NULL(&args[0]);
        ZVAL_NULL(&args[1]);

        ZVAL_ZVAL(&args[0], &cbs->zrk, 1, 0);
        kafka_message_new(&args[1], msg, msg_opaque);

        rdkafka_call_function(&cbs->dr_msg->fci, &cbs->dr_msg->fcc, NULL, 2, args);

        zval_ptr_dtor(&args[0]);
        zval_ptr_dtor(&args[1]);
    }

    if (msg_opaque != NULL) {
        zend_string_release(msg_opaque);
    }
}

static int kafka_conf_stats_cb(rd_kafka_t *rk, char *json, size_t json_len, void *opaque)
{
    kafka_conf_callbacks *cbs = (kafka_conf_callbacks*) opaque;
    zval args[3];

    if (!opaque) {
        return 0;
    }

    if (!cbs->stats) {
        return 0;
    }

    ZVAL_NULL(&args[0]);
    ZVAL_NULL(&args[1]);
    ZVAL_NULL(&args[2]);

    ZVAL_ZVAL(&args[0], &cbs->zrk, 1, 0);
    ZVAL_STRING(&args[1], json);
    ZVAL_LONG(&args[2], json_len);

    rdkafka_call_function(&cbs->stats->fci, &cbs->stats->fcc, NULL, 3, args);

    zval_ptr_dtor(&args[0]);
    zval_ptr_dtor(&args[1]);
    zval_ptr_dtor(&args[2]);

    return 0;
}

static void kafka_conf_rebalance_cb(rd_kafka_t *rk, rd_kafka_resp_err_t err, rd_kafka_topic_partition_list_t *partitions, void *opaque)
{
    kafka_conf_callbacks *cbs = (kafka_conf_callbacks*) opaque;
    zval args[3];

    if (!opaque) {
        return;
    }

    if (!cbs->rebalance) {
        err = rd_kafka_assign(rk, NULL);

        if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
            zend_throw_exception(ce_kafka_exception, rd_kafka_err2str(err), err);
            return;
        }

        return;
    }

    ZVAL_NULL(&args[0]);
    ZVAL_NULL(&args[1]);
    ZVAL_NULL(&args[2]);

    ZVAL_ZVAL(&args[0], &cbs->zrk, 1, 0);
    ZVAL_LONG(&args[1], err);
    kafka_topic_partition_list_to_array(&args[2], partitions);

    rdkafka_call_function(&cbs->rebalance->fci, &cbs->rebalance->fcc, NULL, 3, args);

    zval_ptr_dtor(&args[0]);
    zval_ptr_dtor(&args[1]);
    zval_ptr_dtor(&args[2]);
}

static void kafka_conf_consume_cb(rd_kafka_message_t *msg, void *opaque)
{
    kafka_conf_callbacks *cbs = (kafka_conf_callbacks*) opaque;
    zval args[2];

    if (!opaque) {
        return;
    }

    if (!cbs->consume) {
        return;
    }

    ZVAL_NULL(&args[0]);
    ZVAL_NULL(&args[1]);

    kafka_message_new(&args[0], msg, NULL);
    ZVAL_ZVAL(&args[1], &cbs->zrk, 1, 0);


    rdkafka_call_function(&cbs->consume->fci, &cbs->consume->fcc, NULL, 2, args);

    zval_ptr_dtor(&args[0]);
    zval_ptr_dtor(&args[1]);
}

static void kafka_conf_offset_commit_cb(rd_kafka_t *rk, rd_kafka_resp_err_t err, rd_kafka_topic_partition_list_t *partitions, void *opaque)
{
    kafka_conf_callbacks *cbs = (kafka_conf_callbacks*) opaque;
    zval args[3];

    if (!opaque) {
        return;
    }

    if (!cbs->offset_commit) {
        return;
    }

    ZVAL_NULL(&args[0]);
    ZVAL_NULL(&args[1]);
    ZVAL_NULL(&args[2]);

    ZVAL_ZVAL(&args[0], &cbs->zrk, 1, 0);
    ZVAL_LONG(&args[1], err);
    kafka_topic_partition_list_to_array(&args[2], partitions);

    rdkafka_call_function(&cbs->offset_commit->fci, &cbs->offset_commit->fcc, NULL, 3, args);

    zval_ptr_dtor(&args[0]);
    zval_ptr_dtor(&args[1]);
    zval_ptr_dtor(&args[2]);
}

static void kafka_conf_log_cb(const rd_kafka_t *rk, int level, const char *facility, const char *message)
{
    zval args[4];

    kafka_conf_callbacks *cbs = (kafka_conf_callbacks*) rd_kafka_opaque(rk);

    if (!cbs->log) {
        return;
    }

    ZVAL_NULL(&args[0]);
    ZVAL_NULL(&args[1]);
    ZVAL_NULL(&args[2]);
    ZVAL_NULL(&args[3]);

    ZVAL_ZVAL(&args[0], &cbs->zrk, 1, 0);
    ZVAL_LONG(&args[1], level);
    ZVAL_STRING(&args[2], facility);
    ZVAL_STRING(&args[3], message);

    rdkafka_call_function(&cbs->log->fci, &cbs->log->fcc, NULL, 4, args);

    zval_ptr_dtor(&args[0]);
    zval_ptr_dtor(&args[1]);
    zval_ptr_dtor(&args[2]);
    zval_ptr_dtor(&args[3]);
}

#ifdef HAS_RD_KAFKA_OAUTHBEARER
/* 
void rd_kafka_conf_set_oauthbearer_token_refresh_cb(
    rd_kafka_conf_t *conf,
    void (*oauthbearer_token_refresh_cb)(rd_kafka_t *rk,
                                         const char *oauthbearer_config,
                                         void *opaque)) {
}*/
static void kafka_conf_set_oauthbearer_token_refresh_cb(rd_kafka_t *rk, const char *oauthbearer_config, void *opaque)
{
    kafka_conf_callbacks *cbs = (kafka_conf_callbacks*) opaque;
    zval args[2];

    if (!opaque) {
        return;
    }

    if (!cbs->oauthbearer_token_refresh) {
        return;
    }

    ZVAL_NULL(&args[0]);
    ZVAL_NULL(&args[1]);

    ZVAL_ZVAL(&args[0], &cbs->zrk, 1, 0);
    ZVAL_STRING(&args[1], oauthbearer_config);

    rdkafka_call_function(&cbs->oauthbearer_token_refresh->fci, &cbs->oauthbearer_token_refresh->fcc, NULL, 2, args);

    zval_ptr_dtor(&args[0]);
    zval_ptr_dtor(&args[1]);
}
#endif

/* {{{ proto RdKafka\Conf::__construct() */
PHP_METHOD(RdKafka_Conf, __construct)
{
    kafka_conf_object *intern;
    zend_error_handling error_handling;

    zend_replace_error_handling(EH_THROW, spl_ce_InvalidArgumentException, &error_handling);

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "") == FAILURE) {
        zend_restore_error_handling(&error_handling);
        return;
    }

    intern = Z_RDKAFKA_P(kafka_conf_object, getThis());
    intern->type = KAFKA_CONF;
    intern->u.conf = rd_kafka_conf_new();

    zend_restore_error_handling(&error_handling);
}
/* }}} */

/* {{{ proto array RfKafka\Conf::dump()
   Dump the configuration properties and values of `conf` to an array */
PHP_METHOD(RdKafka_Conf, dump)
{
    size_t cntp;
    const char **dump;
    kafka_conf_object *intern;
    size_t i;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "") == FAILURE) {
        return;
    }

    intern = get_kafka_conf_object(getThis());
    if (!intern) {
        return;
    }

    switch (intern->type) {
        case KAFKA_CONF:
            dump = rd_kafka_conf_dump(intern->u.conf, &cntp);
            break;
        case KAFKA_TOPIC_CONF:
            dump = rd_kafka_topic_conf_dump(intern->u.topic_conf, &cntp);
            break;
        default:
            return;
    }

    array_init(return_value);

    for (i = 0; i < cntp; i+=2) {
        const char *key = dump[i];
        const char *value = dump[i+1];
        add_assoc_string(return_value, (char*)key, (char*)value);
    }

    rd_kafka_conf_dump_free(dump, cntp);
}
/* }}} */

/* {{{ proto void RdKafka\Conf::set(string $name, string $value)
   Sets a configuration property. */
PHP_METHOD(RdKafka_Conf, set)
{
    char *name;
    size_t name_len;
    char *value;
    size_t value_len;
    kafka_conf_object *intern;
    rd_kafka_conf_res_t ret = 0;
    char errstr[512];

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "ss", &name, &name_len, &value, &value_len) == FAILURE) {
        return;
    }

    intern = get_kafka_conf_object(getThis());
    if (!intern) {
        return;
    }

    errstr[0] = '\0';

    switch (intern->type) {
        case KAFKA_CONF:
            ret = rd_kafka_conf_set(intern->u.conf, name, value, errstr, sizeof(errstr));
            break;
        case KAFKA_TOPIC_CONF:
            ret = rd_kafka_topic_conf_set(intern->u.topic_conf, name, value, errstr, sizeof(errstr));
            break;
    }

    switch (ret) {
        case RD_KAFKA_CONF_UNKNOWN:
            zend_throw_exception(ce_kafka_exception, errstr, RD_KAFKA_CONF_UNKNOWN);
            return;
        case RD_KAFKA_CONF_INVALID:
            zend_throw_exception(ce_kafka_exception, errstr, RD_KAFKA_CONF_INVALID);
            return;
        case RD_KAFKA_CONF_OK:
            break;
    }
}
/* }}} */

/* {{{ proto RdKafka\Conf::setDefaultTopicConf(RdKafka\TopicConf $topicConf) */
PHP_METHOD(RdKafka_Conf, setDefaultTopicConf)
{
    zval *ztopic_conf;
    kafka_conf_object *intern;
    kafka_conf_object *topic_conf_intern;
    rd_kafka_topic_conf_t *topic_conf;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "O", &ztopic_conf, ce_kafka_topic_conf) == FAILURE) {
        return;
    }

    intern = get_kafka_conf_object(getThis());
    if (!intern) {
        return;
    }

    topic_conf_intern = get_kafka_conf_object(ztopic_conf);
    if (!topic_conf_intern) {
        return;
    }

    topic_conf = rd_kafka_topic_conf_dup(topic_conf_intern->u.topic_conf);

    rd_kafka_conf_set_default_topic_conf(intern->u.conf, topic_conf);
}
/* }}} */

/* {{{ proto void RdKafka\Conf::setErrorCb(callable $callback)
   Sets the error callback */
PHP_METHOD(RdKafka_Conf, setErrorCb)
{
    zend_fcall_info fci;
    zend_fcall_info_cache fcc;
    kafka_conf_object *intern;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "f", &fci, &fcc) == FAILURE) {
        return;
    }

    intern = get_kafka_conf_object(getThis());
    if (!intern) {
        return;
    }

    Z_ADDREF_P(&fci.function_name);

    if (intern->cbs.error) {
        zval_ptr_dtor(&intern->cbs.error->fci.function_name);
    } else {
        intern->cbs.error = ecalloc(1, sizeof(*intern->cbs.error));
    }

    intern->cbs.error->fci = fci;
    intern->cbs.error->fcc = fcc;

    rd_kafka_conf_set_error_cb(intern->u.conf, kafka_conf_error_cb);
}
/* }}} */

/* {{{ proto void RdKafka\Conf::setDrMsgCb(callable $callback)
   Sets the delivery report callback */
PHP_METHOD(RdKafka_Conf, setDrMsgCb)
{
    zend_fcall_info fci;
    zend_fcall_info_cache fcc;
    kafka_conf_object *intern;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "f", &fci, &fcc) == FAILURE) {
        return;
    }

    intern = get_kafka_conf_object(getThis());
    if (!intern) {
        return;
    }

    Z_ADDREF_P(&fci.function_name);

    if (intern->cbs.dr_msg) {
        zval_ptr_dtor(&intern->cbs.dr_msg->fci.function_name);
    } else {
        intern->cbs.dr_msg = ecalloc(1, sizeof(*intern->cbs.dr_msg));
    }

    intern->cbs.dr_msg->fci = fci;
    intern->cbs.dr_msg->fcc = fcc;

    rd_kafka_conf_set_dr_msg_cb(intern->u.conf, kafka_conf_dr_msg_cb);
}
/* }}} */

/* {{{ proto void RdKafka\Conf::setStatsCb(callable $callback)
   Sets the statistics report callback */
PHP_METHOD(RdKafka_Conf, setStatsCb)
{
    zend_fcall_info fci;
    zend_fcall_info_cache fcc;
    kafka_conf_object *intern;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "f", &fci, &fcc) == FAILURE) {
        return;
    }

    intern = get_kafka_conf_object(getThis());
    if (!intern) {
        return;
    }

    Z_ADDREF_P(&fci.function_name);

    if (intern->cbs.stats) {
        zval_ptr_dtor(&intern->cbs.stats->fci.function_name);
    } else {
        intern->cbs.stats = ecalloc(1, sizeof(*intern->cbs.stats));
    }

    intern->cbs.stats->fci = fci;
    intern->cbs.stats->fcc = fcc;

    rd_kafka_conf_set_stats_cb(intern->u.conf, kafka_conf_stats_cb);
}
/* }}} */

/* {{{ proto void RdKafka\Conf::setRebalanceCb(mixed $callback)
   Set rebalance callback for use with coordinated consumer group balancing */
PHP_METHOD(RdKafka_Conf, setRebalanceCb)
{
    zend_fcall_info fci;
    zend_fcall_info_cache fcc;
    kafka_conf_object *intern;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "f", &fci, &fcc) == FAILURE) {
        return;
    }

    intern = get_kafka_conf_object(getThis());
    if (!intern) {
        return;
    }

    Z_ADDREF_P(&fci.function_name);

    if (intern->cbs.rebalance) {
        zval_ptr_dtor(&intern->cbs.rebalance->fci.function_name);
    } else {
        intern->cbs.rebalance = ecalloc(1, sizeof(*intern->cbs.rebalance));
    }

    intern->cbs.rebalance->fci = fci;
    intern->cbs.rebalance->fcc = fcc;

    rd_kafka_conf_set_rebalance_cb(intern->u.conf, kafka_conf_rebalance_cb);
}
/* }}} */

/* {{{ proto void RdKafka\Conf::setConsumeCb(callable $callback)
   Set consume callback to use with poll */
PHP_METHOD(RdKafka_Conf, setConsumeCb)
{
    zend_fcall_info fci;
    zend_fcall_info_cache fcc;
    kafka_conf_object *intern;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "f", &fci, &fcc) == FAILURE) {
        return;
    }

    intern = get_kafka_conf_object(getThis());
    if (!intern) {
        return;
    }

    Z_ADDREF_P(&fci.function_name);

    if (intern->cbs.consume) {
        zval_ptr_dtor(&intern->cbs.consume->fci.function_name);
    } else {
        intern->cbs.consume = ecalloc(1, sizeof(*intern->cbs.consume));
    }

    intern->cbs.consume->fci = fci;
    intern->cbs.consume->fcc = fcc;

    rd_kafka_conf_set_consume_cb(intern->u.conf, kafka_conf_consume_cb);
}
/* }}} */

/* {{{ proto void RdKafka\Conf::setOffsetCommitCb(mixed $callback)
   Set offset commit callback for use with consumer groups */
PHP_METHOD(RdKafka_Conf, setOffsetCommitCb)
{
    zend_fcall_info fci;
    zend_fcall_info_cache fcc;
    kafka_conf_object *intern;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "f", &fci, &fcc) == FAILURE) {
        return;
    }

    intern = get_kafka_conf_object(getThis());
    if (!intern) {
        return;
    }

    Z_ADDREF_P(&fci.function_name);

    if (intern->cbs.offset_commit) {
        zval_ptr_dtor(&intern->cbs.offset_commit->fci.function_name);
    } else {
        intern->cbs.offset_commit = ecalloc(1, sizeof(*intern->cbs.offset_commit));
    }

    intern->cbs.offset_commit->fci = fci;
    intern->cbs.offset_commit->fcc = fcc;

    rd_kafka_conf_set_offset_commit_cb(intern->u.conf, kafka_conf_offset_commit_cb);
}
/* }}} */

/* {{{ proto void RdKafka\Conf::setLogCb(mixed $callback)
   Set offset commit callback for use with consumer groups */
PHP_METHOD(RdKafka_Conf, setLogCb)
{
    zend_fcall_info fci;
    zend_fcall_info_cache fcc;
    kafka_conf_object *conf;
    char errstr[512];

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "f", &fci, &fcc) == FAILURE) {
        return;
    }

    conf = get_kafka_conf_object(getThis());
    if (!conf) {
        return;
    }

    Z_ADDREF_P(&fci.function_name);

    if (conf->cbs.log) {
        zval_ptr_dtor(&conf->cbs.log->fci.function_name);
    } else {
        conf->cbs.log = ecalloc(1, sizeof(*conf->cbs.log));
    }

    conf->cbs.log->fci = fci;
    conf->cbs.log->fcc = fcc;

    rd_kafka_conf_set_log_cb(conf->u.conf, kafka_conf_log_cb);
    rd_kafka_conf_set(conf->u.conf, "log.queue", "true", errstr, sizeof(errstr));
}
/* }}} */

#ifdef HAS_RD_KAFKA_OAUTHBEARER
/* {{{ proto void RdKafka\Conf::setOauthbearerTokenRefreshCb(mixed $callback)
   Set token refresh callback for OAUTHBEARER sasl */
PHP_METHOD(RdKafka_Conf, setOauthbearerTokenRefreshCb)
{
    zend_fcall_info fci;
    zend_fcall_info_cache fcc;
    kafka_conf_object *conf;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "f", &fci, &fcc) == FAILURE) {
        return;
    }

    conf = get_kafka_conf_object(getThis());
    if (!conf) {
        return;
    }

    Z_ADDREF_P(&fci.function_name);

    if (conf->cbs.oauthbearer_token_refresh) {
        zval_ptr_dtor(&conf->cbs.oauthbearer_token_refresh->fci.function_name);
    } else {
        conf->cbs.oauthbearer_token_refresh = ecalloc(1, sizeof(*conf->cbs.oauthbearer_token_refresh));
    }

    conf->cbs.oauthbearer_token_refresh->fci = fci;
    conf->cbs.oauthbearer_token_refresh->fcc = fcc;

    rd_kafka_conf_set_oauthbearer_token_refresh_cb(conf->u.conf, kafka_conf_set_oauthbearer_token_refresh_cb);
}
/* }}} */
#endif

/* {{{ proto RdKafka\TopicConf::__construct() */
PHP_METHOD(RdKafka_TopicConf, __construct)
{
    kafka_conf_object *intern;
    zend_error_handling error_handling;

    zend_replace_error_handling(EH_THROW, spl_ce_InvalidArgumentException, &error_handling);

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "") == FAILURE) {
        zend_restore_error_handling(&error_handling);
        return;
    }

    intern = Z_RDKAFKA_P(kafka_conf_object, getThis());
    intern->type = KAFKA_TOPIC_CONF;
    intern->u.topic_conf = rd_kafka_topic_conf_new();

    zend_restore_error_handling(&error_handling);
}
/* }}} */

/* {{{ proto RdKafka\TopicConf::setPartitioner(int $partitioner) */
PHP_METHOD(RdKafka_TopicConf, setPartitioner)
{
    kafka_conf_object *intern;
    zend_long id;
    int32_t (*partitioner) (const rd_kafka_topic_t * rkt, const void * keydata, size_t keylen, int32_t partition_cnt, void * rkt_opaque, void * msg_opaque);

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "l", &id) == FAILURE) {
        return;
    }

    intern = get_kafka_conf_object(getThis());
    if (!intern) {
        return;
    }

    switch (id) {
        case MSG_PARTITIONER_RANDOM:
            partitioner = rd_kafka_msg_partitioner_random;
            break;
        case MSG_PARTITIONER_CONSISTENT:
            partitioner = rd_kafka_msg_partitioner_consistent;
            break;
        case MSG_PARTITIONER_CONSISTENT_RANDOM:
            partitioner = rd_kafka_msg_partitioner_consistent_random;
            break;
#ifdef HAS_RD_KAFKA_PARTITIONER_MURMUR2
        case MSG_PARTITIONER_MURMUR2:
            partitioner = rd_kafka_msg_partitioner_murmur2;
            break;
        case MSG_PARTITIONER_MURMUR2_RANDOM:
            partitioner = rd_kafka_msg_partitioner_murmur2_random;
            break;
#endif
        default:
            zend_throw_exception_ex(spl_ce_InvalidArgumentException, 0, "Invalid partitioner given");
            return;
    }

    rd_kafka_topic_conf_set_partitioner_cb(intern->u.topic_conf, partitioner);
}
/* }}} */

void kafka_conf_minit(INIT_FUNC_ARGS)
{
    handlers = kafka_default_object_handlers;
    handlers.free_obj = kafka_conf_free;
    handlers.offset = XtOffsetOf(kafka_conf_object, std);

    ce_kafka_conf = register_class_RdKafka_Conf();
    ce_kafka_conf->create_object = kafka_conf_new;

    ce_kafka_topic_conf = register_class_RdKafka_TopicConf();
    ce_kafka_topic_conf->create_object = kafka_conf_new;
}
