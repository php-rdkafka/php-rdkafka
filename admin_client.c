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
#include "Zend/zend_exceptions.h"
#include "admin_client.h"
#include "conf.h"
#include "topic_partition.h"
#include "admin_client_arginfo.h"

/* Default timeout for queue poll in milliseconds */
#define ADMIN_DEFAULT_TIMEOUT_MS 5000

/* Class entries */
zend_class_entry *ce_kafka_admin_client;
zend_class_entry *ce_kafka_admin_options;
zend_class_entry *ce_kafka_new_topic;
zend_class_entry *ce_kafka_delete_topic;
zend_class_entry *ce_kafka_new_partitions;
zend_class_entry *ce_kafka_topic_result;
zend_class_entry *ce_kafka_node;
zend_class_entry *ce_kafka_topic_partition_info;
zend_class_entry *ce_kafka_topic_description;

/* Object handlers */
static zend_object_handlers admin_client_object_handlers;
static zend_object_handlers admin_options_object_handlers;
static zend_object_handlers new_topic_object_handlers;
static zend_object_handlers delete_topic_object_handlers;
static zend_object_handlers new_partitions_object_handlers;

/* {{{ Accessor macros */
#define get_admin_client_object(zv) \
    ((kafka_admin_client_object*)((char*)(Z_OBJ_P(zv)) - XtOffsetOf(kafka_admin_client_object, std)))
#define get_admin_options_object(zv) \
    ((kafka_admin_options_object*)((char*)(Z_OBJ_P(zv)) - XtOffsetOf(kafka_admin_options_object, std)))
#define get_new_topic_object(zv) \
    ((kafka_new_topic_object*)((char*)(Z_OBJ_P(zv)) - XtOffsetOf(kafka_new_topic_object, std)))
#define get_delete_topic_object(zv) \
    ((kafka_delete_topic_object*)((char*)(Z_OBJ_P(zv)) - XtOffsetOf(kafka_delete_topic_object, std)))
#define get_new_partitions_object(zv) \
    ((kafka_new_partitions_object*)((char*)(Z_OBJ_P(zv)) - XtOffsetOf(kafka_new_partitions_object, std)))
/* }}} */

/* {{{ Object create/free: AdminClient */
static void admin_client_free(zend_object *object) /* {{{ */
{
    kafka_admin_client_object *intern = (kafka_admin_client_object*)((char*)(object) - XtOffsetOf(kafka_admin_client_object, std));

    kafka_conf_callbacks_dtor(&intern->cbs);

    if (intern->rk) {
        rd_kafka_destroy(intern->rk);
        intern->rk = NULL;
    }

    zend_object_std_dtor(&intern->std);
}
/* }}} */

static zend_object *admin_client_new(zend_class_entry *class_type) /* {{{ */
{
    kafka_admin_client_object *intern;

    intern = zend_object_alloc(sizeof(*intern), class_type);
    zend_object_std_init(&intern->std, class_type);
    object_properties_init(&intern->std, class_type);

    intern->std.handlers = &admin_client_object_handlers;

    return &intern->std;
}
/* }}} */

/* {{{ Object create/free: AdminOptions */
static void admin_options_free(zend_object *object) /* {{{ */
{
    kafka_admin_options_object *intern = (kafka_admin_options_object*)((char*)(object) - XtOffsetOf(kafka_admin_options_object, std));

    if (intern->options) {
        rd_kafka_AdminOptions_destroy(intern->options);
        intern->options = NULL;
    }

    zend_object_std_dtor(&intern->std);
}
/* }}} */

static zend_object *admin_options_new(zend_class_entry *class_type) /* {{{ */
{
    kafka_admin_options_object *intern;

    intern = zend_object_alloc(sizeof(*intern), class_type);
    zend_object_std_init(&intern->std, class_type);
    object_properties_init(&intern->std, class_type);

    intern->std.handlers = &admin_options_object_handlers;

    return &intern->std;
}
/* }}} */

/* {{{ Object create/free: NewTopic */
static void new_topic_free(zend_object *object) /* {{{ */
{
    kafka_new_topic_object *intern = (kafka_new_topic_object*)((char*)(object) - XtOffsetOf(kafka_new_topic_object, std));

    if (intern->new_topic) {
        rd_kafka_NewTopic_destroy(intern->new_topic);
        intern->new_topic = NULL;
    }

    zend_object_std_dtor(&intern->std);
}
/* }}} */

static zend_object *new_topic_new(zend_class_entry *class_type) /* {{{ */
{
    kafka_new_topic_object *intern;

    intern = zend_object_alloc(sizeof(*intern), class_type);
    zend_object_std_init(&intern->std, class_type);
    object_properties_init(&intern->std, class_type);

    intern->std.handlers = &new_topic_object_handlers;

    return &intern->std;
}
/* }}} */

/* {{{ Object create/free: DeleteTopic */
static void delete_topic_free(zend_object *object) /* {{{ */
{
    kafka_delete_topic_object *intern = (kafka_delete_topic_object*)((char*)(object) - XtOffsetOf(kafka_delete_topic_object, std));

    if (intern->delete_topic) {
        rd_kafka_DeleteTopic_destroy(intern->delete_topic);
        intern->delete_topic = NULL;
    }

    zend_object_std_dtor(&intern->std);
}
/* }}} */

static zend_object *delete_topic_new(zend_class_entry *class_type) /* {{{ */
{
    kafka_delete_topic_object *intern;

    intern = zend_object_alloc(sizeof(*intern), class_type);
    zend_object_std_init(&intern->std, class_type);
    object_properties_init(&intern->std, class_type);

    intern->std.handlers = &delete_topic_object_handlers;

    return &intern->std;
}
/* }}} */

/* {{{ Object create/free: NewPartitions */
static void new_partitions_free(zend_object *object) /* {{{ */
{
    kafka_new_partitions_object *intern = (kafka_new_partitions_object*)((char*)(object) - XtOffsetOf(kafka_new_partitions_object, std));

    if (intern->new_partitions) {
        rd_kafka_NewPartitions_destroy(intern->new_partitions);
        intern->new_partitions = NULL;
    }

    zend_object_std_dtor(&intern->std);
}
/* }}} */

static zend_object *new_partitions_new(zend_class_entry *class_type) /* {{{ */
{
    kafka_new_partitions_object *intern;

    intern = zend_object_alloc(sizeof(*intern), class_type);
    zend_object_std_init(&intern->std, class_type);
    object_properties_init(&intern->std, class_type);

    intern->std.handlers = &new_partitions_object_handlers;

    return &intern->std;
}
/* }}} */

/* {{{ Helper: convert topic_result_t array to PHP array of TopicResult objects */
static void kafka_topic_results_to_array(zval *return_value, const rd_kafka_topic_result_t **results, size_t cnt)
{
    size_t i;

    array_init_size(return_value, cnt);

    for (i = 0; i < cnt; i++) {
        zval topic_result_zv;
        const rd_kafka_topic_result_t *result = results[i];

        object_init_ex(&topic_result_zv, ce_kafka_topic_result);

        zend_update_property_long(NULL, Z_OBJ(topic_result_zv), "error", sizeof("error") - 1,
            rd_kafka_topic_result_error(result));

        const char *errstr = rd_kafka_topic_result_error_string(result);
        if (errstr) {
            zend_update_property_string(NULL, Z_OBJ(topic_result_zv), "error_string", sizeof("error_string") - 1, errstr);
        } else {
            zend_update_property_null(NULL, Z_OBJ(topic_result_zv), "error_string", sizeof("error_string") - 1);
        }

        zend_update_property_string(NULL, Z_OBJ(topic_result_zv), "name", sizeof("name") - 1,
            rd_kafka_topic_result_name(result));

        add_next_index_zval(return_value, &topic_result_zv);
    }
}
/* }}} */

/* {{{ AdminClient::__construct(Conf $conf) */
ZEND_METHOD(RdKafka_Admin_AdminClient, __construct)
{
    zval *zconf;
    char errstr[512];
    rd_kafka_t *rk;
    kafka_admin_client_object *intern;
    kafka_conf_object *conf_intern;
    rd_kafka_conf_t *conf = NULL;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "O", &zconf, ce_kafka_conf) == FAILURE) {
        return;
    }

    intern = get_admin_client_object(getThis());

    conf_intern = get_kafka_conf_object(zconf);
    if (conf_intern) {
        conf = rd_kafka_conf_dup(conf_intern->u.conf);
        kafka_conf_callbacks_copy(&intern->cbs, &conf_intern->cbs);
        intern->cbs.zrk = *getThis();
        rd_kafka_conf_set_opaque(conf, &intern->cbs);
    }

    if (conf == NULL) {
        conf = rd_kafka_conf_new();
    }

    /* AdminClient uses a producer-type handle internally */
    rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));

    if (rk == NULL) {
        zend_throw_exception(ce_kafka_exception, errstr, 0);
        return;
    }

    if (intern->cbs.log) {
        rd_kafka_set_log_queue(rk, NULL);
    }

    intern->rk = rk;
}
/* }}} */

/* {{{ AdminClient::newAdminOptions(int $for_api): AdminOptions */
ZEND_METHOD(RdKafka_Admin_AdminClient, newAdminOptions)
{
    zend_long for_api;
    kafka_admin_client_object *intern;
    kafka_admin_options_object *options_intern;
    rd_kafka_AdminOptions_t *options;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "l", &for_api) == FAILURE) {
        return;
    }

    intern = get_admin_client_object(getThis());
    if (!intern->rk) {
        zend_throw_exception(ce_kafka_exception, "AdminClient is not properly initialized", 0);
        return;
    }

    options = rd_kafka_AdminOptions_new(intern->rk, (rd_kafka_admin_op_t)for_api);
    if (!options) {
        zend_throw_exception(ce_kafka_exception, "Failed to create AdminOptions: invalid for_api value", 0);
        return;
    }

    object_init_ex(return_value, ce_kafka_admin_options);
    options_intern = get_admin_options_object(return_value);
    options_intern->options = options;
}
/* }}} */

/* {{{ AdminClient::createTopics(array $new_topics, ?AdminOptions $options = null): array */
ZEND_METHOD(RdKafka_Admin_AdminClient, createTopics)
{
    zval *znew_topics, *zoptions = NULL;
    kafka_admin_client_object *intern;
    rd_kafka_NewTopic_t **new_topics = NULL;
    size_t new_topic_cnt;
    rd_kafka_AdminOptions_t *options = NULL;
    rd_kafka_queue_t *queue = NULL;
    rd_kafka_event_t *event = NULL;
    zval *zitem;
    size_t i;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "a|O!", &znew_topics, &zoptions, ce_kafka_admin_options) == FAILURE) {
        return;
    }

    intern = get_admin_client_object(getThis());
    if (!intern->rk) {
        zend_throw_exception(ce_kafka_exception, "AdminClient is not properly initialized", 0);
        return;
    }

    /* Extract AdminOptions if provided */
    if (zoptions) {
        kafka_admin_options_object *options_intern = get_admin_options_object(zoptions);
        options = options_intern->options;
    }

    /* Build array of NewTopic pointers from PHP array */
    new_topic_cnt = zend_hash_num_elements(Z_ARRVAL_P(znew_topics));
    if (new_topic_cnt == 0) {
        zend_throw_exception(ce_kafka_exception, "new_topics array must not be empty", 0);
        return;
    }

    new_topics = ecalloc(new_topic_cnt, sizeof(rd_kafka_NewTopic_t*));

    i = 0;
    ZEND_HASH_FOREACH_VAL(Z_ARRVAL_P(znew_topics), zitem) {
        if (Z_TYPE_P(zitem) != IS_OBJECT || !instanceof_function(Z_OBJCE_P(zitem), ce_kafka_new_topic)) {
            zend_throw_exception(ce_kafka_exception, "All items in new_topics must be instances of RdKafka\\Admin\\NewTopic", 0);
            efree(new_topics);
            return;
        }
        kafka_new_topic_object *topic_intern = get_new_topic_object(zitem);
        if (!topic_intern->new_topic) {
            zend_throw_exception(ce_kafka_exception, "NewTopic object is not properly initialized", 0);
            efree(new_topics);
            return;
        }
        new_topics[i++] = topic_intern->new_topic;
    } ZEND_HASH_FOREACH_END();

    /* Create temporary queue */
    queue = rd_kafka_queue_new(intern->rk);
    if (!queue) {
        zend_throw_exception(ce_kafka_exception, "Failed to create admin queue", 0);
        efree(new_topics);
        return;
    }

    /* Call the admin operation */
    rd_kafka_CreateTopics(intern->rk, new_topics, new_topic_cnt, options, queue);

    /* Poll for result */
    event = rd_kafka_queue_poll(queue, ADMIN_DEFAULT_TIMEOUT_MS);

    efree(new_topics);
    rd_kafka_queue_destroy(queue);

    if (!event) {
        zend_throw_exception(ce_kafka_exception, "Timed out waiting for CreateTopics result", RD_KAFKA_RESP_ERR__TIMED_OUT);
        return;
    }

    if (rd_kafka_event_error(event)) {
        zend_throw_exception(ce_kafka_exception,
            rd_kafka_event_error_string(event),
            rd_kafka_event_error(event));
        rd_kafka_event_destroy(event);
        return;
    }

    /* Extract results */
    const rd_kafka_CreateTopics_result_t *result = rd_kafka_event_CreateTopics_result(event);
    if (!result) {
        zend_throw_exception(ce_kafka_exception, "Unexpected event type in CreateTopics result", 0);
        rd_kafka_event_destroy(event);
        return;
    }

    size_t result_cnt;
    const rd_kafka_topic_result_t **topic_results = rd_kafka_CreateTopics_result_topics(result, &result_cnt);

    kafka_topic_results_to_array(return_value, topic_results, result_cnt);

    rd_kafka_event_destroy(event);
}
/* }}} */

/* {{{ AdminClient::deleteTopics(array $delete_topics, ?AdminOptions $options = null): array */
ZEND_METHOD(RdKafka_Admin_AdminClient, deleteTopics)
{
    zval *zdelete_topics, *zoptions = NULL;
    kafka_admin_client_object *intern;
    rd_kafka_DeleteTopic_t **delete_topics = NULL;
    size_t delete_topic_cnt;
    rd_kafka_AdminOptions_t *options = NULL;
    rd_kafka_queue_t *queue = NULL;
    rd_kafka_event_t *event = NULL;
    zval *zitem;
    size_t i;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "a|O!", &zdelete_topics, &zoptions, ce_kafka_admin_options) == FAILURE) {
        return;
    }

    intern = get_admin_client_object(getThis());
    if (!intern->rk) {
        zend_throw_exception(ce_kafka_exception, "AdminClient is not properly initialized", 0);
        return;
    }

    if (zoptions) {
        kafka_admin_options_object *options_intern = get_admin_options_object(zoptions);
        options = options_intern->options;
    }

    delete_topic_cnt = zend_hash_num_elements(Z_ARRVAL_P(zdelete_topics));
    if (delete_topic_cnt == 0) {
        zend_throw_exception(ce_kafka_exception, "delete_topics array must not be empty", 0);
        return;
    }

    delete_topics = ecalloc(delete_topic_cnt, sizeof(rd_kafka_DeleteTopic_t*));

    i = 0;
    ZEND_HASH_FOREACH_VAL(Z_ARRVAL_P(zdelete_topics), zitem) {
        if (Z_TYPE_P(zitem) != IS_OBJECT || !instanceof_function(Z_OBJCE_P(zitem), ce_kafka_delete_topic)) {
            zend_throw_exception(ce_kafka_exception, "All items in delete_topics must be instances of RdKafka\\Admin\\DeleteTopic", 0);
            efree(delete_topics);
            return;
        }
        kafka_delete_topic_object *topic_intern = get_delete_topic_object(zitem);
        if (!topic_intern->delete_topic) {
            zend_throw_exception(ce_kafka_exception, "DeleteTopic object is not properly initialized", 0);
            efree(delete_topics);
            return;
        }
        delete_topics[i++] = topic_intern->delete_topic;
    } ZEND_HASH_FOREACH_END();

    queue = rd_kafka_queue_new(intern->rk);
    if (!queue) {
        zend_throw_exception(ce_kafka_exception, "Failed to create admin queue", 0);
        efree(delete_topics);
        return;
    }

    rd_kafka_DeleteTopics(intern->rk, delete_topics, delete_topic_cnt, options, queue);

    event = rd_kafka_queue_poll(queue, ADMIN_DEFAULT_TIMEOUT_MS);

    efree(delete_topics);
    rd_kafka_queue_destroy(queue);

    if (!event) {
        zend_throw_exception(ce_kafka_exception, "Timed out waiting for DeleteTopics result", RD_KAFKA_RESP_ERR__TIMED_OUT);
        return;
    }

    if (rd_kafka_event_error(event)) {
        zend_throw_exception(ce_kafka_exception,
            rd_kafka_event_error_string(event),
            rd_kafka_event_error(event));
        rd_kafka_event_destroy(event);
        return;
    }

    const rd_kafka_DeleteTopics_result_t *result = rd_kafka_event_DeleteTopics_result(event);
    if (!result) {
        zend_throw_exception(ce_kafka_exception, "Unexpected event type in DeleteTopics result", 0);
        rd_kafka_event_destroy(event);
        return;
    }

    size_t result_cnt;
    const rd_kafka_topic_result_t **topic_results = rd_kafka_DeleteTopics_result_topics(result, &result_cnt);

    kafka_topic_results_to_array(return_value, topic_results, result_cnt);

    rd_kafka_event_destroy(event);
}
/* }}} */

/* {{{ AdminClient::createPartitions(array $new_partitions, ?AdminOptions $options = null): array */
ZEND_METHOD(RdKafka_Admin_AdminClient, createPartitions)
{
    zval *znew_partitions, *zoptions = NULL;
    kafka_admin_client_object *intern;
    rd_kafka_NewPartitions_t **new_partitions = NULL;
    size_t new_partitions_cnt;
    rd_kafka_AdminOptions_t *options = NULL;
    rd_kafka_queue_t *queue = NULL;
    rd_kafka_event_t *event = NULL;
    zval *zitem;
    size_t i;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "a|O!", &znew_partitions, &zoptions, ce_kafka_admin_options) == FAILURE) {
        return;
    }

    intern = get_admin_client_object(getThis());
    if (!intern->rk) {
        zend_throw_exception(ce_kafka_exception, "AdminClient is not properly initialized", 0);
        return;
    }

    if (zoptions) {
        kafka_admin_options_object *options_intern = get_admin_options_object(zoptions);
        options = options_intern->options;
    }

    new_partitions_cnt = zend_hash_num_elements(Z_ARRVAL_P(znew_partitions));
    if (new_partitions_cnt == 0) {
        zend_throw_exception(ce_kafka_exception, "new_partitions array must not be empty", 0);
        return;
    }

    new_partitions = ecalloc(new_partitions_cnt, sizeof(rd_kafka_NewPartitions_t*));

    i = 0;
    ZEND_HASH_FOREACH_VAL(Z_ARRVAL_P(znew_partitions), zitem) {
        if (Z_TYPE_P(zitem) != IS_OBJECT || !instanceof_function(Z_OBJCE_P(zitem), ce_kafka_new_partitions)) {
            zend_throw_exception(ce_kafka_exception, "All items in new_partitions must be instances of RdKafka\\Admin\\NewPartitions", 0);
            efree(new_partitions);
            return;
        }
        kafka_new_partitions_object *part_intern = get_new_partitions_object(zitem);
        if (!part_intern->new_partitions) {
            zend_throw_exception(ce_kafka_exception, "NewPartitions object is not properly initialized", 0);
            efree(new_partitions);
            return;
        }
        new_partitions[i++] = part_intern->new_partitions;
    } ZEND_HASH_FOREACH_END();

    queue = rd_kafka_queue_new(intern->rk);
    if (!queue) {
        zend_throw_exception(ce_kafka_exception, "Failed to create admin queue", 0);
        efree(new_partitions);
        return;
    }

    rd_kafka_CreatePartitions(intern->rk, new_partitions, new_partitions_cnt, options, queue);

    event = rd_kafka_queue_poll(queue, ADMIN_DEFAULT_TIMEOUT_MS);

    efree(new_partitions);
    rd_kafka_queue_destroy(queue);

    if (!event) {
        zend_throw_exception(ce_kafka_exception, "Timed out waiting for CreatePartitions result", RD_KAFKA_RESP_ERR__TIMED_OUT);
        return;
    }

    if (rd_kafka_event_error(event)) {
        zend_throw_exception(ce_kafka_exception,
            rd_kafka_event_error_string(event),
            rd_kafka_event_error(event));
        rd_kafka_event_destroy(event);
        return;
    }

    const rd_kafka_CreatePartitions_result_t *result = rd_kafka_event_CreatePartitions_result(event);
    if (!result) {
        zend_throw_exception(ce_kafka_exception, "Unexpected event type in CreatePartitions result", 0);
        rd_kafka_event_destroy(event);
        return;
    }

    size_t result_cnt;
    const rd_kafka_topic_result_t **topic_results = rd_kafka_CreatePartitions_result_topics(result, &result_cnt);

    kafka_topic_results_to_array(return_value, topic_results, result_cnt);

    rd_kafka_event_destroy(event);
}
/* }}} */

/* {{{ AdminOptions::setRequestTimeout(int $timeout_ms): void */
ZEND_METHOD(RdKafka_Admin_AdminOptions, setRequestTimeout)
{
    zend_long timeout_ms;
    kafka_admin_options_object *intern;
    char errstr[512];
    rd_kafka_resp_err_t err;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "l", &timeout_ms) == FAILURE) {
        return;
    }

    intern = get_admin_options_object(getThis());
    if (!intern->options) {
        zend_throw_exception(ce_kafka_exception, "AdminOptions is not properly initialized", 0);
        return;
    }

    err = rd_kafka_AdminOptions_set_request_timeout(intern->options, (int)timeout_ms, errstr, sizeof(errstr));
    if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        zend_throw_exception(ce_kafka_exception, errstr, err);
    }
}
/* }}} */

/* {{{ AdminOptions::setOperationTimeout(int $timeout_ms): void */
ZEND_METHOD(RdKafka_Admin_AdminOptions, setOperationTimeout)
{
    zend_long timeout_ms;
    kafka_admin_options_object *intern;
    char errstr[512];
    rd_kafka_resp_err_t err;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "l", &timeout_ms) == FAILURE) {
        return;
    }

    intern = get_admin_options_object(getThis());
    if (!intern->options) {
        zend_throw_exception(ce_kafka_exception, "AdminOptions is not properly initialized", 0);
        return;
    }

    err = rd_kafka_AdminOptions_set_operation_timeout(intern->options, (int)timeout_ms, errstr, sizeof(errstr));
    if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        zend_throw_exception(ce_kafka_exception, errstr, err);
    }
}
/* }}} */

/* {{{ AdminOptions::setValidateOnly(bool $validate_only): void */
ZEND_METHOD(RdKafka_Admin_AdminOptions, setValidateOnly)
{
    zend_bool validate_only;
    kafka_admin_options_object *intern;
    char errstr[512];
    rd_kafka_resp_err_t err;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "b", &validate_only) == FAILURE) {
        return;
    }

    intern = get_admin_options_object(getThis());
    if (!intern->options) {
        zend_throw_exception(ce_kafka_exception, "AdminOptions is not properly initialized", 0);
        return;
    }

    err = rd_kafka_AdminOptions_set_validate_only(intern->options, (int)validate_only, errstr, sizeof(errstr));
    if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        zend_throw_exception(ce_kafka_exception, errstr, err);
    }
}
/* }}} */

/* {{{ AdminOptions::setBrokerId(int $broker_id): void */
ZEND_METHOD(RdKafka_Admin_AdminOptions, setBrokerId)
{
    zend_long broker_id;
    kafka_admin_options_object *intern;
    char errstr[512];
    rd_kafka_resp_err_t err;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "l", &broker_id) == FAILURE) {
        return;
    }

    intern = get_admin_options_object(getThis());
    if (!intern->options) {
        zend_throw_exception(ce_kafka_exception, "AdminOptions is not properly initialized", 0);
        return;
    }

    err = rd_kafka_AdminOptions_set_broker(intern->options, (int32_t)broker_id, errstr, sizeof(errstr));
    if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        zend_throw_exception(ce_kafka_exception, errstr, err);
    }
}
/* }}} */

/* {{{ AdminOptions::setIncludeAuthorizedOperations(bool $include): void */
ZEND_METHOD(RdKafka_Admin_AdminOptions, setIncludeAuthorizedOperations)
{
    zend_bool include;
    kafka_admin_options_object *intern;
    rd_kafka_error_t *error;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "b", &include) == FAILURE) {
        return;
    }

    intern = get_admin_options_object(getThis());
    if (!intern->options) {
        zend_throw_exception(ce_kafka_exception, "AdminOptions is not properly initialized", 0);
        return;
    }

    error = rd_kafka_AdminOptions_set_include_authorized_operations(intern->options, (int)include);
    if (error) {
        zend_throw_exception(ce_kafka_exception, rd_kafka_error_string(error), rd_kafka_error_code(error));
        rd_kafka_error_destroy(error);
    }
}
/* }}} */

/* {{{ NewTopic::__construct(string $topic, int $num_partitions, int $replication_factor) */
ZEND_METHOD(RdKafka_Admin_NewTopic, __construct)
{
    char *topic;
    size_t topic_len;
    zend_long num_partitions;
    zend_long replication_factor;
    kafka_new_topic_object *intern;
    char errstr[512];

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "sll", &topic, &topic_len, &num_partitions, &replication_factor) == FAILURE) {
        return;
    }

    intern = get_new_topic_object(getThis());

    intern->new_topic = rd_kafka_NewTopic_new(topic, (int)num_partitions, (int)replication_factor, errstr, sizeof(errstr));
    if (!intern->new_topic) {
        zend_throw_exception(ce_kafka_exception, errstr, 0);
        return;
    }
}
/* }}} */

/* {{{ NewTopic::setReplicaAssignment(int $partition, array $broker_ids): void */
ZEND_METHOD(RdKafka_Admin_NewTopic, setReplicaAssignment)
{
    zend_long partition;
    zval *zbroker_ids;
    kafka_new_topic_object *intern;
    char errstr[512];
    rd_kafka_resp_err_t err;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "la", &partition, &zbroker_ids) == FAILURE) {
        return;
    }

    intern = get_new_topic_object(getThis());
    if (!intern->new_topic) {
        zend_throw_exception(ce_kafka_exception, "NewTopic is not properly initialized", 0);
        return;
    }

    /* Convert PHP array to int32_t array */
    size_t broker_cnt = zend_hash_num_elements(Z_ARRVAL_P(zbroker_ids));
    int32_t *broker_ids = ecalloc(broker_cnt, sizeof(int32_t));

    zval *zbid;
    size_t j = 0;
    ZEND_HASH_FOREACH_VAL(Z_ARRVAL_P(zbroker_ids), zbid) {
        broker_ids[j++] = (int32_t)zval_get_long(zbid);
    } ZEND_HASH_FOREACH_END();

    err = rd_kafka_NewTopic_set_replica_assignment(intern->new_topic, (int32_t)partition, broker_ids, broker_cnt, errstr, sizeof(errstr));
    efree(broker_ids);

    if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        zend_throw_exception(ce_kafka_exception, errstr, err);
    }
}
/* }}} */

/* {{{ NewTopic::setConfig(string $name, string $value): void */
ZEND_METHOD(RdKafka_Admin_NewTopic, setConfig)
{
    char *name, *value;
    size_t name_len, value_len;
    kafka_new_topic_object *intern;
    rd_kafka_resp_err_t err;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "ss", &name, &name_len, &value, &value_len) == FAILURE) {
        return;
    }

    intern = get_new_topic_object(getThis());
    if (!intern->new_topic) {
        zend_throw_exception(ce_kafka_exception, "NewTopic is not properly initialized", 0);
        return;
    }

    err = rd_kafka_NewTopic_set_config(intern->new_topic, name, value);
    if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        zend_throw_exception(ce_kafka_exception, rd_kafka_err2str(err), err);
    }
}
/* }}} */

/* {{{ DeleteTopic::__construct(string $topic) */
ZEND_METHOD(RdKafka_Admin_DeleteTopic, __construct)
{
    char *topic;
    size_t topic_len;
    kafka_delete_topic_object *intern;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "s", &topic, &topic_len) == FAILURE) {
        return;
    }

    intern = get_delete_topic_object(getThis());

    intern->delete_topic = rd_kafka_DeleteTopic_new(topic);
    if (!intern->delete_topic) {
        zend_throw_exception(ce_kafka_exception, "Failed to create DeleteTopic", 0);
        return;
    }
}
/* }}} */

/* {{{ NewPartitions::__construct(string $topic, int $new_total_count) */
ZEND_METHOD(RdKafka_Admin_NewPartitions, __construct)
{
    char *topic;
    size_t topic_len;
    zend_long new_total_count;
    kafka_new_partitions_object *intern;
    char errstr[512];

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "sl", &topic, &topic_len, &new_total_count) == FAILURE) {
        return;
    }

    intern = get_new_partitions_object(getThis());

    intern->new_partitions = rd_kafka_NewPartitions_new(topic, (size_t)new_total_count, errstr, sizeof(errstr));
    if (!intern->new_partitions) {
        zend_throw_exception(ce_kafka_exception, errstr, 0);
        return;
    }
}
/* }}} */

/* {{{ NewPartitions::setReplicaAssignment(int $new_partition_index, array $broker_ids): void */
ZEND_METHOD(RdKafka_Admin_NewPartitions, setReplicaAssignment)
{
    zend_long new_partition_index;
    zval *zbroker_ids;
    kafka_new_partitions_object *intern;
    char errstr[512];
    rd_kafka_resp_err_t err;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "la", &new_partition_index, &zbroker_ids) == FAILURE) {
        return;
    }

    intern = get_new_partitions_object(getThis());
    if (!intern->new_partitions) {
        zend_throw_exception(ce_kafka_exception, "NewPartitions is not properly initialized", 0);
        return;
    }

    size_t broker_cnt = zend_hash_num_elements(Z_ARRVAL_P(zbroker_ids));
    int32_t *broker_ids = ecalloc(broker_cnt, sizeof(int32_t));

    zval *zbid;
    size_t j = 0;
    ZEND_HASH_FOREACH_VAL(Z_ARRVAL_P(zbroker_ids), zbid) {
        broker_ids[j++] = (int32_t)zval_get_long(zbid);
    } ZEND_HASH_FOREACH_END();

    err = rd_kafka_NewPartitions_set_replica_assignment(intern->new_partitions, (int32_t)new_partition_index, broker_ids, broker_cnt, errstr, sizeof(errstr));
    efree(broker_ids);

    if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        zend_throw_exception(ce_kafka_exception, errstr, err);
    }
}
/* }}} */

/* {{{ TopicResult::getError(): int */
ZEND_METHOD(RdKafka_Admin_TopicResult, getError)
{
    zval *zerr;

    if (zend_parse_parameters_none() == FAILURE) {
        return;
    }

    zerr = rdkafka_read_property(NULL, Z_OBJ_P(getThis()), "error", sizeof("error") - 1, 0);
    RETURN_LONG(Z_LVAL_P(zerr));
}
/* }}} */

/* {{{ TopicResult::getErrorString(): ?string */
ZEND_METHOD(RdKafka_Admin_TopicResult, getErrorString)
{
    zval *zerrstr;

    if (zend_parse_parameters_none() == FAILURE) {
        return;
    }

    zerrstr = rdkafka_read_property(NULL, Z_OBJ_P(getThis()), "error_string", sizeof("error_string") - 1, 0);
    if (Z_TYPE_P(zerrstr) == IS_NULL) {
        RETURN_NULL();
    }
    RETURN_STRING(Z_STRVAL_P(zerrstr));
}
/* }}} */

/* {{{ TopicResult::getName(): string */
ZEND_METHOD(RdKafka_Admin_TopicResult, getName)
{
    zval *zname;

    if (zend_parse_parameters_none() == FAILURE) {
        return;
    }

    zname = rdkafka_read_property(NULL, Z_OBJ_P(getThis()), "name", sizeof("name") - 1, 0);
    RETURN_STRING(Z_STRVAL_P(zname));
}
/* }}} */

/* {{{ Helper: convert rd_kafka_Node_t to PHP Node object */
static void kafka_node_to_zval(zval *return_value, const rd_kafka_Node_t *node)
{
    const char *rack;

    object_init_ex(return_value, ce_kafka_node);

    zend_update_property_long(NULL, Z_OBJ_P(return_value), "id", sizeof("id") - 1,
        rd_kafka_Node_id(node));
    zend_update_property_string(NULL, Z_OBJ_P(return_value), "host", sizeof("host") - 1,
        rd_kafka_Node_host(node));
    zend_update_property_long(NULL, Z_OBJ_P(return_value), "port", sizeof("port") - 1,
        (zend_long)rd_kafka_Node_port(node));

    rack = rd_kafka_Node_rack(node);
    if (rack) {
        zend_update_property_string(NULL, Z_OBJ_P(return_value), "rack", sizeof("rack") - 1, rack);
    } else {
        zend_update_property_null(NULL, Z_OBJ_P(return_value), "rack", sizeof("rack") - 1);
    }
}
/* }}} */

/* {{{ Helper: convert rd_kafka_Node_t** array to PHP array */
static void kafka_node_array_to_zval(zval *return_value, const rd_kafka_Node_t **nodes, size_t cnt)
{
    size_t i;
    array_init_size(return_value, cnt);

    for (i = 0; i < cnt; i++) {
        zval node_zv;
        kafka_node_to_zval(&node_zv, nodes[i]);
        add_next_index_zval(return_value, &node_zv);
    }
}
/* }}} */

/* {{{ Helper: convert TopicPartitionInfo to PHP object */
static void kafka_topic_partition_info_to_zval(zval *return_value, const rd_kafka_TopicPartitionInfo_t *partition)
{
    const rd_kafka_Node_t *leader;
    const rd_kafka_Node_t **isr_nodes;
    const rd_kafka_Node_t **replica_nodes;
    size_t isr_cnt, replica_cnt;
    zval isr_zv, replicas_zv;

    object_init_ex(return_value, ce_kafka_topic_partition_info);

    zend_update_property_long(NULL, Z_OBJ_P(return_value), "partition", sizeof("partition") - 1,
        rd_kafka_TopicPartitionInfo_partition(partition));

    leader = rd_kafka_TopicPartitionInfo_leader(partition);
    if (leader) {
        zval leader_zv;
        kafka_node_to_zval(&leader_zv, leader);
        zend_update_property(NULL, Z_OBJ_P(return_value), "leader", sizeof("leader") - 1, &leader_zv);
        zval_ptr_dtor(&leader_zv);
    } else {
        zend_update_property_null(NULL, Z_OBJ_P(return_value), "leader", sizeof("leader") - 1);
    }

    isr_nodes = rd_kafka_TopicPartitionInfo_isr(partition, &isr_cnt);
    kafka_node_array_to_zval(&isr_zv, isr_nodes, isr_cnt);
    zend_update_property(NULL, Z_OBJ_P(return_value), "isr", sizeof("isr") - 1, &isr_zv);
    zval_ptr_dtor(&isr_zv);

    replica_nodes = rd_kafka_TopicPartitionInfo_replicas(partition, &replica_cnt);
    kafka_node_array_to_zval(&replicas_zv, replica_nodes, replica_cnt);
    zend_update_property(NULL, Z_OBJ_P(return_value), "replicas", sizeof("replicas") - 1, &replicas_zv);
    zval_ptr_dtor(&replicas_zv);
}
/* }}} */

/* {{{ Helper: convert TopicDescription to PHP object */
static void kafka_topic_description_to_zval(zval *return_value, const rd_kafka_TopicDescription_t *topicdesc)
{
    const rd_kafka_error_t *error;
    const rd_kafka_Uuid_t *topic_id;
    const rd_kafka_TopicPartitionInfo_t **partitions;
    size_t partition_cnt;
    size_t i;
    zval partitions_zv;

    object_init_ex(return_value, ce_kafka_topic_description);

    zend_update_property_string(NULL, Z_OBJ_P(return_value), "name", sizeof("name") - 1,
        rd_kafka_TopicDescription_name(topicdesc));

    topic_id = rd_kafka_TopicDescription_topic_id(topicdesc);
    if (topic_id) {
        const char *base64 = rd_kafka_Uuid_base64str(topic_id);
        if (base64) {
            zend_update_property_string(NULL, Z_OBJ_P(return_value), "topic_id", sizeof("topic_id") - 1, base64);
        } else {
            zend_update_property_null(NULL, Z_OBJ_P(return_value), "topic_id", sizeof("topic_id") - 1);
        }
    } else {
        zend_update_property_null(NULL, Z_OBJ_P(return_value), "topic_id", sizeof("topic_id") - 1);
    }

    zend_update_property_bool(NULL, Z_OBJ_P(return_value), "is_internal", sizeof("is_internal") - 1,
        rd_kafka_TopicDescription_is_internal(topicdesc));

    error = rd_kafka_TopicDescription_error(topicdesc);
    if (error) {
        zend_update_property_long(NULL, Z_OBJ_P(return_value), "error", sizeof("error") - 1,
            rd_kafka_error_code(error));
        const char *errstr = rd_kafka_error_string(error);
        if (errstr && rd_kafka_error_code(error) != RD_KAFKA_RESP_ERR_NO_ERROR) {
            zend_update_property_string(NULL, Z_OBJ_P(return_value), "error_string", sizeof("error_string") - 1, errstr);
        } else {
            zend_update_property_null(NULL, Z_OBJ_P(return_value), "error_string", sizeof("error_string") - 1);
        }
    } else {
        zend_update_property_long(NULL, Z_OBJ_P(return_value), "error", sizeof("error") - 1, 0);
        zend_update_property_null(NULL, Z_OBJ_P(return_value), "error_string", sizeof("error_string") - 1);
    }

    partitions = rd_kafka_TopicDescription_partitions(topicdesc, &partition_cnt);
    array_init_size(&partitions_zv, partition_cnt);
    for (i = 0; i < partition_cnt; i++) {
        zval part_zv;
        kafka_topic_partition_info_to_zval(&part_zv, partitions[i]);
        add_next_index_zval(&partitions_zv, &part_zv);
    }
    zend_update_property(NULL, Z_OBJ_P(return_value), "partitions", sizeof("partitions") - 1, &partitions_zv);
    zval_ptr_dtor(&partitions_zv);
}
/* }}} */

/* {{{ AdminClient::describeTopics(array $topics, ?AdminOptions $options = null): array */
ZEND_METHOD(RdKafka_Admin_AdminClient, describeTopics)
{
    zval *ztopics, *zoptions = NULL;
    kafka_admin_client_object *intern;
    rd_kafka_AdminOptions_t *options = NULL;
    rd_kafka_queue_t *queue = NULL;
    rd_kafka_event_t *event = NULL;
    rd_kafka_TopicCollection_t *topic_collection = NULL;
    zval *zitem;
    size_t i;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "a|O!", &ztopics, &zoptions, ce_kafka_admin_options) == FAILURE) {
        return;
    }

    intern = get_admin_client_object(getThis());
    if (!intern->rk) {
        zend_throw_exception(ce_kafka_exception, "AdminClient is not properly initialized", 0);
        return;
    }

    if (zoptions) {
        kafka_admin_options_object *options_intern = get_admin_options_object(zoptions);
        options = options_intern->options;
    }

    /* Build const char** array from PHP string array */
    size_t topic_cnt = zend_hash_num_elements(Z_ARRVAL_P(ztopics));
    if (topic_cnt == 0) {
        zend_throw_exception(ce_kafka_exception, "topics array must not be empty", 0);
        return;
    }

    const char **topic_names = ecalloc(topic_cnt, sizeof(const char *));
    i = 0;
    ZEND_HASH_FOREACH_VAL(Z_ARRVAL_P(ztopics), zitem) {
        if (Z_TYPE_P(zitem) != IS_STRING) {
            zend_throw_exception(ce_kafka_exception, "All items in topics must be strings", 0);
            efree(topic_names);
            return;
        }
        topic_names[i++] = Z_STRVAL_P(zitem);
    } ZEND_HASH_FOREACH_END();

    topic_collection = rd_kafka_TopicCollection_of_topic_names(topic_names, topic_cnt);
    efree(topic_names);

    if (!topic_collection) {
        zend_throw_exception(ce_kafka_exception, "Failed to create TopicCollection", 0);
        return;
    }

    queue = rd_kafka_queue_new(intern->rk);
    if (!queue) {
        rd_kafka_TopicCollection_destroy(topic_collection);
        zend_throw_exception(ce_kafka_exception, "Failed to create admin queue", 0);
        return;
    }

    rd_kafka_DescribeTopics(intern->rk, topic_collection, options, queue);
    rd_kafka_TopicCollection_destroy(topic_collection);

    event = rd_kafka_queue_poll(queue, ADMIN_DEFAULT_TIMEOUT_MS);
    rd_kafka_queue_destroy(queue);

    if (!event) {
        zend_throw_exception(ce_kafka_exception, "Timed out waiting for DescribeTopics result", RD_KAFKA_RESP_ERR__TIMED_OUT);
        return;
    }

    if (rd_kafka_event_error(event)) {
        zend_throw_exception(ce_kafka_exception,
            rd_kafka_event_error_string(event),
            rd_kafka_event_error(event));
        rd_kafka_event_destroy(event);
        return;
    }

    const rd_kafka_DescribeTopics_result_t *result = rd_kafka_event_DescribeTopics_result(event);
    if (!result) {
        zend_throw_exception(ce_kafka_exception, "Unexpected event type in DescribeTopics result", 0);
        rd_kafka_event_destroy(event);
        return;
    }

    size_t result_cnt;
    const rd_kafka_TopicDescription_t **descriptions = rd_kafka_DescribeTopics_result_topics(result, &result_cnt);

    array_init_size(return_value, result_cnt);
    for (i = 0; i < result_cnt; i++) {
        zval desc_zv;
        kafka_topic_description_to_zval(&desc_zv, descriptions[i]);
        add_next_index_zval(return_value, &desc_zv);
    }

    rd_kafka_event_destroy(event);
}
/* }}} */

/* {{{ AdminClient::deleteRecords(array $topic_partitions, ?AdminOptions $options = null): array */
ZEND_METHOD(RdKafka_Admin_AdminClient, deleteRecords)
{
    zval *ztopic_partitions, *zoptions = NULL;
    kafka_admin_client_object *intern;
    rd_kafka_AdminOptions_t *options = NULL;
    rd_kafka_queue_t *queue = NULL;
    rd_kafka_event_t *event = NULL;
    rd_kafka_DeleteRecords_t *del_records = NULL;
    rd_kafka_DeleteRecords_t *del_records_arr[1];
    rd_kafka_topic_partition_list_t *partitions;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "a|O!", &ztopic_partitions, &zoptions, ce_kafka_admin_options) == FAILURE) {
        return;
    }

    intern = get_admin_client_object(getThis());
    if (!intern->rk) {
        zend_throw_exception(ce_kafka_exception, "AdminClient is not properly initialized", 0);
        return;
    }

    if (zoptions) {
        kafka_admin_options_object *options_intern = get_admin_options_object(zoptions);
        options = options_intern->options;
    }

    /* Convert PHP TopicPartition[] to rd_kafka_topic_partition_list_t */
    partitions = array_arg_to_kafka_topic_partition_list(1, Z_ARRVAL_P(ztopic_partitions));
    if (!partitions) {
        return; /* Exception already thrown by array_arg_to_kafka_topic_partition_list */
    }

    del_records = rd_kafka_DeleteRecords_new(partitions);
    rd_kafka_topic_partition_list_destroy(partitions);

    if (!del_records) {
        zend_throw_exception(ce_kafka_exception, "Failed to create DeleteRecords", 0);
        return;
    }

    queue = rd_kafka_queue_new(intern->rk);
    if (!queue) {
        rd_kafka_DeleteRecords_destroy(del_records);
        zend_throw_exception(ce_kafka_exception, "Failed to create admin queue", 0);
        return;
    }

    del_records_arr[0] = del_records;
    rd_kafka_DeleteRecords(intern->rk, del_records_arr, 1, options, queue);

    event = rd_kafka_queue_poll(queue, ADMIN_DEFAULT_TIMEOUT_MS);

    rd_kafka_DeleteRecords_destroy(del_records);
    rd_kafka_queue_destroy(queue);

    if (!event) {
        zend_throw_exception(ce_kafka_exception, "Timed out waiting for DeleteRecords result", RD_KAFKA_RESP_ERR__TIMED_OUT);
        return;
    }

    if (rd_kafka_event_error(event)) {
        zend_throw_exception(ce_kafka_exception,
            rd_kafka_event_error_string(event),
            rd_kafka_event_error(event));
        rd_kafka_event_destroy(event);
        return;
    }

    const rd_kafka_DeleteRecords_result_t *result = rd_kafka_event_DeleteRecords_result(event);
    if (!result) {
        zend_throw_exception(ce_kafka_exception, "Unexpected event type in DeleteRecords result", 0);
        rd_kafka_event_destroy(event);
        return;
    }

    const rd_kafka_topic_partition_list_t *result_offsets = rd_kafka_DeleteRecords_result_offsets(result);
    kafka_topic_partition_list_to_array(return_value, (rd_kafka_topic_partition_list_t *)result_offsets);

    rd_kafka_event_destroy(event);
}
/* }}} */

/* {{{ kafka_admin_client_minit */
void kafka_admin_client_minit(INIT_FUNC_ARGS)
{
    /* AdminClient */
    ce_kafka_admin_client = register_class_RdKafka_Admin_AdminClient();
    ce_kafka_admin_client->create_object = admin_client_new;

    memcpy(&admin_client_object_handlers, &kafka_default_object_handlers, sizeof(zend_object_handlers));
    admin_client_object_handlers.clone_obj = NULL;
    admin_client_object_handlers.free_obj = admin_client_free;
    admin_client_object_handlers.offset = XtOffsetOf(kafka_admin_client_object, std);

    /* AdminOptions */
    ce_kafka_admin_options = register_class_RdKafka_Admin_AdminOptions();
    ce_kafka_admin_options->create_object = admin_options_new;

    memcpy(&admin_options_object_handlers, &kafka_default_object_handlers, sizeof(zend_object_handlers));
    admin_options_object_handlers.clone_obj = NULL;
    admin_options_object_handlers.free_obj = admin_options_free;
    admin_options_object_handlers.offset = XtOffsetOf(kafka_admin_options_object, std);

    /* NewTopic */
    ce_kafka_new_topic = register_class_RdKafka_Admin_NewTopic();
    ce_kafka_new_topic->create_object = new_topic_new;

    memcpy(&new_topic_object_handlers, &kafka_default_object_handlers, sizeof(zend_object_handlers));
    new_topic_object_handlers.clone_obj = NULL;
    new_topic_object_handlers.free_obj = new_topic_free;
    new_topic_object_handlers.offset = XtOffsetOf(kafka_new_topic_object, std);

    /* DeleteTopic */
    ce_kafka_delete_topic = register_class_RdKafka_Admin_DeleteTopic();
    ce_kafka_delete_topic->create_object = delete_topic_new;

    memcpy(&delete_topic_object_handlers, &kafka_default_object_handlers, sizeof(zend_object_handlers));
    delete_topic_object_handlers.clone_obj = NULL;
    delete_topic_object_handlers.free_obj = delete_topic_free;
    delete_topic_object_handlers.offset = XtOffsetOf(kafka_delete_topic_object, std);

    /* NewPartitions */
    ce_kafka_new_partitions = register_class_RdKafka_Admin_NewPartitions();
    ce_kafka_new_partitions->create_object = new_partitions_new;

    memcpy(&new_partitions_object_handlers, &kafka_default_object_handlers, sizeof(zend_object_handlers));
    new_partitions_object_handlers.clone_obj = NULL;
    new_partitions_object_handlers.free_obj = new_partitions_free;
    new_partitions_object_handlers.offset = XtOffsetOf(kafka_new_partitions_object, std);

    /* TopicResult - no custom object handler needed, uses default */
    ce_kafka_topic_result = register_class_RdKafka_Admin_TopicResult();

    /* Node - property-based, no custom handler */
    ce_kafka_node = register_class_RdKafka_Admin_Node();

    /* TopicPartitionInfo - property-based, no custom handler */
    ce_kafka_topic_partition_info = register_class_RdKafka_Admin_TopicPartitionInfo();

    /* TopicDescription - property-based, no custom handler */
    ce_kafka_topic_description = register_class_RdKafka_Admin_TopicDescription();

    /* Register RD_KAFKA_ADMIN_OP_* constants */
    REGISTER_LONG_CONSTANT("RD_KAFKA_ADMIN_OP_ANY", RD_KAFKA_ADMIN_OP_ANY, CONST_CS | CONST_PERSISTENT);
    REGISTER_LONG_CONSTANT("RD_KAFKA_ADMIN_OP_CREATETOPICS", RD_KAFKA_ADMIN_OP_CREATETOPICS, CONST_CS | CONST_PERSISTENT);
    REGISTER_LONG_CONSTANT("RD_KAFKA_ADMIN_OP_DELETETOPICS", RD_KAFKA_ADMIN_OP_DELETETOPICS, CONST_CS | CONST_PERSISTENT);
    REGISTER_LONG_CONSTANT("RD_KAFKA_ADMIN_OP_CREATEPARTITIONS", RD_KAFKA_ADMIN_OP_CREATEPARTITIONS, CONST_CS | CONST_PERSISTENT);
    REGISTER_LONG_CONSTANT("RD_KAFKA_ADMIN_OP_ALTERCONFIGS", RD_KAFKA_ADMIN_OP_ALTERCONFIGS, CONST_CS | CONST_PERSISTENT);
    REGISTER_LONG_CONSTANT("RD_KAFKA_ADMIN_OP_DESCRIBECONFIGS", RD_KAFKA_ADMIN_OP_DESCRIBECONFIGS, CONST_CS | CONST_PERSISTENT);
    REGISTER_LONG_CONSTANT("RD_KAFKA_ADMIN_OP_DESCRIBETOPICS", RD_KAFKA_ADMIN_OP_DESCRIBETOPICS, CONST_CS | CONST_PERSISTENT);
    REGISTER_LONG_CONSTANT("RD_KAFKA_ADMIN_OP_DELETERECORDS", RD_KAFKA_ADMIN_OP_DELETERECORDS, CONST_CS | CONST_PERSISTENT);
}
/* }}} */
