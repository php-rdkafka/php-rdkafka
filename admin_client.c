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


/* Class entries */
zend_class_entry *ce_kafka_admin_options;
zend_class_entry *ce_kafka_new_topic;
zend_class_entry *ce_kafka_delete_topic;
zend_class_entry *ce_kafka_new_partitions;
zend_class_entry *ce_kafka_topic_result;
#ifdef HAS_RD_KAFKA_DESCRIBE_TOPICS
zend_class_entry *ce_kafka_node;
zend_class_entry *ce_kafka_topic_partition_info;
zend_class_entry *ce_kafka_topic_description;
#endif

/* Object handlers */
static zend_object_handlers admin_options_object_handlers;
static zend_object_handlers new_topic_object_handlers;
static zend_object_handlers delete_topic_object_handlers;
static zend_object_handlers new_partitions_object_handlers;

/* Accessor macros are in admin_client.h. */

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
void kafka_topic_results_to_array(zval *return_value, const rd_kafka_topic_result_t **results, size_t cnt)
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

#ifdef HAS_RD_KAFKA_DESCRIBE_TOPICS
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
#endif

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

#ifdef HAS_RD_KAFKA_DESCRIBE_TOPICS
/* {{{ Helper: convert rd_kafka_Node_t to PHP Node object */
void kafka_node_to_zval(zval *return_value, const rd_kafka_Node_t *node)
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
void kafka_node_array_to_zval(zval *return_value, const rd_kafka_Node_t **nodes, size_t cnt)
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
void kafka_topic_partition_info_to_zval(zval *return_value, const rd_kafka_TopicPartitionInfo_t *partition)
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
void kafka_topic_description_to_zval(zval *return_value, const rd_kafka_TopicDescription_t *topicdesc)
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

#endif /* HAS_RD_KAFKA_DESCRIBE_TOPICS */

/* {{{ kafka_admin_client_minit */
void kafka_admin_client_minit(INIT_FUNC_ARGS)
{
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

#ifdef HAS_RD_KAFKA_DESCRIBE_TOPICS
    /* Node - property-based, no custom handler */
    ce_kafka_node = register_class_RdKafka_Admin_Node();

    /* TopicPartitionInfo - property-based, no custom handler */
    ce_kafka_topic_partition_info = register_class_RdKafka_Admin_TopicPartitionInfo();

    /* TopicDescription - property-based, no custom handler */
    ce_kafka_topic_description = register_class_RdKafka_Admin_TopicDescription();
#endif

    /* Register RD_KAFKA_ADMIN_OP_* constants */
    REGISTER_LONG_CONSTANT("RD_KAFKA_ADMIN_OP_ANY", RD_KAFKA_ADMIN_OP_ANY, CONST_CS | CONST_PERSISTENT);
    REGISTER_LONG_CONSTANT("RD_KAFKA_ADMIN_OP_CREATETOPICS", RD_KAFKA_ADMIN_OP_CREATETOPICS, CONST_CS | CONST_PERSISTENT);
    REGISTER_LONG_CONSTANT("RD_KAFKA_ADMIN_OP_DELETETOPICS", RD_KAFKA_ADMIN_OP_DELETETOPICS, CONST_CS | CONST_PERSISTENT);
    REGISTER_LONG_CONSTANT("RD_KAFKA_ADMIN_OP_CREATEPARTITIONS", RD_KAFKA_ADMIN_OP_CREATEPARTITIONS, CONST_CS | CONST_PERSISTENT);
    REGISTER_LONG_CONSTANT("RD_KAFKA_ADMIN_OP_ALTERCONFIGS", RD_KAFKA_ADMIN_OP_ALTERCONFIGS, CONST_CS | CONST_PERSISTENT);
    REGISTER_LONG_CONSTANT("RD_KAFKA_ADMIN_OP_DESCRIBECONFIGS", RD_KAFKA_ADMIN_OP_DESCRIBECONFIGS, CONST_CS | CONST_PERSISTENT);
    REGISTER_LONG_CONSTANT("RD_KAFKA_ADMIN_OP_DELETERECORDS", RD_KAFKA_ADMIN_OP_DELETERECORDS, CONST_CS | CONST_PERSISTENT);
#ifdef HAS_RD_KAFKA_DESCRIBE_TOPICS
    REGISTER_LONG_CONSTANT("RD_KAFKA_ADMIN_OP_DESCRIBETOPICS", RD_KAFKA_ADMIN_OP_DESCRIBETOPICS, CONST_CS | CONST_PERSISTENT);
#endif
}
/* }}} */
