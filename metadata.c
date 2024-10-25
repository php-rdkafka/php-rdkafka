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
#include "metadata_collection.h"
#include "metadata_topic.h"
#include "metadata_broker.h"
#include "metadata_partition.h"
#include "Zend/zend_exceptions.h"
#include "metadata_arginfo.h"

typedef struct _object_intern {
    const rd_kafka_metadata_t *metadata;
    zend_object               std;
} object_intern;

static HashTable *get_debug_info(zend_object *object, int *is_temp);

static zend_class_entry * ce;
static zend_object_handlers handlers;

static void brokers_collection(zval *return_value, zend_object *parent, object_intern *intern) { /* {{{ */
    kafka_metadata_collection_init(return_value, parent, intern->metadata->brokers, intern->metadata->broker_cnt, sizeof(*intern->metadata->brokers), kafka_metadata_broker_ctor);
}
/* }}} */

static void topics_collection(zval *return_value, zend_object *parent, object_intern *intern) { /* {{{ */
    kafka_metadata_collection_init(return_value, parent, intern->metadata->topics, intern->metadata->topic_cnt, sizeof(*intern->metadata->topics), kafka_metadata_topic_ctor);
}
/* }}} */

static void kafka_metadata_free(zend_object *object) /* {{{ */
{
    object_intern *intern = php_kafka_from_obj(object_intern, object);

    if (intern->metadata) {
        rd_kafka_metadata_destroy(intern->metadata);
    }

    zend_object_std_dtor(&intern->std);
}
/* }}} */

static zend_object *kafka_metadata_new(zend_class_entry *class_type) /* {{{ */
{
    zend_object* retval;
    object_intern *intern;

    intern = zend_object_alloc(sizeof(*intern), class_type);
    zend_object_std_init(&intern->std, class_type);
    object_properties_init(&intern->std, class_type);

    retval = &intern->std;
    retval->handlers = &handlers;

    return retval;
}
/* }}} */

static object_intern * get_object(zval *zmetadata)
{
    object_intern *ometadata = Z_RDKAFKA_P(object_intern, zmetadata);

    if (!ometadata->metadata) {
        zend_throw_exception_ex(NULL, 0, "RdKafka\\Metadata::__construct() has not been called");
        return NULL;
    }

    return ometadata;
}

static HashTable *get_debug_info(zend_object *object, int *is_temp) /* {{{ */
{
    zval ary;
    object_intern *intern;
    zval brokers;
    zval topics;

    *is_temp = 1;

    array_init(&ary);

    intern = php_kafka_from_obj(object_intern, object);
    if (!intern || !intern->metadata) {
        return Z_ARRVAL(ary);
    }

    ZVAL_NULL(&brokers);
    brokers_collection(&brokers, object, intern);
    add_assoc_zval(&ary, "brokers", &brokers);

    ZVAL_NULL(&topics);
    topics_collection(&topics, object, intern);
    add_assoc_zval(&ary, "topics", &topics);

    add_assoc_long(&ary, "orig_broker_id", intern->metadata->orig_broker_id);
    add_assoc_string(&ary, "orig_broker_name", intern->metadata->orig_broker_name);

    return Z_ARRVAL(ary);
}
/* }}} */

/* {{{ proto long RdKafka\Metadata::getOrigBrokerId()
   Broker originating this metadata */
PHP_METHOD(RdKafka_Metadata, getOrigBrokerId)
{
    object_intern *intern;

    if (zend_parse_parameters_none() == FAILURE) {
        return;
    }

    intern = get_object(getThis());
    if (!intern) {
        return;
    }

    RETURN_LONG(intern->metadata->orig_broker_id);
}
/* }}} */

/* {{{ proto string RdKafka\Metadata::getOrigBrokerName()
   Name of originating broker */
PHP_METHOD(RdKafka_Metadata, getOrigBrokerName)
{
    object_intern *intern;

    if (zend_parse_parameters_none() == FAILURE) {
        return;
    }

    intern = get_object(getThis());
    if (!intern) {
        return;
    }

    RETURN_STRING(intern->metadata->orig_broker_name);
}
/* }}} */

/* {{{ proto RdKafka\Metadata\Collection RdKafka\Metadata::getBrokers()
   Topics */
PHP_METHOD(RdKafka_Metadata, getBrokers)
{
    object_intern *intern;

    if (zend_parse_parameters_none() == FAILURE) {
        return;
    }

    intern = get_object(getThis());
    if (!intern) {
        return;
    }

    brokers_collection(return_value, Z_OBJ_P(getThis()), intern);
}
/* }}} */

/* {{{ proto RdKafka\Metadata\Collection RdKafka\Metadata::getTopics()
   Topics */
PHP_METHOD(RdKafka_Metadata, getTopics)
{
    object_intern *intern;

    if (zend_parse_parameters_none() == FAILURE) {
        return;
    }

    intern = get_object(getThis());
    if (!intern) {
        return;
    }

    topics_collection(return_value, Z_OBJ_P(getThis()), intern);
}
/* }}} */

void kafka_metadata_minit(INIT_FUNC_ARGS)
{
    ce = register_class_RdKafka_Metadata();
    ce->create_object = kafka_metadata_new;

    handlers = kafka_default_object_handlers;
    handlers.get_debug_info = get_debug_info;
    handlers.free_obj = kafka_metadata_free;
    handlers.offset = XtOffsetOf(object_intern, std);

    kafka_metadata_topic_minit(INIT_FUNC_ARGS_PASSTHRU);
    kafka_metadata_broker_minit(INIT_FUNC_ARGS_PASSTHRU);
    kafka_metadata_partition_minit(INIT_FUNC_ARGS_PASSTHRU);
    kafka_metadata_collection_minit(INIT_FUNC_ARGS_PASSTHRU);
}

void kafka_metadata_init(zval *return_value, const rd_kafka_metadata_t *metadata)
{
    object_intern *intern;

    if (object_init_ex(return_value, ce) != SUCCESS) {
        return;
    }

    intern = Z_RDKAFKA_P(object_intern, return_value);
    if (!intern) {
        return;
    }

    intern->metadata = metadata;
}
