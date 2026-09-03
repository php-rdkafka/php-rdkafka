/*
  +----------------------------------------------------------------------+
  | php-rdkafka                                                          |
  +----------------------------------------------------------------------+
  | Copyright (c) 2026 Ralph Schindler                                   |
  +----------------------------------------------------------------------+
  | This source file is subject to version 3.01 of the PHP license,      |
  | that is bundled with this package in the file LICENSE, and is        |
  | available through the world-wide-web at the following url:           |
  | http://www.php.net/license/3_01.txt                                  |
  | If you did not receive a copy of the PHP license and are unable to   |
  | obtain it through the world-wide-web, please send a note to          |
  | license@php.net so we can mail you a copy immediately.               |
  +----------------------------------------------------------------------+
  | Author: Ralph Schindler <ralph@ralphschindler.com>                   |
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
#include "consumer_group_metadata.h"
#include "consumer_group_metadata_arginfo.h"

zend_class_entry * ce_kafka_consumer_group_metadata;

static zend_object_handlers handlers;

/* {{{ proto void RdKafka\ConsumerGroupMetadata::__construct(string $group_id[, int $generation_id[, string $member_id[, string $group_instance_id]]]) */
PHP_METHOD(RdKafka_ConsumerGroupMetadata, __construct)
{
    kafka_consumer_group_metadata_object *intern;
    char *group_id;
    size_t group_id_len;
    zend_long generation_id = -1;
    char *member_id = "";
    size_t member_id_len = 0;
    char *group_instance_id = NULL;
    size_t group_instance_id_len = 0;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "s|lss!",
            &group_id, &group_id_len,
            &generation_id,
            &member_id, &member_id_len,
            &group_instance_id, &group_instance_id_len) == FAILURE) {
        return;
    }

    intern = php_kafka_from_obj(kafka_consumer_group_metadata_object, Z_OBJ_P(getThis()));

    if (intern->cgmd) {
        rd_kafka_consumer_group_metadata_destroy(intern->cgmd);
        intern->cgmd = NULL;
    }

#ifdef HAS_RD_KAFKA_CONSUMER_GROUP_METADATA_NEW_WITH_GENID
    if (generation_id != -1 || member_id_len > 0 || group_instance_id != NULL) {
        intern->cgmd = rd_kafka_consumer_group_metadata_new_with_genid(
            group_id,
            (int32_t) generation_id,
            member_id,
            group_instance_id
        );
    } else {
        intern->cgmd = rd_kafka_consumer_group_metadata_new(group_id);
    }
#else
    if (generation_id != -1 || member_id_len > 0 || group_instance_id != NULL) {
        zend_throw_exception(ce_kafka_exception, "Full ConsumerGroupMetadata constructor requires librdkafka >= 1.7.0", 0);
        return;
    }
    intern->cgmd = rd_kafka_consumer_group_metadata_new(group_id);
#endif

    if (!intern->cgmd) {
        zend_throw_exception(ce_kafka_exception, "Failed to create ConsumerGroupMetadata", 0);
        return;
    }
}
/* }}} */

#ifdef HAS_RD_KAFKA_CONSUMER_GROUP_METADATA_GETTERS

/* {{{ proto string RdKafka\ConsumerGroupMetadata::getGroupId() */
PHP_METHOD(RdKafka_ConsumerGroupMetadata, getGroupId)
{
    kafka_consumer_group_metadata_object *intern;
    const char *str;

    if (zend_parse_parameters_none() == FAILURE) {
        return;
    }

    intern = get_consumer_group_metadata_object(getThis());
    if (!intern) { return; }

    str = rd_kafka_consumer_group_metadata_group_id(intern->cgmd);
    RETURN_STRING(str ? str : "");
}
/* }}} */

/* {{{ proto string RdKafka\ConsumerGroupMetadata::getMemberId() */
PHP_METHOD(RdKafka_ConsumerGroupMetadata, getMemberId)
{
    kafka_consumer_group_metadata_object *intern;
    const char *str;

    if (zend_parse_parameters_none() == FAILURE) {
        return;
    }

    intern = get_consumer_group_metadata_object(getThis());
    if (!intern) { return; }

    str = rd_kafka_consumer_group_metadata_member_id(intern->cgmd);
    RETURN_STRING(str ? str : "");
}
/* }}} */

/* {{{ proto string|null RdKafka\ConsumerGroupMetadata::getGroupInstanceId() */
PHP_METHOD(RdKafka_ConsumerGroupMetadata, getGroupInstanceId)
{
    kafka_consumer_group_metadata_object *intern;
    const char *str;

    if (zend_parse_parameters_none() == FAILURE) {
        return;
    }

    intern = get_consumer_group_metadata_object(getThis());
    if (!intern) { return; }

    str = rd_kafka_consumer_group_metadata_group_instance_id(intern->cgmd);
    if (str) {
        RETURN_STRING(str);
    } else {
        RETURN_NULL();
    }
}
/* }}} */

/* {{{ proto int RdKafka\ConsumerGroupMetadata::getGenerationId() */
PHP_METHOD(RdKafka_ConsumerGroupMetadata, getGenerationId)
{
    kafka_consumer_group_metadata_object *intern;

    if (zend_parse_parameters_none() == FAILURE) {
        return;
    }

    intern = get_consumer_group_metadata_object(getThis());
    if (!intern) { return; }

    RETURN_LONG(rd_kafka_consumer_group_metadata_generation_id(intern->cgmd));
}
/* }}} */

/* {{{ */
static HashTable *get_debug_info(zend_object *object, int *is_temp)
{
    zval ary;
    kafka_consumer_group_metadata_object *intern;
    const char *str;
    int32_t genid;

    *is_temp = 1;
    array_init(&ary);

    intern = php_kafka_from_obj(kafka_consumer_group_metadata_object, object);

    if (!intern || !intern->cgmd) {
        return Z_ARRVAL(ary);
    }

    str = rd_kafka_consumer_group_metadata_group_id(intern->cgmd);
    add_assoc_string(&ary, "group_id", str ? str : "");

    str = rd_kafka_consumer_group_metadata_member_id(intern->cgmd);
    add_assoc_string(&ary, "member_id", str ? str : "");

    str = rd_kafka_consumer_group_metadata_group_instance_id(intern->cgmd);
    if (str) {
        add_assoc_string(&ary, "group_instance_id", str);
    } else {
        add_assoc_null(&ary, "group_instance_id");
    }

    genid = rd_kafka_consumer_group_metadata_generation_id(intern->cgmd);
    add_assoc_long(&ary, "generation_id", genid);

    return Z_ARRVAL(ary);
}
/* }}} */

#else /* HAS_RD_KAFKA_CONSUMER_GROUP_METADATA_GETTERS */

/* {{{ proto string RdKafka\ConsumerGroupMetadata::getGroupId() */
PHP_METHOD(RdKafka_ConsumerGroupMetadata, getGroupId)
{
    zend_throw_exception(ce_kafka_exception, "RdKafka\\ConsumerGroupMetadata::getGroupId() requires librdkafka >= 2.8.0", 0);
}
/* }}} */

/* {{{ proto string RdKafka\ConsumerGroupMetadata::getMemberId() */
PHP_METHOD(RdKafka_ConsumerGroupMetadata, getMemberId)
{
    zend_throw_exception(ce_kafka_exception, "RdKafka\\ConsumerGroupMetadata::getMemberId() requires librdkafka >= 2.8.0", 0);
}
/* }}} */

/* {{{ proto string|null RdKafka\ConsumerGroupMetadata::getGroupInstanceId() */
PHP_METHOD(RdKafka_ConsumerGroupMetadata, getGroupInstanceId)
{
    zend_throw_exception(ce_kafka_exception, "RdKafka\\ConsumerGroupMetadata::getGroupInstanceId() requires librdkafka >= 2.8.0", 0);
}
/* }}} */

/* {{{ proto int RdKafka\ConsumerGroupMetadata::getGenerationId() */
PHP_METHOD(RdKafka_ConsumerGroupMetadata, getGenerationId)
{
    zend_throw_exception(ce_kafka_exception, "RdKafka\\ConsumerGroupMetadata::getGenerationId() requires librdkafka >= 2.8.0", 0);
}
/* }}} */

#endif /* HAS_RD_KAFKA_CONSUMER_GROUP_METADATA_GETTERS */

/* {{{ */
static void free_object(zend_object *object)
{
    kafka_consumer_group_metadata_object *intern = php_kafka_from_obj(kafka_consumer_group_metadata_object, object);

    if (intern->cgmd) {
        rd_kafka_consumer_group_metadata_destroy(intern->cgmd);
        intern->cgmd = NULL;
    }

    zend_object_std_dtor(&intern->std);
}
/* }}} */

/* {{{ */
static zend_object *create_object(zend_class_entry *class_type)
{
    kafka_consumer_group_metadata_object *intern;

    intern = zend_object_alloc(sizeof(*intern), class_type);
    zend_object_std_init(&intern->std, class_type);
    object_properties_init(&intern->std, class_type);

    intern->std.handlers = &handlers;

    return &intern->std;
}
/* }}} */

/* {{{ */
kafka_consumer_group_metadata_object * get_consumer_group_metadata_object(zval *z)
{
    kafka_consumer_group_metadata_object *intern = Z_RDKAFKA_P(kafka_consumer_group_metadata_object, z);

    if (!intern->cgmd) {
        zend_throw_exception_ex(NULL, 0, "RdKafka\\ConsumerGroupMetadata is not initialized");
        return NULL;
    }

    return intern;
}
/* }}} */

/* {{{ */
void kafka_consumer_group_metadata_minit(INIT_FUNC_ARGS)
{
    memcpy(&handlers, &std_object_handlers, sizeof(handlers));
    handlers.free_obj = free_object;
    handlers.clone_obj = NULL;
    handlers.offset = offsetof(kafka_consumer_group_metadata_object, std);
#ifdef HAS_RD_KAFKA_CONSUMER_GROUP_METADATA_GETTERS
    handlers.get_debug_info = get_debug_info;
#endif

    ce_kafka_consumer_group_metadata = register_class_RdKafka_ConsumerGroupMetadata();
    ce_kafka_consumer_group_metadata->create_object = create_object;
}
/* }}} */
