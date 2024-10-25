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
#include "metadata_collection.h"
#include "Zend/zend_exceptions.h"
#include "metadata_collection_arginfo.h"

typedef struct _object_intern {
    zval                             zmetadata;
    const void                       *items;
    size_t                           item_cnt;
    size_t                           item_size;
    size_t                           position;
    kafka_metadata_collection_ctor_t ctor;
    zend_object                      std;
} object_intern;

static HashTable *get_debug_info(zend_object *object, int *is_temp);

static zend_class_entry *ce;
static zend_object_handlers handlers;

static void free_object(zend_object *object) /* {{{ */
{
    object_intern *intern = php_kafka_from_obj(object_intern, object);

    if (intern->items) {
        zval_dtor(&intern->zmetadata);
    }

    zend_object_std_dtor(&intern->std);
}
/* }}} */

static zend_object *create_object(zend_class_entry *class_type) /* {{{ */
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

static object_intern * get_object(zval *zmti)
{
    object_intern *omti = Z_RDKAFKA_P(object_intern, zmti);

    if (!omti->items) {
        zend_throw_exception_ex(NULL, 0, "RdKafka\\Metadata\\Collection::__construct() has not been called");
        return NULL;
    }

    return omti;
}

static HashTable *get_debug_info(zend_object *object, int *is_temp) /* {{{ */
{
    zval ary;
    object_intern *intern;
    size_t i;
    zval item;

    *is_temp = 1;

    array_init(&ary);

    intern = php_kafka_from_obj(object_intern, object);
    if (!intern || !intern->items) {
        return Z_ARRVAL(ary);
    }
    
    for (i = 0; i < intern->item_cnt; i++) {
        ZVAL_NULL(&item);
        intern->ctor(&item, &intern->zmetadata, (char *)intern->items + i * intern->item_size);
        add_next_index_zval(&ary, &item);
    }

    return Z_ARRVAL(ary);
}
/* }}} */

/* {{{ proto int RdKafka\Metadata\Collection::count()
   */
PHP_METHOD(RdKafka_Metadata_Collection, count)
{
    object_intern *intern;

    if (zend_parse_parameters_none() == FAILURE) {
        return;
    }

    intern = get_object(getThis());
    if (!intern) {
        return;
    }

    RETURN_LONG(intern->item_cnt);
}
/* }}} */

/* {{{ proto void RdKafka\Metadata\Collection::rewind()
   */
PHP_METHOD(RdKafka_Metadata_Collection, rewind)
{
    object_intern *intern;

    if (zend_parse_parameters_none() == FAILURE) {
        return;
    }

    intern = get_object(getThis());
    if (!intern) {
        return;
    }

    intern->position = 0;
}
/* }}} */

/* {{{ proto mixed RdKafka\Metadata\Collection::current()
   */
PHP_METHOD(RdKafka_Metadata_Collection, current)
{
    object_intern *intern;

    if (zend_parse_parameters_none() == FAILURE) {
        return;
    }

    intern = get_object(getThis());
    if (!intern) {
        return;
    }

    if (intern->position >= intern->item_cnt) {
        zend_throw_exception(ce_kafka_exception, "Called current() on invalid iterator", 0);
        return;
    }

    intern->ctor(return_value, &intern->zmetadata, (char *)intern->items + intern->position * intern->item_size);
}
/* }}} */

/* {{{ proto mixed RdKafka\Metadata\Collection::key()
   */
PHP_METHOD(RdKafka_Metadata_Collection, key)
{
    object_intern *intern;

    if (zend_parse_parameters_none() == FAILURE) {
        return;
    }

    intern = get_object(getThis());
    if (!intern) {
        return;
    }

    if (intern->position >= intern->item_cnt) {
        zend_throw_exception(ce_kafka_exception, "Called key() on invalid iterator", 0);
        return;
    }

    RETURN_LONG(intern->position);
}
/* }}} */

/* {{{ proto void RdKafka\Metadata\Collection::next()
   */
PHP_METHOD(RdKafka_Metadata_Collection, next)
{
    object_intern *intern;

    if (zend_parse_parameters_none() == FAILURE) {
        return;
    }

    intern = get_object(getThis());
    if (!intern) {
        return;
    }

    intern->position ++;
}
/* }}} */

/* {{{ proto bool RdKafka\Metadata\Collection::valid()
   */
PHP_METHOD(RdKafka_Metadata_Collection, valid)
{
    object_intern *intern;

    if (zend_parse_parameters_none() == FAILURE) {
        return;
    }

    intern = get_object(getThis());
    if (!intern) {
        return;
    }

    RETURN_BOOL(intern->position < intern->item_cnt);
}
/* }}} */

void kafka_metadata_collection_minit(INIT_FUNC_ARGS)
{
    ce = register_class_RdKafka_Metadata_Collection(zend_ce_countable, zend_ce_iterator);
    ce->create_object = create_object;

    handlers = kafka_default_object_handlers;
    handlers.get_debug_info = get_debug_info;
    handlers.free_obj = free_object;
    handlers.offset = XtOffsetOf(object_intern, std);
}

void kafka_metadata_collection_init(zval *return_value, zend_object *zmetadata, const void * items, size_t item_cnt, size_t item_size, kafka_metadata_collection_ctor_t ctor)
{
    object_intern *intern;

    if (object_init_ex(return_value, ce) != SUCCESS) {
        return;
    }

    intern = Z_RDKAFKA_P(object_intern, return_value);
    if (!intern) {
        return;
    }

    ZVAL_OBJ_COPY(&intern->zmetadata, zmetadata);
    intern->items = items;
    intern->item_cnt = item_cnt;
    intern->item_size = item_size;
    intern->ctor = ctor;
}
