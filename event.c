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
*/

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "php.h"
#include "php_rdkafka.h"
#include "php_rdkafka_priv.h"
#include "librdkafka/rdkafka.h"
#include "Zend/zend_exceptions.h"
#include "event.h"
#include "event_arginfo.h"

zend_class_entry *ce_kafka_event;

static zend_object_handlers handlers;

static void kafka_event_free(zend_object *object) /* {{{ */
{
    kafka_event_object *intern = php_kafka_from_obj(kafka_event_object, object);

    if (intern->rkev) {
        rd_kafka_event_destroy(intern->rkev);
        intern->rkev = NULL;
    }

    zend_object_std_dtor(&intern->std);
}
/* }}} */

static zend_object *kafka_event_new_obj(zend_class_entry *class_type) /* {{{ */
{
    kafka_event_object *intern;

    intern = zend_object_alloc(sizeof(*intern), class_type);
    zend_object_std_init(&intern->std, class_type);
    object_properties_init(&intern->std, class_type);

    intern->std.handlers = &handlers;

    return &intern->std;
}
/* }}} */

static kafka_event_object *get_kafka_event_object(zval *zev) /* {{{ */
{
    kafka_event_object *intern = Z_RDKAFKA_P(kafka_event_object, zev);

    if (!intern->rkev) {
        zend_throw_exception_ex(NULL, 0, "RdKafka\\Event has no underlying librdkafka event");
        return NULL;
    }

    return intern;
}
/* }}} */

void kafka_event_new(zval *return_value, rd_kafka_event_t *rkev) /* {{{ */
{
    kafka_event_object *intern;

    if (object_init_ex(return_value, ce_kafka_event) != SUCCESS) {
        return;
    }

    intern = Z_RDKAFKA_P(kafka_event_object, return_value);
    intern->rkev = rkev;
}
/* }}} */

/* {{{ proto int RdKafka\Event::getType()
   Returns the event type (one of the RD_KAFKA_EVENT_* constants). */
PHP_METHOD(RdKafka_Event, getType)
{
    kafka_event_object *intern;

    if (zend_parse_parameters_none() == FAILURE) {
        return;
    }

    intern = get_kafka_event_object(getThis());
    if (!intern) {
        return;
    }

    RETURN_LONG(rd_kafka_event_type(intern->rkev));
}
/* }}} */

/* {{{ proto string RdKafka\Event::getName()
   Returns the human-readable event type name. */
PHP_METHOD(RdKafka_Event, getName)
{
    kafka_event_object *intern;

    if (zend_parse_parameters_none() == FAILURE) {
        return;
    }

    intern = get_kafka_event_object(getThis());
    if (!intern) {
        return;
    }

    RETURN_STRING(rd_kafka_event_name(intern->rkev));
}
/* }}} */

/* {{{ proto int RdKafka\Event::getError()
   Returns the librdkafka error code associated with this event (RD_KAFKA_RESP_ERR_NO_ERROR if none). */
PHP_METHOD(RdKafka_Event, getError)
{
    kafka_event_object *intern;

    if (zend_parse_parameters_none() == FAILURE) {
        return;
    }

    intern = get_kafka_event_object(getThis());
    if (!intern) {
        return;
    }

    RETURN_LONG(rd_kafka_event_error(intern->rkev));
}
/* }}} */

/* {{{ proto ?string RdKafka\Event::getErrorString()
   Returns a human-readable error string, or null if there was no error. */
PHP_METHOD(RdKafka_Event, getErrorString)
{
    kafka_event_object *intern;
    const char *errstr;

    if (zend_parse_parameters_none() == FAILURE) {
        return;
    }

    intern = get_kafka_event_object(getThis());
    if (!intern) {
        return;
    }

    errstr = rd_kafka_event_error_string(intern->rkev);
    if (errstr == NULL) {
        RETURN_NULL();
    }

    RETURN_STRING(errstr);
}
/* }}} */

void kafka_event_minit(INIT_FUNC_ARGS) /* {{{ */
{
    handlers = kafka_default_object_handlers;
    handlers.free_obj = kafka_event_free;
    handlers.offset = XtOffsetOf(kafka_event_object, std);

    ce_kafka_event = register_class_RdKafka_Event();
    ce_kafka_event->create_object = kafka_event_new_obj;

    REGISTER_LONG_CONSTANT("RD_KAFKA_EVENT_NONE", RD_KAFKA_EVENT_NONE, CONST_CS | CONST_PERSISTENT);
    REGISTER_LONG_CONSTANT("RD_KAFKA_EVENT_DR", RD_KAFKA_EVENT_DR, CONST_CS | CONST_PERSISTENT);
    REGISTER_LONG_CONSTANT("RD_KAFKA_EVENT_FETCH", RD_KAFKA_EVENT_FETCH, CONST_CS | CONST_PERSISTENT);
    REGISTER_LONG_CONSTANT("RD_KAFKA_EVENT_LOG", RD_KAFKA_EVENT_LOG, CONST_CS | CONST_PERSISTENT);
    REGISTER_LONG_CONSTANT("RD_KAFKA_EVENT_ERROR", RD_KAFKA_EVENT_ERROR, CONST_CS | CONST_PERSISTENT);
    REGISTER_LONG_CONSTANT("RD_KAFKA_EVENT_REBALANCE", RD_KAFKA_EVENT_REBALANCE, CONST_CS | CONST_PERSISTENT);
    REGISTER_LONG_CONSTANT("RD_KAFKA_EVENT_OFFSET_COMMIT", RD_KAFKA_EVENT_OFFSET_COMMIT, CONST_CS | CONST_PERSISTENT);
    REGISTER_LONG_CONSTANT("RD_KAFKA_EVENT_STATS", RD_KAFKA_EVENT_STATS, CONST_CS | CONST_PERSISTENT);
    REGISTER_LONG_CONSTANT("RD_KAFKA_EVENT_CREATETOPICS_RESULT", RD_KAFKA_EVENT_CREATETOPICS_RESULT, CONST_CS | CONST_PERSISTENT);
    REGISTER_LONG_CONSTANT("RD_KAFKA_EVENT_DELETETOPICS_RESULT", RD_KAFKA_EVENT_DELETETOPICS_RESULT, CONST_CS | CONST_PERSISTENT);
    REGISTER_LONG_CONSTANT("RD_KAFKA_EVENT_CREATEPARTITIONS_RESULT", RD_KAFKA_EVENT_CREATEPARTITIONS_RESULT, CONST_CS | CONST_PERSISTENT);
    REGISTER_LONG_CONSTANT("RD_KAFKA_EVENT_DELETERECORDS_RESULT", RD_KAFKA_EVENT_DELETERECORDS_RESULT, CONST_CS | CONST_PERSISTENT);
#ifdef HAS_RD_KAFKA_DESCRIBE_TOPICS
    REGISTER_LONG_CONSTANT("RD_KAFKA_EVENT_DESCRIBETOPICS_RESULT", RD_KAFKA_EVENT_DESCRIBETOPICS_RESULT, CONST_CS | CONST_PERSISTENT);
#endif
}
/* }}} */
