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
#ifdef HAS_RD_KAFKA_TRANSACTIONS
#include "php_rdkafka.h"
#include "php_rdkafka_priv.h"
#include "Zend/zend_interfaces.h"
#include "Zend/zend_exceptions.h"
#include "kafka_error_exception.h"
#if PHP_VERSION_ID < 80000
#include "kafka_error_exception_legacy_arginfo.h"
#else
#include "kafka_error_exception_arginfo.h"
#endif

zend_class_entry * ce_kafka_error;

void create_kafka_error(zval *return_value, const rd_kafka_error_t *error) /* {{{ */
{
    object_init_ex(return_value, ce_kafka_error);

    char message[1024];
    snprintf(message, sizeof(message), "%s (RD_KAFKA_RESP_ERR_%s)", rd_kafka_error_string(error), rd_kafka_error_name(error));
    zend_update_property_string(ce_kafka_error, Z_RDKAFKA_PROP_OBJ(return_value), ZEND_STRL("message"), message);

    zend_update_property_long(ce_kafka_error, Z_RDKAFKA_PROP_OBJ(return_value), ZEND_STRL("code"), rd_kafka_error_code(error));
    zend_update_property_string(ce_kafka_error, Z_RDKAFKA_PROP_OBJ(return_value), ZEND_STRL("error_string"), rd_kafka_error_string(error));
    zend_update_property_bool(ce_kafka_error, Z_RDKAFKA_PROP_OBJ(return_value), ZEND_STRL("isFatal"), rd_kafka_error_is_fatal(error));
    zend_update_property_bool(ce_kafka_error, Z_RDKAFKA_PROP_OBJ(return_value), ZEND_STRL("isRetriable"), rd_kafka_error_is_retriable(error));
    zend_update_property_bool(ce_kafka_error, Z_RDKAFKA_PROP_OBJ(return_value), ZEND_STRL("transactionRequiresAbort"), rd_kafka_error_txn_requires_abort(error));

    Z_ADDREF_P(return_value);
}
/* }}} */

/* {{{ proto RdKafka\KafkaErrorException::__construct(string $message, int $code[, string $error_string, bool $isFatal, bool $isRetriable, bool $transactionRequiresAbort]) */
PHP_METHOD(RdKafka_KafkaErrorException, __construct)
{
    char *message, *error_string = "";
    size_t message_length = 0, error_string_length = 0;
    zend_bool isFatal = 0, isRetriable = 0, transactionRequiresAbort = 0;
    zend_long code = 0;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "sl|sbbb", &message, &message_length, &code, &error_string, &error_string_length, &isFatal, &isRetriable, &transactionRequiresAbort) == FAILURE) {
        return;
    }

    zend_update_property_string(ce_kafka_error, Z_RDKAFKA_PROP_OBJ(getThis()), ZEND_STRL("message"), message);
    zend_update_property_long(ce_kafka_error, Z_RDKAFKA_PROP_OBJ(getThis()), ZEND_STRL("code"), code);
    zend_update_property_string(ce_kafka_error, Z_RDKAFKA_PROP_OBJ(getThis()), ZEND_STRL("error_string"), error_string);
    zend_update_property_bool(ce_kafka_error, Z_RDKAFKA_PROP_OBJ(getThis()), ZEND_STRL("isFatal"), isFatal);
    zend_update_property_bool(ce_kafka_error, Z_RDKAFKA_PROP_OBJ(getThis()), ZEND_STRL("isRetriable"), isRetriable);
    zend_update_property_bool(ce_kafka_error, Z_RDKAFKA_PROP_OBJ(getThis()), ZEND_STRL("transactionRequiresAbort"), transactionRequiresAbort);
}
/* }}} */

/* {{{ proto void RdKafka\KafkaErrorException::getErrorString()
    Get name of error */
PHP_METHOD(RdKafka_KafkaErrorException, getErrorString)
{
    zval *res;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "") == FAILURE) {
        return;
    }

    res = rdkafka_read_property(ce_kafka_error, Z_RDKAFKA_PROP_OBJ(getThis()), ZEND_STRL("error_string"), 0);

    if (!res || Z_TYPE_P(res) != IS_STRING) {
        return;
    }

    ZVAL_DEREF(res);
    ZVAL_COPY(return_value, res);
}
/* }}} */


/* {{{ proto void RdKafka\KafkaErrorException::isFatal()
    Return true if error is fatal */
PHP_METHOD(RdKafka_KafkaErrorException, isFatal)
{
    zval *res;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "") == FAILURE) {
        return;
    }

    res = rdkafka_read_property(ce_kafka_error, Z_RDKAFKA_PROP_OBJ(getThis()), ZEND_STRL("isFatal"), 0);

    if (!res || (Z_TYPE_P(res) != IS_TRUE && Z_TYPE_P(res) != IS_FALSE)) {
        return;
    }

    ZVAL_DEREF(res);
    ZVAL_COPY(return_value, res);
}
/* }}} */

/* {{{ proto void RdKafka\KafkaErrorException::isRetriable()
    Return true if error is fatal */
PHP_METHOD(RdKafka_KafkaErrorException, isRetriable)
{
    zval *res;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "") == FAILURE) {
        return;
    }

    res = rdkafka_read_property(ce_kafka_error, Z_RDKAFKA_PROP_OBJ(getThis()), ZEND_STRL("isRetriable"), 0);

    if (!res || (Z_TYPE_P(res) != IS_TRUE && Z_TYPE_P(res) != IS_FALSE)) {
        return;
    }

    ZVAL_DEREF(res);
    ZVAL_COPY(return_value, res);
}
/* }}} */

/* {{{ proto void RdKafka\KafkaErrorException::transactionRequiresAbort()
    Return true if error is fatal */
PHP_METHOD(RdKafka_KafkaErrorException, transactionRequiresAbort)
{
    zval *res;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "") == FAILURE) {
        return;
    }

    res = rdkafka_read_property(ce_kafka_error, Z_RDKAFKA_PROP_OBJ(getThis()), ZEND_STRL("transactionRequiresAbort"), 0);

    if (!res || (Z_TYPE_P(res) != IS_TRUE && Z_TYPE_P(res) != IS_FALSE)) {
        return;
    }

    ZVAL_DEREF(res);
    ZVAL_COPY(return_value, res);
}
/* }}} */

void kafka_error_minit() /* {{{ */
{
    ce_kafka_error = register_class_RdKafka_KafkaErrorException(ce_kafka_exception);
} /* }}} */
#endif

