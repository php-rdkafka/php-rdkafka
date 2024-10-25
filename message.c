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
#include "topic.h"
#include "message.h"
#include "message_arginfo.h"

zend_class_entry * ce_kafka_message;

void kafka_message_new(zval *return_value, const rd_kafka_message_t *message, zend_string *msg_opaque)
{
    object_init_ex(return_value, ce_kafka_message);

    rd_kafka_timestamp_type_t tstype;
    int64_t timestamp;

    timestamp = rd_kafka_message_timestamp(message, &tstype);

    zval headers_array;
    rd_kafka_headers_t *message_headers = NULL;
    rd_kafka_resp_err_t header_response;
    const char *header_name = NULL;
    const void *header_value = NULL;
    size_t header_size = 0;
    size_t i;

    zend_update_property_long(NULL, Z_OBJ_P(return_value), ZEND_STRL("err"), message->err);

    if (message->rkt) {
        zend_update_property_string(NULL, Z_OBJ_P(return_value), ZEND_STRL("topic_name"), rd_kafka_topic_name(message->rkt));
    }
    zend_update_property_long(NULL, Z_OBJ_P(return_value), ZEND_STRL("partition"), message->partition);
    if (message->payload) {
        zend_update_property_long(NULL, Z_OBJ_P(return_value), ZEND_STRL("timestamp"), timestamp);
        zend_update_property_stringl(NULL, Z_OBJ_P(return_value), ZEND_STRL("payload"), message->payload, message->len);
        zend_update_property_long(NULL, Z_OBJ_P(return_value), ZEND_STRL("len"), message->len);
    }
    if (message->key) {
        zend_update_property_stringl(NULL, Z_OBJ_P(return_value), ZEND_STRL("key"), message->key, message->key_len);
    }
    zend_update_property_long(NULL, Z_OBJ_P(return_value), ZEND_STRL("offset"), message->offset);

    array_init(&headers_array);
    if (message->err == RD_KAFKA_RESP_ERR_NO_ERROR) {
        rd_kafka_message_headers(message, &message_headers);
        if (message_headers != NULL) {
            for (i = 0; i < rd_kafka_header_cnt(message_headers); i++) {
                header_response = rd_kafka_header_get_all(message_headers, i, &header_name, &header_value, &header_size);
                if (header_response != RD_KAFKA_RESP_ERR_NO_ERROR) {
                    break;
                }
                add_assoc_stringl(&headers_array, header_name, (const char*)header_value, header_size);
            }
        }
    }
    zend_update_property(NULL, Z_OBJ_P(return_value), ZEND_STRL("headers"), &headers_array);
    zval_ptr_dtor(&headers_array);

    if (msg_opaque != NULL) {
        zend_update_property_str(NULL, Z_OBJ_P(return_value), ZEND_STRL("opaque"), msg_opaque);
    }
}

void kafka_message_list_to_array(zval *return_value, rd_kafka_message_t **messages, long size) /* {{{ */
{
    rd_kafka_message_t *msg;
    zval zmsg;
    int i;

    array_init_size(return_value, size);

    for (i = 0; i < size; i++) {
        msg = messages[i];
        ZVAL_NULL(&zmsg);
        kafka_message_new(&zmsg, msg, NULL);
        add_next_index_zval(return_value, &zmsg);
    }
} /* }}} */

/* {{{ proto string RdKafka\Message::errstr()
 *  Returns the error string for an errored KrKafka\Message or NULL if there was no error.
 */
PHP_METHOD(RdKafka_Message, errstr)
{
    zval *zerr;
    zval *zpayload;
    const char *errstr;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "") == FAILURE) {
        return;
    }

    zerr = rdkafka_read_property(NULL, Z_OBJ_P(getThis()), ZEND_STRL("err"), 0);

    if (!zerr || Z_TYPE_P(zerr) != IS_LONG) {
        return;
    }

    errstr = rd_kafka_err2str(Z_LVAL_P(zerr));

    if (errstr) {
        RETURN_STRING(errstr);
    }

    zpayload = rdkafka_read_property(NULL, Z_OBJ_P(getThis()), ZEND_STRL("payload"), 0);

    if (zpayload && Z_TYPE_P(zpayload) == IS_STRING) {
        RETURN_ZVAL(zpayload, 1, 0);
    }
}
/* }}} */

void kafka_message_minit(INIT_FUNC_ARGS) { /* {{{ */
    ce_kafka_message = register_class_RdKafka_Message();
} /* }}} */
