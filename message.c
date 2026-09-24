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
    zval header_pairs;
    zval header_pair;
    rd_kafka_headers_t *message_headers = NULL;
    rd_kafka_resp_err_t header_response;
    const char *header_name = NULL;
    const void *header_value = NULL;
    size_t header_size = 0;
    size_t header_count = 0;
    zend_bool has_null_header = 0;
    size_t i;

    zend_update_property_long(NULL, Z_OBJ_P(return_value), ZEND_STRL("err"), message->err);

    if (message->rkt) {
        zend_update_property_string(NULL, Z_OBJ_P(return_value), ZEND_STRL("topic_name"), rd_kafka_topic_name(message->rkt));
    }
    zend_update_property_long(NULL, Z_OBJ_P(return_value), ZEND_STRL("partition"), message->partition);
    /* Error messages without payloads have historically left timestamp null. */
    if (message->err == RD_KAFKA_RESP_ERR_NO_ERROR || message->payload) {
        zend_update_property_long(NULL, Z_OBJ_P(return_value), ZEND_STRL("timestamp"), timestamp);
    }
    if (message->payload) {
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
                if (header_value == NULL) {
                    has_null_header = 1;
                    add_assoc_str(&headers_array, header_name, ZSTR_EMPTY_ALLOC());
                } else {
                    add_assoc_stringl(&headers_array, header_name, (const char*)header_value, header_size);
                }
            }
            header_count = i;
        }
    }
    zend_update_property(NULL, Z_OBJ_P(return_value), ZEND_STRL("headers"), &headers_array);

    /* getHeaderPairs() derives pairs from the map when it has every header.
     * Repeated names and null values need their own copy, made while the
     * native message is still available. */
    if (has_null_header || zend_hash_num_elements(Z_ARRVAL(headers_array)) != header_count) {
        array_init_size(&header_pairs, header_count);
        for (i = 0; i < header_count; i++) {
            rd_kafka_header_get_all(message_headers, i, &header_name, &header_value, &header_size);
            array_init_size(&header_pair, 2);
            add_next_index_string(&header_pair, header_name);
            if (header_value == NULL) {
                add_next_index_null(&header_pair);
            } else {
                add_next_index_stringl(&header_pair, (const char*)header_value, header_size);
            }
            add_next_index_zval(&header_pairs, &header_pair);
        }
        zend_update_property(ce_kafka_message, Z_OBJ_P(return_value), ZEND_STRL("native_headers"), &header_pairs);
        zval_ptr_dtor(&header_pairs);
    } else {
        zend_update_property(ce_kafka_message, Z_OBJ_P(return_value), ZEND_STRL("native_headers"), &headers_array);
    }
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
 *  Returns the error as a string.
 */
PHP_METHOD(RdKafka_Message, errstr)
{
    zval zerr;
    zval zpayload;
    const char *errstr;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "") == FAILURE) {
        return;
    }

    rdkafka_read_property(NULL, Z_OBJ_P(getThis()), ZEND_STRL("err"), 0, &zerr);

    if (Z_TYPE(zerr) != IS_LONG) {
        zval_ptr_dtor(&zerr);
        return;
    }

    errstr = rd_kafka_err2str(Z_LVAL(zerr));

    if (errstr) {
        RETURN_STRING(errstr);
    }

    rdkafka_read_property(NULL, Z_OBJ_P(getThis()), ZEND_STRL("payload"), 0, &zpayload);

    if (Z_TYPE(zpayload) == IS_STRING) {
        RETURN_COPY_VALUE(&zpayload);
    }

    zval_ptr_dtor(&zpayload);
}
/* }}} */

/* {{{ proto array RdKafka\Message::getHeaderPairs()
 *  Returns the message headers as [name, value] pairs, in their original order.
 */
PHP_METHOD(RdKafka_Message, getHeaderPairs)
{
    zval headers;
    zval *header_value;
    zval header_pair;
    zend_string *header_key;
    zend_string *header_value_string;
    zend_ulong header_index;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "") == FAILURE) {
        return;
    }

    rdkafka_read_property(ce_kafka_message, Z_OBJ_P(getThis()), ZEND_STRL("native_headers"), 1, &headers);

    if (Z_TYPE(headers) == IS_ARRAY) {
        ZEND_HASH_FOREACH_VAL(Z_ARRVAL(headers), header_value) {
            if (Z_TYPE_P(header_value) == IS_ARRAY) {
                RETURN_COPY_VALUE(&headers);
            }
            break;
        } ZEND_HASH_FOREACH_END();
    } else {
        zval_ptr_dtor(&headers);
        if (EG(exception)) {
            return;
        }

        /* Messages that were not created by the extension, or were
         * serialized before this property existed, only have the map. */
        rdkafka_read_property(NULL, Z_OBJ_P(getThis()), ZEND_STRL("headers"), 1, &headers);

        if (Z_TYPE(headers) != IS_ARRAY) {
            zval_ptr_dtor(&headers);
            if (!EG(exception)) {
                RETURN_EMPTY_ARRAY();
            }
            return;
        }
    }

    array_init_size(return_value, zend_hash_num_elements(Z_ARRVAL(headers)));

    ZEND_HASH_FOREACH_KEY_VAL(Z_ARRVAL(headers), header_index, header_key, header_value) {
        header_value_string = zval_try_get_string(header_value);

        if (header_value_string == NULL) {
            zval_ptr_dtor(&headers);
            zval_ptr_dtor(return_value);
            RETURN_NULL();
        }

        array_init_size(&header_pair, 2);
        add_next_index_str(&header_pair, header_key != NULL ? zend_string_copy(header_key) : zend_long_to_str((zend_long) header_index));
        add_next_index_str(&header_pair, header_value_string);
        add_next_index_zval(return_value, &header_pair);
    } ZEND_HASH_FOREACH_END();

    zval_ptr_dtor(&headers);
}
/* }}} */

void kafka_message_minit(INIT_FUNC_ARGS) { /* {{{ */
    ce_kafka_message = register_class_RdKafka_Message();
} /* }}} */
