/*
+----------------------------------------------------------------------+
  | php-rdkafka                                                          |
  +----------------------------------------------------------------------+
  | Copyright (c) 2025 Arnaud Le Blanc                                   |
  +----------------------------------------------------------------------+
  | This source file is subject to version 3.01 of the PHP license,      |
  | that is bundled with this package in the file LICENSE, and is        |
  | available through the world-wide-web at the following url:           |
  | http://www.php.net/license/3_01.txt                                  |
  | If you did not receive a copy of the PHP license and are unable to   |
  | obtain it through the world-wide-web, please send a note to          |
  | license@php.net so we can mail you a copy immediately.               |
  +----------------------------------------------------------------------+
  | Author: Martin Fris <rasta@lj.sk>                                    |
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
#include "ext/spl/spl_exceptions.h"
#include "conf.h"
#include "topic_partition.h"
#include "topic.h"
#include "message.h"
#include "metadata.h"
#if PHP_VERSION_ID < 80000
#include "kafka_consumer_legacy_arginfo.h"
#else
#include "kafka_consumer_arginfo.h"
#endif

void oauthbearer_set_token(
    rd_kafka_t *rk,
    const char *token_value,
    zend_long lifetime_ms,
    const char *principal_name,
    const HashTable *extensions_hash
) {
    char errstr[512];
    rd_kafka_resp_err_t ret = 0;

    errstr[0] = '\0';

    int extensions_size = 0;
    char **extensions = NULL;

    if (extensions_hash != NULL) {
        extensions_size = zend_hash_num_elements(extensions_hash) * 2;
        extensions = safe_emalloc((extensions_size * 2), sizeof(char *), 0);

        int pos = 0;
        zend_ulong num_key;
        zend_string *extension_key_str;
        zval *extension_zval;
        ZEND_HASH_FOREACH_KEY_VAL((HashTable*)extensions_hash, num_key, extension_key_str, extension_zval) {
            if (!extension_key_str) {
                extension_key_str = zend_long_to_str(num_key);
                extensions[pos++] = estrdup(ZSTR_VAL(extension_key_str));
                zend_string_release(extension_key_str);
            } else {
                extensions[pos++] = estrdup(ZSTR_VAL(extension_key_str));
            }

            zend_string *tmp_extension_val_str;
            zend_string *extension_val_str = zval_get_tmp_string(extension_zval, &tmp_extension_val_str);
            extensions[pos++] = estrdup(ZSTR_VAL(extension_val_str));
            if (tmp_extension_val_str) {
                zend_string_release(tmp_extension_val_str);
            }
        } ZEND_HASH_FOREACH_END();
    }

    ret = rd_kafka_oauthbearer_set_token(
        rk,
        token_value,
        lifetime_ms,
        principal_name,
        (const char **)extensions,
        extensions_size,
        errstr,
        sizeof(errstr));

    if (extensions != NULL) {
        for (int i = 0; i < extensions_size; i++) {
            efree(extensions[i]);
        }
        efree(extensions);
    }

    switch (ret) {
        case RD_KAFKA_RESP_ERR__INVALID_ARG:
            zend_throw_exception(ce_kafka_exception, errstr, RD_KAFKA_RESP_ERR__INVALID_ARG);
            return;
        case RD_KAFKA_RESP_ERR__NOT_IMPLEMENTED:
            zend_throw_exception(ce_kafka_exception, errstr, RD_KAFKA_RESP_ERR__NOT_IMPLEMENTED);
            return;
        case RD_KAFKA_RESP_ERR__STATE:
            zend_throw_exception(ce_kafka_exception, errstr, RD_KAFKA_RESP_ERR__STATE);
            return;
        case RD_KAFKA_RESP_ERR_NO_ERROR:
            break;
        default:
            return;
    }
}

void oauthbearer_set_token_failure(rd_kafka_t *rk, const char *errstr) {
    rd_kafka_resp_err_t ret = rd_kafka_oauthbearer_set_token_failure(rk, errstr);

    switch (ret) {
        case RD_KAFKA_RESP_ERR__INVALID_ARG:
            zend_throw_exception(ce_kafka_exception, NULL, RD_KAFKA_RESP_ERR__INVALID_ARG);
        return;
        case RD_KAFKA_RESP_ERR__STATE:
            zend_throw_exception(ce_kafka_exception, NULL, RD_KAFKA_RESP_ERR__STATE);
        return;
        case RD_KAFKA_RESP_ERR_NO_ERROR:
            break;
        default:
            return;
    }
}

int64_t zval_to_int64(zval *zval, const char *errstr) {
    int64_t converted;

    switch (Z_TYPE_P(zval)) {
        case IS_LONG:
            return (int64_t) Z_LVAL_P(zval);
        break;
        case IS_DOUBLE:
            return (int64_t) Z_DVAL_P(zval);
        break;
        case IS_STRING:;
           char *str = Z_STRVAL_P(zval);
           char *end;
           converted = (int64_t) strtoll(str, &end, 10);
           if (end != str + Z_STRLEN_P(zval)) {
               zend_throw_exception(spl_ce_InvalidArgumentException, errstr, 0);
               return 0;
           }
           return converted;
        break;
        EMPTY_SWITCH_DEFAULT_CASE();
    }
}
