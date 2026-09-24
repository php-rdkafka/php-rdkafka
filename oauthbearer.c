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

#include <errno.h>
#include "php.h"
#include "php_rdkafka.h"
#include "php_rdkafka_priv.h"
#include "Zend/zend_exceptions.h"
#include "ext/spl/spl_exceptions.h"

void oauthbearer_extensions_free(char **extensions, int extensions_size) {
    if (extensions == NULL) {
        return;
    }

    for (int i = 0; i < extensions_size; i++) {
        efree(extensions[i]);
    }
    efree(extensions);
}

char **oauthbearer_extensions_new(const HashTable *extensions_hash, int *extensions_size) {
    char **extensions;
    int pos = 0;
    zend_ulong num_key;
    zend_string *extension_key_str;
    zval *extension_zval;

    *extensions_size = 0;

    if (extensions_hash == NULL) {
        return NULL;
    }

    extensions = safe_emalloc(zend_hash_num_elements(extensions_hash) * 2, sizeof(char *), 0);

    ZEND_HASH_FOREACH_KEY_VAL((HashTable*)extensions_hash, num_key, extension_key_str, extension_zval) {
        if (!extension_key_str) {
            extension_key_str = zend_long_to_str(num_key);
            extensions[pos++] = estrdup(ZSTR_VAL(extension_key_str));
            zend_string_release(extension_key_str);
        } else if (CHECK_NULL_PATH(ZSTR_VAL(extension_key_str), ZSTR_LEN(extension_key_str))) {
            zend_argument_value_error(4, "must not contain any null bytes");
            break;
        } else {
            extensions[pos++] = estrdup(ZSTR_VAL(extension_key_str));
        }

        zend_string *tmp_extension_val_str;
        zend_string *extension_val_str = zval_try_get_tmp_string(extension_zval, &tmp_extension_val_str);
        if (!extension_val_str) {
            break;
        }
        if (CHECK_NULL_PATH(ZSTR_VAL(extension_val_str), ZSTR_LEN(extension_val_str))) {
            zend_tmp_string_release(tmp_extension_val_str);
            zend_argument_value_error(4, "must not contain any null bytes");
            break;
        }
        extensions[pos++] = estrdup(ZSTR_VAL(extension_val_str));
        zend_tmp_string_release(tmp_extension_val_str);
    } ZEND_HASH_FOREACH_END();

    if (EG(exception)) {
        oauthbearer_extensions_free(extensions, pos);
        return NULL;
    }

    *extensions_size = pos;

    return extensions;
}

void oauthbearer_set_token(
    rd_kafka_t *rk,
    const char *token_value,
    int64_t lifetime_ms,
    const char *principal_name,
    char **extensions,
    int extensions_size
) {
    char errstr[512];
    rd_kafka_resp_err_t ret = 0;

    errstr[0] = '\0';

    ret = rd_kafka_oauthbearer_set_token(
        rk,
        token_value,
        lifetime_ms,
        principal_name,
        (const char **)extensions,
        extensions_size,
        errstr,
        sizeof(errstr));

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

int64_t zval_to_int64(zval *zval, uint32_t arg_num) {
    int64_t converted;

    switch (Z_TYPE_P(zval)) {
        case IS_LONG:
            return (int64_t) Z_LVAL_P(zval);
        break;
        case IS_DOUBLE:
            /* < as (double) INT64_MAX is outside the int64 range */
            if (Z_DVAL_P(zval) >= (double) INT64_MIN && Z_DVAL_P(zval) < (double) INT64_MAX) {
                return (int64_t) Z_DVAL_P(zval);
            }
        break;
        case IS_STRING:;
           char *str = Z_STRVAL_P(zval);
           char *end;
           errno = 0;
           converted = (int64_t) strtoll(str, &end, 10);
           if (end != str && end == str + Z_STRLEN_P(zval) && errno != ERANGE) {
               return converted;
           }
        break;
        default:
            zend_argument_type_error(arg_num, "must be of type int|float|string, %s given", zend_zval_type_name(zval));
            return 0;
    }

    zend_argument_error(spl_ce_InvalidArgumentException, arg_num, "must be a valid integer");
    return 0;
}
