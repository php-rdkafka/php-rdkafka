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

#ifndef PHP_RDKAFKA_PRIV_H
#define PHP_RDKAFKA_PRIV_H

#ifndef Z_PARAM_STRING_OR_NULL
#define Z_PARAM_STRING_OR_NULL(dest, dest_len) \
    Z_PARAM_STRING_EX(dest, dest_len, 1, 0)
#endif

#ifndef Z_PARAM_STR_OR_NULL
#define Z_PARAM_STR_OR_NULL(dest) \
    Z_PARAM_STR_EX(dest, 1, 0)
#endif

#ifndef Z_PARAM_ARRAY_HT_OR_NULL
#define Z_PARAM_ARRAY_HT_OR_NULL(dest) \
    Z_PARAM_ARRAY_HT_EX(dest, 1, 0)
#endif

#ifndef Z_PARAM_LONG_OR_NULL
#define Z_PARAM_LONG_OR_NULL(dest, is_null) \
    Z_PARAM_LONG_EX(dest, is_null, 1, 0)
#endif

#ifndef ZEND_BEGIN_ARG_WITH_TENTATIVE_RETURN_OBJ_INFO_EX
#define ZEND_BEGIN_ARG_WITH_TENTATIVE_RETURN_OBJ_INFO_EX(name, return_reference, required_num_args, type, allow_null) \
    ZEND_BEGIN_ARG_INFO_EX(name, 0, return_reference, required_num_args)
#endif

#ifndef ZEND_BEGIN_ARG_WITH_TENTATIVE_RETURN_TYPE_INFO_EX
#define ZEND_BEGIN_ARG_WITH_TENTATIVE_RETURN_TYPE_INFO_EX(name, return_reference, required_num_args, type, allow_null) \
    ZEND_BEGIN_ARG_INFO_EX(name, 0, return_reference, required_num_args)
#endif

#if PHP_MAJOR_VERSION >= 8

#define Z_RDKAFKA_OBJ zend_object

#define Z_RDKAFKA_OBJ_COPY(z, o) ZVAL_OBJ_COPY(z, o)

#define Z_RDKAFKA_PROP_OBJ(object) Z_OBJ_P(object)

#define rdkafka_get_debug_object(type, object) php_kafka_from_obj(type, object)

#else // PHP 7

#define Z_RDKAFKA_OBJ zval

#define Z_RDKAFKA_OBJ_COPY(z, o) ZVAL_ZVAL(z, o, 1, 0)

#define Z_RDKAFKA_PROP_OBJ(object) object

#define rdkafka_get_debug_object(type, object) php_kafka_from_obj(type, Z_OBJ_P(object))

#if PHP_MINOR_VERSION < 3
/* Allocates object type and zeros it, but not the properties.
 * Properties MUST be initialized using object_properties_init(). */
static zend_always_inline void *zend_object_alloc(size_t obj_size, zend_class_entry *ce) {
    void *obj = emalloc(obj_size + zend_object_properties_size(ce));
    /* Subtraction of sizeof(zval) is necessary, because zend_object_properties_size() may be
     * -sizeof(zval), if the object has no properties. */
    memset(obj, 0, obj_size - sizeof(zval));
    return obj;
}

static zend_always_inline zend_string *zval_get_tmp_string(zval *op, zend_string **tmp) {
	if (EXPECTED(Z_TYPE_P(op) == IS_STRING)) {
		*tmp = NULL;
		return Z_STR_P(op);
	} else {
		return *tmp = _zval_get_string_func(op);
	}
}
static zend_always_inline void zend_tmp_string_release(zend_string *tmp) {
	if (UNEXPECTED(tmp)) {
		zend_string_release(tmp);
	}
}
#endif // PHP_MINOR_VERSION < 3

#endif // PHP 7

#define Z_RDKAFKA_P(php_kafka_type, zobject) php_kafka_from_obj(php_kafka_type, Z_OBJ_P(zobject))

#define php_kafka_from_obj(php_kafka_type, object) \
    ((php_kafka_type*)((char *)(object) - XtOffsetOf(php_kafka_type, std)))

static inline void rdkafka_call_function(zend_fcall_info *fci, zend_fcall_info_cache *fci_cache, zval *retval, uint32_t param_count, zval params[])
{
    int local_retval;
    zval local_retval_zv;

    if (retval) {
        local_retval = 0;
    } else {
        local_retval = 1;
        retval = &local_retval_zv;
    }

    fci->retval = retval;
    fci->params = params;
    fci->param_count = param_count;

    zend_call_function(fci, fci_cache);

    if (local_retval) {
        zval_ptr_dtor(retval);
    }
}

static inline zval *rdkafka_read_property(zend_class_entry *scope, Z_RDKAFKA_OBJ *object, const char *name, size_t name_length, zend_bool silent)
{
    zval rv;
    return zend_read_property(scope, object, name, name_length, silent, &rv);
}


static inline char *rdkafka_hash_get_current_key_ex(HashTable *ht, HashPosition *pos)
{
    zend_string* key;
    zend_ulong index;

    if (zend_hash_get_current_key_ex(ht, &key, &index, pos) == HASH_KEY_IS_STRING) {
        return key->val;
    }

    return NULL;
}

kafka_object * get_kafka_object(zval *zrk);
void add_consuming_toppar(kafka_object * intern, rd_kafka_topic_t * rkt, int32_t partition);
void del_consuming_toppar(kafka_object * intern, rd_kafka_topic_t * rkt, int32_t partition);
int is_consuming_toppar(kafka_object * intern, rd_kafka_topic_t * rkt, int32_t partition);

#endif /* PHP_RDKAFKA_PRIV_H */
