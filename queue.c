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
#include "ext/spl/spl_exceptions.h"
#include "Zend/zend_interfaces.h"
#include "Zend/zend_exceptions.h"
#include "topic.h"
#include "queue.h"
#include "message.h"
#include "queue_arginfo.h"

#ifndef PHP_WIN32
#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include "php_network.h"
#endif

zend_class_entry * ce_kafka_queue;

static zend_object_handlers handlers;

#ifndef PHP_WIN32
static void kafka_queue_io_event_disable(kafka_queue_object *intern) /* {{{ */
{
    if (intern->io_event_fd == -1) {
        return;
    }

    // Unregister first: librdkafka writes to the descriptor until then
    rd_kafka_queue_io_event_enable(intern->rkqu, -1, NULL, 0);
    close(intern->io_event_fd);
    intern->io_event_fd = -1;
}
/* }}} */
#endif

void kafka_queue_object_pre_free(kafka_queue_object **pp) /* {{{ */
{
    kafka_queue_object *intern = *pp;
    zval zrk;

#ifndef PHP_WIN32
    kafka_queue_io_event_disable(intern);
#endif

    rd_kafka_queue_destroy(intern->rkqu);
    intern->rkqu = NULL;
    intern->registry = NULL;

    if (intern->registry_key) {
        zend_string_release(intern->registry_key);
        intern->registry_key = NULL;
    }

    // Releasing the parent can destroy it along with its registry, so do it last
    ZVAL_COPY_VALUE(&zrk, &intern->zrk);
    ZVAL_UNDEF(&intern->zrk);
    zval_ptr_dtor(&zrk);
}
/* }}} */

static void kafka_queue_free(zend_object *object) /* {{{ */
{
    kafka_queue_object *intern = php_kafka_from_obj(kafka_queue_object, object);

    if (intern->registry) {
        if (intern->registry_key) {
            zend_hash_del(intern->registry, intern->registry_key);
        } else {
            zend_hash_index_del(intern->registry, (zend_ulong)intern);
        }
    }

    zend_object_std_dtor(&intern->std);
}
/* }}} */

static HashTable *kafka_queue_get_gc(zend_object *object, zval **table, int *n) /* {{{ */
{
    kafka_queue_object *intern = php_kafka_from_obj(kafka_queue_object, object);
    zend_get_gc_buffer *gc_buffer = zend_get_gc_buffer_create();

    zend_get_gc_buffer_add_zval(gc_buffer, &intern->zrk);
    zend_get_gc_buffer_use(gc_buffer, table, n);

    if (*n == 0) {
        return zend_std_get_gc(object, table, n);
    }

    if (object->properties == NULL && object->ce->default_properties_count == 0) {
        return NULL;
    }

    return zend_std_get_properties(object);
}
/* }}} */

static zend_object *kafka_queue_new(zend_class_entry *class_type) /* {{{ */
{
    zend_object* retval;
    kafka_queue_object *intern;

    intern = zend_object_alloc(sizeof(*intern), class_type);
    zend_object_std_init(&intern->std, class_type);
    object_properties_init(&intern->std, class_type);

#ifndef PHP_WIN32
    intern->io_event_fd = -1;
#endif

    retval = &intern->std;
    retval->handlers = &handlers;

    return retval;
}
/* }}} */

kafka_queue_object * get_kafka_queue_object(zval *zrkqu)
{
    kafka_queue_object *orkqu = Z_RDKAFKA_P(kafka_queue_object, zrkqu);

    if (!orkqu->rkqu) {
        zend_throw_exception_ex(NULL, 0, "RdKafka\\Queue is not initialized or its client has been closed");
        return NULL;
    }

    return orkqu;
}

/* Takes ownership of rkqu and registry_key. A NULL registry_key registers the
 * queue by its address, so each call returns a distinct queue. */
kafka_queue_object * kafka_queue_object_init(zval *return_value, zval *zrk, HashTable *registry, rd_kafka_queue_t *rkqu, zend_string *registry_key) /* {{{ */
{
    kafka_queue_object *intern;

    if (object_init_ex(return_value, ce_kafka_queue) != SUCCESS) {
        rd_kafka_queue_destroy(rkqu);
        if (registry_key) {
            zend_string_release(registry_key);
        }
        return NULL;
    }

    intern = Z_RDKAFKA_P(kafka_queue_object, return_value);
    intern->rkqu = rkqu;
    intern->registry = registry;
    intern->registry_key = registry_key;

    // The queue keeps its client alive so that it is released before the
    // client is destroyed. The client's registry only points back to the
    // queue, so it can invalidate it when the client is closed.
    ZVAL_COPY(&intern->zrk, zrk);

    if (registry_key) {
        zend_hash_add_new_ptr(registry, registry_key, intern);
    } else {
        zend_hash_index_add_new_ptr(registry, (zend_ulong)intern, intern);
    }

    return intern;
}
/* }}} */

/* {{{ proto RdKafka\Message RdKafka\Queue::consume(int timeout_ms)
   Consume a single message */
PHP_METHOD(RdKafka_Queue, consume)
{
    kafka_queue_object *intern;
    zend_long timeout_ms;
    rd_kafka_message_t *message;
    rd_kafka_resp_err_t err;

    if (zend_parse_parameters(ZEND_NUM_ARGS(), "l", &timeout_ms) == FAILURE) {
        return;
    }

    intern = get_kafka_queue_object(getThis());
    if (!intern) {
        return;
    }

    message = rd_kafka_consume_queue(intern->rkqu, timeout_ms);

    if (!message) {
        err = rd_kafka_last_error();
        // A callback exception interrupts consumption; let it propagate
        // instead of the interruption error
        if (err == RD_KAFKA_RESP_ERR__TIMED_OUT || EG(exception)) {
            return;
        }
        zend_throw_exception(ce_kafka_exception, rd_kafka_err2str(err), err);
        return;
    }

    kafka_message_new(return_value, message, NULL);

    rd_kafka_message_destroy(message);
}
/* }}} */

/* {{{ proto int RdKafka\Queue::getLength()
   Returns the number of events in the queue, including forwarded queues */
PHP_METHOD(RdKafka_Queue, getLength)
{
    kafka_queue_object *intern;

    if (zend_parse_parameters_none() == FAILURE) {
        return;
    }

    intern = get_kafka_queue_object(getThis());
    if (!intern) {
        return;
    }

    RETURN_LONG(rd_kafka_queue_length(intern->rkqu));
}
/* }}} */

#ifndef PHP_WIN32
/* {{{ proto void RdKafka\Queue::ioEventEnable(?resource $stream, string $payload = "\x01")
   Write $payload to $stream when the queue changes from empty to non-empty.
   Pass null to disable notifications. */
PHP_METHOD(RdKafka_Queue, ioEventEnable)
{
    kafka_queue_object *intern;
    zval *zstream;
    zend_string *payload = NULL;
    php_stream *stream;
    php_socket_t fd;
    struct stat st;
    int flags;
    int io_event_fd;

    ZEND_PARSE_PARAMETERS_START(1, 2)
        Z_PARAM_RESOURCE_OR_NULL(zstream)
        Z_PARAM_OPTIONAL
        Z_PARAM_STR(payload)
    ZEND_PARSE_PARAMETERS_END();

    intern = get_kafka_queue_object(getThis());
    if (!intern) {
        return;
    }

    if (!zstream) {
        kafka_queue_io_event_disable(intern);
        return;
    }

    if (payload && ZSTR_LEN(payload) == 0) {
        zend_throw_exception(spl_ce_InvalidArgumentException, "The notification payload must not be empty", 0);
        return;
    }

    php_stream_from_zval(stream, zstream);

    // Raw writes would bypass the encryption or filters of a stream that
    // cannot be cast to a plain descriptor. Checking without a result
    // pointer does not flush the stream, except that plain file streams
    // flush their stdio buffer, so they skip the check. The selectable cast
    // still rejects filtered streams.
    if (php_stream_is(stream, PHP_STREAM_IS_USERSPACE)
            || (!php_stream_is(stream, PHP_STREAM_IS_STDIO)
                && php_stream_cast(stream, PHP_STREAM_AS_FD | PHP_STREAM_CAST_INTERNAL, NULL, 0) == FAILURE)
            || php_stream_cast(stream, PHP_STREAM_AS_FD_FOR_SELECT | PHP_STREAM_CAST_INTERNAL, (void **)&fd, 0) == FAILURE
            || fstat(fd, &st) == -1
            || !(S_ISSOCK(st.st_mode) || S_ISFIFO(st.st_mode))) {
        zend_throw_exception(spl_ce_InvalidArgumentException, "The stream must be a plain socket or pipe", 0);
        return;
    }

    flags = fcntl(fd, F_GETFL);
    if (flags == -1) {
        zend_throw_exception_ex(ce_kafka_exception, 0, "Failed to get the stream's descriptor flags: %s", strerror(errno));
        return;
    }

    if ((flags & O_ACCMODE) == O_RDONLY) {
        zend_throw_exception(spl_ce_InvalidArgumentException, "The stream must be writable", 0);
        return;
    }

    // librdkafka writes from its own threads and must never block
    if (!(flags & O_NONBLOCK)) {
        zend_throw_exception(spl_ce_InvalidArgumentException, "The stream must be non-blocking", 0);
        return;
    }

    // Own a duplicate, so closing the stream and reusing its descriptor
    // number cannot redirect notifications to another resource
    io_event_fd = fcntl(fd, F_DUPFD_CLOEXEC, 0);
    if (io_event_fd == -1) {
        zend_throw_exception_ex(ce_kafka_exception, 0, "Failed to duplicate the stream's descriptor: %s", strerror(errno));
        return;
    }

    kafka_queue_io_event_disable(intern);

    if (payload) {
        rd_kafka_queue_io_event_enable(intern->rkqu, io_event_fd, ZSTR_VAL(payload), ZSTR_LEN(payload));
    } else {
        rd_kafka_queue_io_event_enable(intern->rkqu, io_event_fd, "\x01", 1);
    }

    intern->io_event_fd = io_event_fd;
}
/* }}} */
#endif

void kafka_queue_minit(INIT_FUNC_ARGS) { /* {{{ */

    handlers = kafka_default_object_handlers;
    handlers.free_obj = kafka_queue_free;
    handlers.get_gc = kafka_queue_get_gc;
    handlers.offset = offsetof(kafka_queue_object, std);

    ce_kafka_queue = register_class_RdKafka_Queue();
    ce_kafka_queue->create_object = kafka_queue_new;
} /* }}} */
