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

typedef struct _kafka_queue_object {
    rd_kafka_queue_t    *rkqu;
    HashTable           *registry;
    zend_string         *registry_key;
#ifndef PHP_WIN32
    int                 io_event_fd;
#endif
    zval                zrk;
    zend_object         std;
} kafka_queue_object;

void kafka_queue_minit(INIT_FUNC_ARGS);
kafka_queue_object * get_kafka_queue_object(zval *zrkqu);
kafka_queue_object * kafka_queue_object_init(zval *return_value, zval *zrk, HashTable *registry, rd_kafka_queue_t *rkqu, zend_string *registry_key);
void kafka_queue_object_pre_free(kafka_queue_object **pp);

extern zend_class_entry * ce_kafka_queue;
