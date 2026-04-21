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

/* $Id$ */

#ifndef PHP_RDKAFKA_H
#define PHP_RDKAFKA_H

#include "librdkafka/rdkafka.h"
#include "conf.h"

#ifndef PHP_FE_END
#define PHP_FE_END { NULL, NULL, NULL, 0, 0 }
#endif

typedef struct _kafka_object {
    rd_kafka_type_t         type;
    rd_kafka_t              *rk;
    kafka_conf_callbacks    cbs;
    HashTable               consuming;
	HashTable				topics;
	HashTable				queues;
    zend_object             std;
} kafka_object;

PHP_METHOD(RdKafka, __construct);

extern zend_module_entry rdkafka_module_entry;
#define phpext_rdkafka_ptr &rdkafka_module_entry

#define PHP_RDKAFKA_VERSION "7.0.0alpha1"

extern zend_object_handlers kafka_default_object_handlers;
extern zend_class_entry * ce_kafka_exception;

#ifdef PHP_WIN32
#	define PHP_RDKAFKA_API __declspec(dllexport)
#elif defined(__GNUC__) && __GNUC__ >= 4
#	define PHP_RDKAFKA_API __attribute__ ((visibility("default")))
#else
#	define PHP_RDKAFKA_API
#endif

#endif	/* PHP_RDKAFKA_H */
