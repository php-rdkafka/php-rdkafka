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

#ifndef KAFKA_EVENT_H
#define KAFKA_EVENT_H

typedef struct _kafka_event_object {
    rd_kafka_event_t    *rkev;
    zend_object         std;
} kafka_event_object;

void kafka_event_minit(INIT_FUNC_ARGS);
void kafka_event_new(zval *return_value, rd_kafka_event_t *rkev);

extern zend_class_entry *ce_kafka_event;

#endif /* KAFKA_EVENT_H */
