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

typedef struct _kafka_topic_object {
    rd_kafka_topic_t    *rkt;
    HashTable           *registry;
    zval                zrk;
    zend_object         std;
} kafka_topic_object;

void kafka_topic_minit(INIT_FUNC_ARGS);
kafka_topic_object * get_kafka_topic_object(zval *zrkt);
void kafka_topic_object_pre_free(kafka_topic_object **pp);

extern zend_class_entry * ce_kafka_consumer_topic;
extern zend_class_entry * ce_kafka_kafka_consumer_topic;
extern zend_class_entry * ce_kafka_producer_topic;
extern zend_class_entry * ce_kafka_topic;
