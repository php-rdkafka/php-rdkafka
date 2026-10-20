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

#ifndef KAFKA_CONF_H
#define KAFKA_CONF_H

enum {
        MSG_PARTITIONER_RANDOM = 2,
        MSG_PARTITIONER_CONSISTENT = 3,
        MSG_PARTITIONER_CONSISTENT_RANDOM = 4,
        MSG_PARTITIONER_MURMUR2 = 5,
        MSG_PARTITIONER_MURMUR2_RANDOM = 6
};

typedef enum {
    KAFKA_CONF = 1,
    KAFKA_TOPIC_CONF
} kafka_conf_type;

typedef struct _kafka_conf_callback {
    zend_fcall_info fci;
    zend_fcall_info_cache fcc;
} kafka_conf_callback;

typedef struct _kafka_conf_callbacks {
    zval zrk;
    kafka_conf_callback *error;
    kafka_conf_callback *rebalance;
    kafka_conf_callback *dr_msg;
    kafka_conf_callback *stats;
    kafka_conf_callback *consume;
    kafka_conf_callback *offset_commit;
    kafka_conf_callback *log;
    kafka_conf_callback *oauthbearer_token_refresh;
} kafka_conf_callbacks;

typedef struct _kafka_conf_object {
    kafka_conf_type type;
    union {
        rd_kafka_conf_t         *conf;
        rd_kafka_topic_conf_t   *topic_conf;
    } u;
    kafka_conf_callbacks cbs;
    zend_object                 std;
} kafka_conf_object;

kafka_conf_object * get_kafka_conf_object(zval *zconf);
void kafka_conf_minit(INIT_FUNC_ARGS);

void kafka_conf_callbacks_dtor(kafka_conf_callbacks *cbs);
void kafka_conf_callbacks_copy(kafka_conf_callbacks *to, kafka_conf_callbacks *from);

void kafka_conf_dr_msg_cb(rd_kafka_t *rk, const rd_kafka_message_t *msg, void *opaque);

extern zend_class_entry * ce_kafka_conf;
extern zend_class_entry * ce_kafka_topic_conf;

#endif /* KAFKA_CONF_H */
