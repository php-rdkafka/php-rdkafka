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

#ifndef KAFKA_ADMIN_CLIENT_H
#define KAFKA_ADMIN_CLIENT_H

typedef struct _kafka_admin_options_object {
    rd_kafka_AdminOptions_t *options;
    zend_object              std;
} kafka_admin_options_object;

typedef struct _kafka_new_topic_object {
    rd_kafka_NewTopic_t     *new_topic;
    zend_object              std;
} kafka_new_topic_object;

typedef struct _kafka_delete_topic_object {
    rd_kafka_DeleteTopic_t  *delete_topic;
    zend_object              std;
} kafka_delete_topic_object;

typedef struct _kafka_new_partitions_object {
    rd_kafka_NewPartitions_t *new_partitions;
    zend_object               std;
} kafka_new_partitions_object;

void kafka_admin_client_minit(INIT_FUNC_ARGS);

#define get_admin_options_object(zv) \
    ((kafka_admin_options_object*)((char*)(Z_OBJ_P(zv)) - XtOffsetOf(kafka_admin_options_object, std)))
#define get_new_topic_object(zv) \
    ((kafka_new_topic_object*)((char*)(Z_OBJ_P(zv)) - XtOffsetOf(kafka_new_topic_object, std)))
#define get_delete_topic_object(zv) \
    ((kafka_delete_topic_object*)((char*)(Z_OBJ_P(zv)) - XtOffsetOf(kafka_delete_topic_object, std)))
#define get_new_partitions_object(zv) \
    ((kafka_new_partitions_object*)((char*)(Z_OBJ_P(zv)) - XtOffsetOf(kafka_new_partitions_object, std)))

/* Helpers for converting librdkafka admin result structs into PHP values.
 * Used by event.c to expose per-operation result accessors on RdKafka\Event. */
void kafka_topic_results_to_array(zval *return_value, const rd_kafka_topic_result_t **results, size_t cnt);
#ifdef HAS_RD_KAFKA_DESCRIBE_TOPICS
void kafka_node_to_zval(zval *return_value, const rd_kafka_Node_t *node);
void kafka_node_array_to_zval(zval *return_value, const rd_kafka_Node_t **nodes, size_t cnt);
void kafka_topic_partition_info_to_zval(zval *return_value, const rd_kafka_TopicPartitionInfo_t *partition);
void kafka_topic_description_to_zval(zval *return_value, const rd_kafka_TopicDescription_t *topicdesc);
#endif

extern zend_class_entry *ce_kafka_admin_options;
extern zend_class_entry *ce_kafka_new_topic;
extern zend_class_entry *ce_kafka_delete_topic;
extern zend_class_entry *ce_kafka_new_partitions;
extern zend_class_entry *ce_kafka_topic_result;
#ifdef HAS_RD_KAFKA_DESCRIBE_TOPICS
extern zend_class_entry *ce_kafka_node;
extern zend_class_entry *ce_kafka_topic_partition_info;
extern zend_class_entry *ce_kafka_topic_description;
#endif

#endif /* KAFKA_ADMIN_CLIENT_H */
