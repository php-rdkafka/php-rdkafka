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

typedef struct _kafka_topic_partition_intern {
    char        *topic;
    int32_t     partition;
    int64_t     offset;
    rd_kafka_resp_err_t err;
    char        *metadata;
    size_t      metadata_size;
    zend_object std;
} kafka_topic_partition_intern;

void kafka_metadata_topic_partition_minit(INIT_FUNC_ARGS);

kafka_topic_partition_intern * get_topic_partition_object(zval *z);
void kafka_topic_partition_init(zval *z, char *topic, int32_t partition, int64_t offset, rd_kafka_resp_err_t err);

void kafka_topic_partition_list_to_array(zval *return_value, rd_kafka_topic_partition_list_t *list);
rd_kafka_topic_partition_list_t * array_arg_to_kafka_topic_partition_list(int argnum, HashTable *ary);

extern zend_class_entry * ce_kafka_topic_partition;
