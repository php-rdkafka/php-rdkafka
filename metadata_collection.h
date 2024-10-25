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

typedef void (*kafka_metadata_collection_ctor_t)(zval *renurn_value, zval *zmetadata, const void *object);

void kafka_metadata_collection_minit(INIT_FUNC_ARGS);
void kafka_metadata_collection_init(zval *return_value, zend_object *zmetadata, const void * items, size_t item_cnt, size_t item_size, kafka_metadata_collection_ctor_t ctor);
