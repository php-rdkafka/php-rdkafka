/*
+----------------------------------------------------------------------+
  | php-rdkafka                                                          |
  +----------------------------------------------------------------------+
  | Copyright (c) 2025 Arnaud Le Blanc                                   |
  +----------------------------------------------------------------------+
  | This source file is subject to version 3.01 of the PHP license,      |
  | that is bundled with this package in the file LICENSE, and is        |
  | available through the world-wide-web at the following url:           |
  | http://www.php.net/license/3_01.txt                                  |
  | If you did not receive a copy of the PHP license and are unable to   |
  | obtain it through the world-wide-web, please send a note to          |
  | license@php.net so we can mail you a copy immediately.               |
  +----------------------------------------------------------------------+
  | Author: Martin Fris <rasta@lj.sk>                                    |
  +----------------------------------------------------------------------+
*/

#include "librdkafka/rdkafka.h"

void oauthbearer_set_token(rd_kafka_t *rk,
    const char *token_value,
    int64_t lifetime_ms,
    const char *principal_name,
    const HashTable *extensions_hash
);

void oauthbearer_set_token_failure(rd_kafka_t *rk, const char *errstr);

int64_t zval_to_int64(zval *zval, const char *errstr);
