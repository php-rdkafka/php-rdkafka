dnl $Id$
dnl config.m4 for extension rdkafka

PHP_ARG_WITH(rdkafka, for rdkafka support,
[  --with-rdkafka             Include rdkafka support])

if test "$PHP_RDKAFKA" != "no"; then

  SEARCH_PATH="/usr/local /usr"     # you might want to change this
  SEARCH_FOR="/include/librdkafka/rdkafka.h"  # you most likely want to change this
  if test -r $PHP_RDKAFKA/$SEARCH_FOR; then # path given as parameter
    RDKAFKA_DIR=$PHP_RDKAFKA
  else # search default path list
    AC_MSG_CHECKING([for librdkafka/rdkafka.h" in default path])
    for i in $SEARCH_PATH ; do
      if test -r $i/$SEARCH_FOR; then
        RDKAFKA_DIR=$i
        AC_MSG_RESULT(found in $i)
      fi
    done
  fi

  if test -z "$RDKAFKA_DIR"; then
    AC_MSG_RESULT([not found])
    AC_MSG_ERROR([Please reinstall the rdkafka distribution])
  fi

  PHP_ADD_INCLUDE($RDKAFKA_DIR/include)

  SOURCES="rdkafka.c metadata.c metadata_broker.c metadata_topic.c metadata_partition.c metadata_collection.c conf.c topic.c queue.c message.c fun.c kafka_consumer.c topic_partition.c"

  LIBNAME=rdkafka
  LIBSYMBOL=rd_kafka_new

  PHP_CHECK_LIBRARY($LIBNAME,$LIBSYMBOL,
  [
    PHP_ADD_INCLUDE($RDKAFKA_DIR/include)
    PHP_ADD_LIBRARY_WITH_PATH($LIBNAME, $RDKAFKA_DIR/$PHP_LIBDIR, RDKAFKA_SHARED_LIBADD)
    AC_DEFINE(HAVE_RDKAFKALIB,1,[ ])
  ],[
    AC_MSG_ERROR([wrong rdkafka lib version or lib not found])
  ],[
    -L$RDKAFKA_DIR/$PHP_LIBDIR -lm
  ])

  ORIG_LDFLAGS="$LDFLAGS"
  ORIG_CPPFLAGS="$CPPFLAGS"
  LDFLAGS="-L$RDKAFKA_DIR/$PHP_LIBDIR -lm"
  CPPFLAGS="-I$RDKAFKA_DIR/include"

  AC_MSG_CHECKING([for librdkafka version])
  AC_EGREP_CPP(yes,[
#include <librdkafka/rdkafka.h>
#if RD_KAFKA_VERSION >= 0x000b0000
  yes
#endif
  ],[
    AC_MSG_RESULT([>= 0.11.0])
  ],[
    AC_MSG_ERROR([librdkafka version 0.11.0 or greater required.])
  ])

  AC_CHECK_LIB($LIBNAME,[rd_kafka_message_headers],[
    AC_DEFINE(HAVE_RD_KAFKA_MESSAGE_HEADERS,1,[ ])
  ],[
    AC_MSG_WARN([no rd_kafka_message_headers, headers support will not be available])
  ])

  AC_CHECK_LIB($LIBNAME,[rd_kafka_purge],[
    AC_DEFINE(HAS_RD_KAFKA_PURGE,1,[ ])
  ],[
    AC_MSG_WARN([purge is not available])
  ])

  AC_CHECK_LIB($LIBNAME,[rd_kafka_controllerid],[
#if RD_KAFKA_VERSION >= 0x010000ff
    AC_DEFINE(HAS_RD_KAFKA_CONTROLLERID,1,[ ])
#else
    AC_MSG_WARN([controllerid is broken on 0.11.x])
#endif
  ],[
    AC_MSG_WARN([controllerid is not available])
  ])

  AC_CHECK_LIB($LIBNAME,[rd_kafka_init_transactions],[
    AC_DEFINE(HAS_RD_KAFKA_TRANSACTIONS,1,[ ])
    SOURCES="$SOURCES kafka_error_exception.c"
  ],[
    AC_MSG_WARN([transactions are not available])
  ])

  AC_CHECK_LIB($LIBNAME,[rd_kafka_msg_partitioner_murmur2],[
    AC_DEFINE(HAS_RD_KAFKA_PARTITIONER_MURMUR2,1,[ ])
  ],[
    AC_MSG_WARN([murmur2 partitioner is not available])
  ])

  AC_CHECK_LIB($LIBNAME,[rd_kafka_oauthbearer_set_token],[
    AC_DEFINE(HAS_RD_KAFKA_OAUTHBEARER,1,[ ])
  ],[
    AC_MSG_WARN([oauthbearer support is not available])
  ])

  AC_CHECK_LIB($LIBNAME,[rd_kafka_incremental_assign, rd_kafka_incremental_unassign],[
    AC_DEFINE(HAS_RD_KAFKA_INCREMENTAL_ASSIGN,1,[ ])
  ],[
    AC_MSG_WARN([no rd_kafka_incremental_(un)assign, incremental rebalance support will not be available])
  ])

  LDFLAGS="$ORIG_LDFLAGS"
  CPPFLAGS="$ORIG_CPPFLAGS"

  PHP_SUBST(RDKAFKA_SHARED_LIBADD)

  PHP_NEW_EXTENSION(rdkafka, $SOURCES, $ext_shared)
fi
