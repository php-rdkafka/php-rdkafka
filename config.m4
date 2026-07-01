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

  SOURCES="rdkafka.c metadata.c metadata_broker.c metadata_topic.c metadata_partition.c metadata_collection.c conf.c topic.c queue.c message.c fun.c kafka_consumer.c topic_partition.c kafka_error_exception.c oauthbearer.c consumer_group_metadata.c"

  LIBNAME=rdkafka
  LIBSYMBOL=rd_kafka_new

  PHP_CHECK_LIBRARY($LIBNAME,$LIBSYMBOL,
  [
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
#if RD_KAFKA_VERSION >= 0x010503ff
  yes
#endif
  ],[
    AC_MSG_RESULT([>= 1.5.3])
  ],[
    AC_MSG_ERROR([librdkafka version 1.5.3 or greater required.])
  ])

  AC_CHECK_LIB($LIBNAME,[rd_kafka_incremental_assign, rd_kafka_incremental_unassign],[
    AC_DEFINE(HAS_RD_KAFKA_INCREMENTAL_ASSIGN,1,[ ])
  ],[
    AC_MSG_WARN([no rd_kafka_incremental_(un)assign, incremental rebalance support will not be available])
  ])

  AC_CHECK_LIB($LIBNAME,[rd_kafka_consumer_group_metadata_new_with_genid],[
    AC_DEFINE(HAS_RD_KAFKA_CONSUMER_GROUP_METADATA_NEW_WITH_GENID,1,[ ])
  ],[
    AC_MSG_WARN([no rd_kafka_consumer_group_metadata_new_with_genid, ConsumerGroupMetadata constructor limited to group_id only (requires librdkafka >= 1.7.0 for full constructor)])
  ])

  AC_CHECK_LIB($LIBNAME,[rd_kafka_consumer_group_metadata_group_id],[
    AC_DEFINE(HAS_RD_KAFKA_CONSUMER_GROUP_METADATA_GETTERS,1,[ ])
  ],[
    AC_MSG_WARN([no rd_kafka_consumer_group_metadata_group_id, ConsumerGroupMetadata getters not available (requires librdkafka >= 2.8.0)])
  ])

  AC_CHECK_LIB($LIBNAME,[rd_kafka_rebalance_protocol],[
    AC_DEFINE(HAS_RD_KAFKA_REBALANCE_PROTOCOL,1,[ ])
  ],[
    AC_MSG_WARN([no rd_kafka_rebalance_protocol, KafkaConsumer::getRebalanceProtocol() not available (requires librdkafka >= 1.6.0)])
  ])

  LDFLAGS="$ORIG_LDFLAGS"
  CPPFLAGS="$ORIG_CPPFLAGS"

  PHP_SUBST(RDKAFKA_SHARED_LIBADD)

  PHP_NEW_EXTENSION(rdkafka, $SOURCES, $ext_shared)
fi
