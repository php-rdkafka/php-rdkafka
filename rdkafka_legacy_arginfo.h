/* This is a generated file, edit the .stub.php file instead.
 * Stub hash: 44a34fe532e7450e431d2772ce89df92d2d966b6 */

ZEND_BEGIN_ARG_INFO_EX(arginfo_class_RdKafka___construct, 0, 0, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_class_RdKafka_addBrokers, 0, 0, 1)
	ZEND_ARG_INFO(0, broker_list)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_class_RdKafka_getMetadata, 0, 0, 3)
	ZEND_ARG_INFO(0, all_topics)
	ZEND_ARG_INFO(0, only_topic)
	ZEND_ARG_INFO(0, timeout_ms)
ZEND_END_ARG_INFO()

#if defined(HAS_RD_KAFKA_CONTROLLERID)
ZEND_BEGIN_ARG_INFO_EX(arginfo_class_RdKafka_getControllerId, 0, 0, 1)
	ZEND_ARG_INFO(0, timeout_ms)
ZEND_END_ARG_INFO()
#endif

ZEND_BEGIN_ARG_INFO_EX(arginfo_class_RdKafka_getOutQLen, 0, 0, 0)
ZEND_END_ARG_INFO()

#define arginfo_class_RdKafka_metadata arginfo_class_RdKafka_getMetadata

ZEND_BEGIN_ARG_INFO_EX(arginfo_class_RdKafka_setLogLevel, 0, 0, 1)
	ZEND_ARG_INFO(0, level)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_class_RdKafka_newTopic, 0, 0, 1)
	ZEND_ARG_INFO(0, topic_name)
	ZEND_ARG_INFO(0, topic_conf)
ZEND_END_ARG_INFO()

#define arginfo_class_RdKafka_outqLen arginfo_class_RdKafka_getOutQLen

ZEND_BEGIN_ARG_INFO_EX(arginfo_class_RdKafka_poll, 0, 0, 1)
	ZEND_ARG_INFO(0, timeout_ms)
ZEND_END_ARG_INFO()

#define arginfo_class_RdKafka_flush arginfo_class_RdKafka_poll

#if defined(HAS_RD_KAFKA_PURGE)
ZEND_BEGIN_ARG_INFO_EX(arginfo_class_RdKafka_purge, 0, 0, 1)
	ZEND_ARG_INFO(0, purge_flags)
ZEND_END_ARG_INFO()
#endif

ZEND_BEGIN_ARG_INFO_EX(arginfo_class_RdKafka_setLogger, 0, 0, 1)
	ZEND_ARG_INFO(0, logger)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_class_RdKafka_queryWatermarkOffsets, 0, 0, 5)
	ZEND_ARG_INFO(0, topic)
	ZEND_ARG_INFO(0, partition)
	ZEND_ARG_INFO(1, low)
	ZEND_ARG_INFO(1, high)
	ZEND_ARG_INFO(0, timeout_ms)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_class_RdKafka_offsetsForTimes, 0, 0, 2)
	ZEND_ARG_INFO(0, topic_partitions)
	ZEND_ARG_INFO(0, timeout_ms)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_class_RdKafka_pausePartitions, 0, 0, 1)
	ZEND_ARG_INFO(0, topic_partitions)
ZEND_END_ARG_INFO()

#define arginfo_class_RdKafka_resumePartitions arginfo_class_RdKafka_pausePartitions

#if defined(HAS_RD_KAFKA_OAUTHBEARER)
ZEND_BEGIN_ARG_INFO_EX(arginfo_class_RdKafka_oauthbearerSetToken, 0, 0, 3)
	ZEND_ARG_INFO(0, token_value)
	ZEND_ARG_INFO(0, lifetime_ms)
	ZEND_ARG_INFO(0, principal_name)
	ZEND_ARG_INFO(0, extensions)
ZEND_END_ARG_INFO()
#endif

#if defined(HAS_RD_KAFKA_OAUTHBEARER)
ZEND_BEGIN_ARG_INFO_EX(arginfo_class_RdKafka_oauthbearerSetTokenFailure, 0, 0, 1)
	ZEND_ARG_INFO(0, error)
ZEND_END_ARG_INFO()
#endif

ZEND_BEGIN_ARG_INFO_EX(arginfo_class_RdKafka_Consumer___construct, 0, 0, 0)
	ZEND_ARG_INFO(0, conf)
ZEND_END_ARG_INFO()

#define arginfo_class_RdKafka_Consumer_newQueue arginfo_class_RdKafka_getOutQLen

#define arginfo_class_RdKafka_Producer___construct arginfo_class_RdKafka_Consumer___construct

#if defined(HAS_RD_KAFKA_TRANSACTIONS)
ZEND_BEGIN_ARG_INFO_EX(arginfo_class_RdKafka_Producer_initTransactions, 0, 0, 1)
	ZEND_ARG_INFO(0, timeout_ms)
ZEND_END_ARG_INFO()
#endif

#if defined(HAS_RD_KAFKA_TRANSACTIONS)
ZEND_BEGIN_ARG_INFO_EX(arginfo_class_RdKafka_Producer_beginTransaction, 0, 0, 0)
ZEND_END_ARG_INFO()
#endif

#if defined(HAS_RD_KAFKA_TRANSACTIONS)
#define arginfo_class_RdKafka_Producer_commitTransaction arginfo_class_RdKafka_Producer_initTransactions
#endif

#if defined(HAS_RD_KAFKA_TRANSACTIONS)
#define arginfo_class_RdKafka_Producer_abortTransaction arginfo_class_RdKafka_Producer_initTransactions
#endif


ZEND_METHOD(RdKafka, __construct);
ZEND_METHOD(RdKafka, addBrokers);
ZEND_METHOD(RdKafka, getMetadata);
#if defined(HAS_RD_KAFKA_CONTROLLERID)
ZEND_METHOD(RdKafka, getControllerId);
#endif
ZEND_METHOD(RdKafka, getOutQLen);
ZEND_METHOD(RdKafka, setLogLevel);
ZEND_METHOD(RdKafka, newTopic);
ZEND_METHOD(RdKafka, poll);
ZEND_METHOD(RdKafka, flush);
#if defined(HAS_RD_KAFKA_PURGE)
ZEND_METHOD(RdKafka, purge);
#endif
ZEND_METHOD(RdKafka, setLogger);
ZEND_METHOD(RdKafka, queryWatermarkOffsets);
ZEND_METHOD(RdKafka, offsetsForTimes);
ZEND_METHOD(RdKafka, pausePartitions);
ZEND_METHOD(RdKafka, resumePartitions);
#if defined(HAS_RD_KAFKA_OAUTHBEARER)
ZEND_METHOD(RdKafka, oauthbearerSetToken);
#endif
#if defined(HAS_RD_KAFKA_OAUTHBEARER)
ZEND_METHOD(RdKafka, oauthbearerSetTokenFailure);
#endif
ZEND_METHOD(RdKafka_Consumer, __construct);
ZEND_METHOD(RdKafka_Consumer, newQueue);
ZEND_METHOD(RdKafka_Producer, __construct);
#if defined(HAS_RD_KAFKA_TRANSACTIONS)
ZEND_METHOD(RdKafka_Producer, initTransactions);
#endif
#if defined(HAS_RD_KAFKA_TRANSACTIONS)
ZEND_METHOD(RdKafka_Producer, beginTransaction);
#endif
#if defined(HAS_RD_KAFKA_TRANSACTIONS)
ZEND_METHOD(RdKafka_Producer, commitTransaction);
#endif
#if defined(HAS_RD_KAFKA_TRANSACTIONS)
ZEND_METHOD(RdKafka_Producer, abortTransaction);
#endif


static const zend_function_entry class_RdKafka_methods[] = {
	ZEND_ME(RdKafka, __construct, arginfo_class_RdKafka___construct, ZEND_ACC_PRIVATE)
	ZEND_ME(RdKafka, addBrokers, arginfo_class_RdKafka_addBrokers, ZEND_ACC_PUBLIC)
	ZEND_ME(RdKafka, getMetadata, arginfo_class_RdKafka_getMetadata, ZEND_ACC_PUBLIC)
#if defined(HAS_RD_KAFKA_CONTROLLERID)
	ZEND_ME(RdKafka, getControllerId, arginfo_class_RdKafka_getControllerId, ZEND_ACC_PUBLIC)
#endif
	ZEND_ME(RdKafka, getOutQLen, arginfo_class_RdKafka_getOutQLen, ZEND_ACC_PUBLIC)
	ZEND_MALIAS(RdKafka, metadata, getMetadata, arginfo_class_RdKafka_metadata, ZEND_ACC_PUBLIC|ZEND_ACC_DEPRECATED)
	ZEND_ME(RdKafka, setLogLevel, arginfo_class_RdKafka_setLogLevel, ZEND_ACC_PUBLIC|ZEND_ACC_DEPRECATED)
	ZEND_ME(RdKafka, newTopic, arginfo_class_RdKafka_newTopic, ZEND_ACC_PUBLIC)
	ZEND_MALIAS(RdKafka, outqLen, getOutQLen, arginfo_class_RdKafka_outqLen, ZEND_ACC_PUBLIC|ZEND_ACC_DEPRECATED)
	ZEND_ME(RdKafka, poll, arginfo_class_RdKafka_poll, ZEND_ACC_PUBLIC)
	ZEND_ME(RdKafka, flush, arginfo_class_RdKafka_flush, ZEND_ACC_PUBLIC)
#if defined(HAS_RD_KAFKA_PURGE)
	ZEND_ME(RdKafka, purge, arginfo_class_RdKafka_purge, ZEND_ACC_PUBLIC)
#endif
	ZEND_ME(RdKafka, setLogger, arginfo_class_RdKafka_setLogger, ZEND_ACC_PUBLIC|ZEND_ACC_DEPRECATED)
	ZEND_ME(RdKafka, queryWatermarkOffsets, arginfo_class_RdKafka_queryWatermarkOffsets, ZEND_ACC_PUBLIC)
	ZEND_ME(RdKafka, offsetsForTimes, arginfo_class_RdKafka_offsetsForTimes, ZEND_ACC_PUBLIC)
	ZEND_ME(RdKafka, pausePartitions, arginfo_class_RdKafka_pausePartitions, ZEND_ACC_PUBLIC)
	ZEND_ME(RdKafka, resumePartitions, arginfo_class_RdKafka_resumePartitions, ZEND_ACC_PUBLIC)
#if defined(HAS_RD_KAFKA_OAUTHBEARER)
	ZEND_ME(RdKafka, oauthbearerSetToken, arginfo_class_RdKafka_oauthbearerSetToken, ZEND_ACC_PUBLIC)
#endif
#if defined(HAS_RD_KAFKA_OAUTHBEARER)
	ZEND_ME(RdKafka, oauthbearerSetTokenFailure, arginfo_class_RdKafka_oauthbearerSetTokenFailure, ZEND_ACC_PUBLIC)
#endif
	ZEND_FE_END
};


static const zend_function_entry class_RdKafka_Exception_methods[] = {
	ZEND_FE_END
};


static const zend_function_entry class_RdKafka_Consumer_methods[] = {
	ZEND_ME(RdKafka_Consumer, __construct, arginfo_class_RdKafka_Consumer___construct, ZEND_ACC_PUBLIC)
	ZEND_ME(RdKafka_Consumer, newQueue, arginfo_class_RdKafka_Consumer_newQueue, ZEND_ACC_PUBLIC)
	ZEND_FE_END
};


static const zend_function_entry class_RdKafka_Producer_methods[] = {
	ZEND_ME(RdKafka_Producer, __construct, arginfo_class_RdKafka_Producer___construct, ZEND_ACC_PUBLIC)
#if defined(HAS_RD_KAFKA_TRANSACTIONS)
	ZEND_ME(RdKafka_Producer, initTransactions, arginfo_class_RdKafka_Producer_initTransactions, ZEND_ACC_PUBLIC)
#endif
#if defined(HAS_RD_KAFKA_TRANSACTIONS)
	ZEND_ME(RdKafka_Producer, beginTransaction, arginfo_class_RdKafka_Producer_beginTransaction, ZEND_ACC_PUBLIC)
#endif
#if defined(HAS_RD_KAFKA_TRANSACTIONS)
	ZEND_ME(RdKafka_Producer, commitTransaction, arginfo_class_RdKafka_Producer_commitTransaction, ZEND_ACC_PUBLIC)
#endif
#if defined(HAS_RD_KAFKA_TRANSACTIONS)
	ZEND_ME(RdKafka_Producer, abortTransaction, arginfo_class_RdKafka_Producer_abortTransaction, ZEND_ACC_PUBLIC)
#endif
	ZEND_FE_END
};

static zend_class_entry *register_class_RdKafka(void)
{
	zend_class_entry ce, *class_entry;

	INIT_CLASS_ENTRY(ce, "RdKafka", class_RdKafka_methods);
	class_entry = zend_register_internal_class_ex(&ce, NULL);
	class_entry->ce_flags |= ZEND_ACC_ABSTRACT;

	zval property_error_cb_default_value;
	ZVAL_NULL(&property_error_cb_default_value);
	zend_string *property_error_cb_name = zend_string_init("error_cb", sizeof("error_cb") - 1, 1);
	zend_declare_property_ex(class_entry, property_error_cb_name, &property_error_cb_default_value, ZEND_ACC_PRIVATE, NULL);
	zend_string_release(property_error_cb_name);

	zval property_dr_cb_default_value;
	ZVAL_NULL(&property_dr_cb_default_value);
	zend_string *property_dr_cb_name = zend_string_init("dr_cb", sizeof("dr_cb") - 1, 1);
	zend_declare_property_ex(class_entry, property_dr_cb_name, &property_dr_cb_default_value, ZEND_ACC_PRIVATE, NULL);
	zend_string_release(property_dr_cb_name);

	return class_entry;
}

static zend_class_entry *register_class_RdKafka_Exception(zend_class_entry *class_entry_Exception)
{
	zend_class_entry ce, *class_entry;

	INIT_NS_CLASS_ENTRY(ce, "RdKafka", "Exception", class_RdKafka_Exception_methods);
	class_entry = zend_register_internal_class_ex(&ce, class_entry_Exception);

	return class_entry;
}

static zend_class_entry *register_class_RdKafka_Consumer(zend_class_entry *class_entry_RdKafka)
{
	zend_class_entry ce, *class_entry;

	INIT_NS_CLASS_ENTRY(ce, "RdKafka", "Consumer", class_RdKafka_Consumer_methods);
	class_entry = zend_register_internal_class_ex(&ce, class_entry_RdKafka);

	return class_entry;
}

static zend_class_entry *register_class_RdKafka_Producer(zend_class_entry *class_entry_RdKafka)
{
	zend_class_entry ce, *class_entry;

	INIT_NS_CLASS_ENTRY(ce, "RdKafka", "Producer", class_RdKafka_Producer_methods);
	class_entry = zend_register_internal_class_ex(&ce, class_entry_RdKafka);

	return class_entry;
}
