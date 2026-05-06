/* This is a generated file, edit the .stub.php file instead.
 * Stub hash: 56e6b4c23579eea111939679c488994e961b4b0d */

ZEND_BEGIN_ARG_INFO_EX(arginfo_class_RdKafka___construct, 0, 0, 0)
ZEND_END_ARG_INFO()

#if (PHP_VERSION_ID >= 80100)
ZEND_BEGIN_ARG_WITH_TENTATIVE_RETURN_TYPE_INFO_EX(arginfo_class_RdKafka_addBrokers, 0, 1, IS_LONG, 0)
#else
ZEND_BEGIN_ARG_INFO_EX(arginfo_class_RdKafka_addBrokers, 0, 0, 1)
#endif
	ZEND_ARG_TYPE_INFO(0, broker_list, IS_STRING, 0)
ZEND_END_ARG_INFO()

#if (PHP_VERSION_ID >= 80100)
ZEND_BEGIN_ARG_WITH_TENTATIVE_RETURN_OBJ_INFO_EX(arginfo_class_RdKafka_getMetadata, 0, 3, RdKafka\\Metadata, 0)
#else
ZEND_BEGIN_ARG_INFO_EX(arginfo_class_RdKafka_getMetadata, 0, 0, 3)
#endif
	ZEND_ARG_TYPE_INFO(0, all_topics, _IS_BOOL, 0)
	ZEND_ARG_OBJ_INFO(0, only_topic, RdKafka\\Topic, 1)
	ZEND_ARG_TYPE_INFO(0, timeout_ms, IS_LONG, 0)
ZEND_END_ARG_INFO()

#if (PHP_VERSION_ID >= 80100)
ZEND_BEGIN_ARG_WITH_TENTATIVE_RETURN_TYPE_INFO_EX(arginfo_class_RdKafka_getControllerId, 0, 1, IS_LONG, 0)
#else
ZEND_BEGIN_ARG_INFO_EX(arginfo_class_RdKafka_getControllerId, 0, 0, 1)
#endif
	ZEND_ARG_TYPE_INFO(0, timeout_ms, IS_LONG, 0)
ZEND_END_ARG_INFO()

#if (PHP_VERSION_ID >= 80100)
ZEND_BEGIN_ARG_WITH_TENTATIVE_RETURN_TYPE_INFO_EX(arginfo_class_RdKafka_getOutQLen, 0, 0, IS_LONG, 0)
#else
ZEND_BEGIN_ARG_INFO_EX(arginfo_class_RdKafka_getOutQLen, 0, 0, 0)
#endif
ZEND_END_ARG_INFO()

#define arginfo_class_RdKafka_metadata arginfo_class_RdKafka_getMetadata

#if (PHP_VERSION_ID >= 80100)
ZEND_BEGIN_ARG_WITH_TENTATIVE_RETURN_TYPE_INFO_EX(arginfo_class_RdKafka_setLogLevel, 0, 1, IS_VOID, 0)
#else
ZEND_BEGIN_ARG_INFO_EX(arginfo_class_RdKafka_setLogLevel, 0, 0, 1)
#endif
	ZEND_ARG_TYPE_INFO(0, level, IS_LONG, 0)
ZEND_END_ARG_INFO()

#if (PHP_VERSION_ID >= 80100)
ZEND_BEGIN_ARG_WITH_TENTATIVE_RETURN_OBJ_INFO_EX(arginfo_class_RdKafka_newTopic, 0, 1, RdKafka\\Topic, 0)
#else
ZEND_BEGIN_ARG_INFO_EX(arginfo_class_RdKafka_newTopic, 0, 0, 1)
#endif
	ZEND_ARG_TYPE_INFO(0, topic_name, IS_STRING, 0)
	ZEND_ARG_OBJ_INFO_WITH_DEFAULT_VALUE(0, topic_conf, RdKafka\\TopicConf, 1, "null")
ZEND_END_ARG_INFO()

#define arginfo_class_RdKafka_outqLen arginfo_class_RdKafka_getOutQLen

#define arginfo_class_RdKafka_poll arginfo_class_RdKafka_getControllerId

#define arginfo_class_RdKafka_flush arginfo_class_RdKafka_getControllerId

#if (PHP_VERSION_ID >= 80100)
ZEND_BEGIN_ARG_WITH_TENTATIVE_RETURN_TYPE_INFO_EX(arginfo_class_RdKafka_purge, 0, 1, IS_LONG, 0)
#else
ZEND_BEGIN_ARG_INFO_EX(arginfo_class_RdKafka_purge, 0, 0, 1)
#endif
	ZEND_ARG_TYPE_INFO(0, purge_flags, IS_LONG, 0)
ZEND_END_ARG_INFO()

#if (PHP_VERSION_ID >= 80100)
ZEND_BEGIN_ARG_WITH_TENTATIVE_RETURN_TYPE_INFO_EX(arginfo_class_RdKafka_setLogger, 0, 1, IS_VOID, 0)
#else
ZEND_BEGIN_ARG_INFO_EX(arginfo_class_RdKafka_setLogger, 0, 0, 1)
#endif
	ZEND_ARG_TYPE_INFO(0, logger, IS_LONG, 0)
ZEND_END_ARG_INFO()

#if (PHP_VERSION_ID >= 80100)
ZEND_BEGIN_ARG_WITH_TENTATIVE_RETURN_TYPE_INFO_EX(arginfo_class_RdKafka_queryWatermarkOffsets, 0, 5, IS_VOID, 0)
#else
ZEND_BEGIN_ARG_INFO_EX(arginfo_class_RdKafka_queryWatermarkOffsets, 0, 0, 5)
#endif
	ZEND_ARG_TYPE_INFO(0, topic, IS_STRING, 0)
	ZEND_ARG_TYPE_INFO(0, partition, IS_LONG, 0)
	ZEND_ARG_TYPE_INFO(1, low, IS_LONG, 0)
	ZEND_ARG_TYPE_INFO(1, high, IS_LONG, 0)
	ZEND_ARG_TYPE_INFO(0, timeout_ms, IS_LONG, 0)
ZEND_END_ARG_INFO()

#if (PHP_VERSION_ID >= 80100)
ZEND_BEGIN_ARG_WITH_TENTATIVE_RETURN_TYPE_INFO_EX(arginfo_class_RdKafka_offsetsForTimes, 0, 2, IS_ARRAY, 0)
#else
ZEND_BEGIN_ARG_INFO_EX(arginfo_class_RdKafka_offsetsForTimes, 0, 0, 2)
#endif
	ZEND_ARG_TYPE_INFO(0, topic_partitions, IS_ARRAY, 0)
	ZEND_ARG_TYPE_INFO(0, timeout_ms, IS_LONG, 0)
ZEND_END_ARG_INFO()

#if (PHP_VERSION_ID >= 80100)
ZEND_BEGIN_ARG_WITH_TENTATIVE_RETURN_TYPE_INFO_EX(arginfo_class_RdKafka_pausePartitions, 0, 1, IS_ARRAY, 0)
#else
ZEND_BEGIN_ARG_INFO_EX(arginfo_class_RdKafka_pausePartitions, 0, 0, 1)
#endif
	ZEND_ARG_TYPE_INFO(0, topic_partitions, IS_ARRAY, 0)
ZEND_END_ARG_INFO()

#define arginfo_class_RdKafka_resumePartitions arginfo_class_RdKafka_pausePartitions

#if (PHP_VERSION_ID >= 80100)
ZEND_BEGIN_ARG_WITH_TENTATIVE_RETURN_TYPE_INFO_EX(arginfo_class_RdKafka_oauthbearerSetToken, 0, 3, IS_VOID, 0)
#else
ZEND_BEGIN_ARG_INFO_EX(arginfo_class_RdKafka_oauthbearerSetToken, 0, 0, 3)
#endif
	ZEND_ARG_TYPE_INFO(0, token_value, IS_STRING, 0)
	ZEND_ARG_TYPE_MASK(0, lifetime_ms, MAY_BE_LONG|MAY_BE_DOUBLE|MAY_BE_STRING, NULL)
	ZEND_ARG_TYPE_INFO(0, principal_name, IS_STRING, 0)
	ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, extensions, IS_ARRAY, 0, "[]")
ZEND_END_ARG_INFO()

#if (PHP_VERSION_ID >= 80100)
ZEND_BEGIN_ARG_WITH_TENTATIVE_RETURN_TYPE_INFO_EX(arginfo_class_RdKafka_oauthbearerSetTokenFailure, 0, 1, IS_VOID, 0)
#else
ZEND_BEGIN_ARG_INFO_EX(arginfo_class_RdKafka_oauthbearerSetTokenFailure, 0, 0, 1)
#endif
	ZEND_ARG_TYPE_INFO(0, error, IS_STRING, 0)
ZEND_END_ARG_INFO()

#if (PHP_VERSION_ID >= 80100)
ZEND_BEGIN_ARG_WITH_TENTATIVE_RETURN_OBJ_INFO_EX(arginfo_class_RdKafka_newQueue, 0, 0, RdKafka\\Queue, 0)
#else
ZEND_BEGIN_ARG_INFO_EX(arginfo_class_RdKafka_newQueue, 0, 0, 0)
#endif
ZEND_END_ARG_INFO()

#if (PHP_VERSION_ID >= 80100)
ZEND_BEGIN_ARG_WITH_TENTATIVE_RETURN_OBJ_INFO_EX(arginfo_class_RdKafka_newAdminOptions, 0, 1, RdKafka\\Admin\\AdminOptions, 0)
#else
ZEND_BEGIN_ARG_INFO_EX(arginfo_class_RdKafka_newAdminOptions, 0, 0, 1)
#endif
	ZEND_ARG_TYPE_INFO(0, for_api, IS_LONG, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_class_RdKafka_Consumer___construct, 0, 0, 0)
	ZEND_ARG_OBJ_INFO_WITH_DEFAULT_VALUE(0, conf, RdKafka\\Conf, 1, "null")
ZEND_END_ARG_INFO()

#define arginfo_class_RdKafka_Producer___construct arginfo_class_RdKafka_Consumer___construct

#if (PHP_VERSION_ID >= 80100)
ZEND_BEGIN_ARG_WITH_TENTATIVE_RETURN_TYPE_INFO_EX(arginfo_class_RdKafka_Producer_initTransactions, 0, 1, IS_VOID, 0)
#else
ZEND_BEGIN_ARG_INFO_EX(arginfo_class_RdKafka_Producer_initTransactions, 0, 0, 1)
#endif
	ZEND_ARG_TYPE_INFO(0, timeout_ms, IS_LONG, 0)
ZEND_END_ARG_INFO()

#if (PHP_VERSION_ID >= 80100)
ZEND_BEGIN_ARG_WITH_TENTATIVE_RETURN_TYPE_INFO_EX(arginfo_class_RdKafka_Producer_beginTransaction, 0, 0, IS_VOID, 0)
#else
ZEND_BEGIN_ARG_INFO_EX(arginfo_class_RdKafka_Producer_beginTransaction, 0, 0, 0)
#endif
ZEND_END_ARG_INFO()

#define arginfo_class_RdKafka_Producer_commitTransaction arginfo_class_RdKafka_Producer_initTransactions

#define arginfo_class_RdKafka_Producer_abortTransaction arginfo_class_RdKafka_Producer_initTransactions

ZEND_METHOD(RdKafka, __construct);
ZEND_METHOD(RdKafka, addBrokers);
ZEND_METHOD(RdKafka, getMetadata);
ZEND_METHOD(RdKafka, getControllerId);
ZEND_METHOD(RdKafka, getOutQLen);
ZEND_METHOD(RdKafka, setLogLevel);
ZEND_METHOD(RdKafka, newTopic);
ZEND_METHOD(RdKafka, poll);
ZEND_METHOD(RdKafka, flush);
ZEND_METHOD(RdKafka, purge);
ZEND_METHOD(RdKafka, setLogger);
ZEND_METHOD(RdKafka, queryWatermarkOffsets);
ZEND_METHOD(RdKafka, offsetsForTimes);
ZEND_METHOD(RdKafka, pausePartitions);
ZEND_METHOD(RdKafka, resumePartitions);
ZEND_METHOD(RdKafka, oauthbearerSetToken);
ZEND_METHOD(RdKafka, oauthbearerSetTokenFailure);
ZEND_METHOD(RdKafka, newQueue);
ZEND_METHOD(RdKafka, newAdminOptions);
ZEND_METHOD(RdKafka_Consumer, __construct);
ZEND_METHOD(RdKafka_Producer, __construct);
ZEND_METHOD(RdKafka_Producer, initTransactions);
ZEND_METHOD(RdKafka_Producer, beginTransaction);
ZEND_METHOD(RdKafka_Producer, commitTransaction);
ZEND_METHOD(RdKafka_Producer, abortTransaction);

static const zend_function_entry class_RdKafka_methods[] = {
	ZEND_ME(RdKafka, __construct, arginfo_class_RdKafka___construct, ZEND_ACC_PRIVATE)
	ZEND_ME(RdKafka, addBrokers, arginfo_class_RdKafka_addBrokers, ZEND_ACC_PUBLIC)
	ZEND_ME(RdKafka, getMetadata, arginfo_class_RdKafka_getMetadata, ZEND_ACC_PUBLIC)
	ZEND_ME(RdKafka, getControllerId, arginfo_class_RdKafka_getControllerId, ZEND_ACC_PUBLIC)
	ZEND_ME(RdKafka, getOutQLen, arginfo_class_RdKafka_getOutQLen, ZEND_ACC_PUBLIC)
#if (PHP_VERSION_ID >= 80400)
	ZEND_RAW_FENTRY("metadata", zim_RdKafka_getMetadata, arginfo_class_RdKafka_metadata, ZEND_ACC_PUBLIC|ZEND_ACC_DEPRECATED, NULL, NULL)
#else
	ZEND_RAW_FENTRY("metadata", zim_RdKafka_getMetadata, arginfo_class_RdKafka_metadata, ZEND_ACC_PUBLIC|ZEND_ACC_DEPRECATED)
#endif
	ZEND_ME(RdKafka, setLogLevel, arginfo_class_RdKafka_setLogLevel, ZEND_ACC_PUBLIC|ZEND_ACC_DEPRECATED)
	ZEND_ME(RdKafka, newTopic, arginfo_class_RdKafka_newTopic, ZEND_ACC_PUBLIC)
#if (PHP_VERSION_ID >= 80400)
	ZEND_RAW_FENTRY("outqLen", zim_RdKafka_getOutQLen, arginfo_class_RdKafka_outqLen, ZEND_ACC_PUBLIC|ZEND_ACC_DEPRECATED, NULL, NULL)
#else
	ZEND_RAW_FENTRY("outqLen", zim_RdKafka_getOutQLen, arginfo_class_RdKafka_outqLen, ZEND_ACC_PUBLIC|ZEND_ACC_DEPRECATED)
#endif
	ZEND_ME(RdKafka, poll, arginfo_class_RdKafka_poll, ZEND_ACC_PUBLIC)
	ZEND_ME(RdKafka, flush, arginfo_class_RdKafka_flush, ZEND_ACC_PUBLIC)
	ZEND_ME(RdKafka, purge, arginfo_class_RdKafka_purge, ZEND_ACC_PUBLIC)
	ZEND_ME(RdKafka, setLogger, arginfo_class_RdKafka_setLogger, ZEND_ACC_PUBLIC|ZEND_ACC_DEPRECATED)
	ZEND_ME(RdKafka, queryWatermarkOffsets, arginfo_class_RdKafka_queryWatermarkOffsets, ZEND_ACC_PUBLIC)
	ZEND_ME(RdKafka, offsetsForTimes, arginfo_class_RdKafka_offsetsForTimes, ZEND_ACC_PUBLIC)
	ZEND_ME(RdKafka, pausePartitions, arginfo_class_RdKafka_pausePartitions, ZEND_ACC_PUBLIC)
	ZEND_ME(RdKafka, resumePartitions, arginfo_class_RdKafka_resumePartitions, ZEND_ACC_PUBLIC)
	ZEND_ME(RdKafka, oauthbearerSetToken, arginfo_class_RdKafka_oauthbearerSetToken, ZEND_ACC_PUBLIC)
	ZEND_ME(RdKafka, oauthbearerSetTokenFailure, arginfo_class_RdKafka_oauthbearerSetTokenFailure, ZEND_ACC_PUBLIC)
	ZEND_ME(RdKafka, newQueue, arginfo_class_RdKafka_newQueue, ZEND_ACC_PUBLIC)
	ZEND_ME(RdKafka, newAdminOptions, arginfo_class_RdKafka_newAdminOptions, ZEND_ACC_PUBLIC)
	ZEND_FE_END
};

static const zend_function_entry class_RdKafka_Consumer_methods[] = {
	ZEND_ME(RdKafka_Consumer, __construct, arginfo_class_RdKafka_Consumer___construct, ZEND_ACC_PUBLIC)
	ZEND_FE_END
};

static const zend_function_entry class_RdKafka_Producer_methods[] = {
	ZEND_ME(RdKafka_Producer, __construct, arginfo_class_RdKafka_Producer___construct, ZEND_ACC_PUBLIC)
	ZEND_ME(RdKafka_Producer, initTransactions, arginfo_class_RdKafka_Producer_initTransactions, ZEND_ACC_PUBLIC)
	ZEND_ME(RdKafka_Producer, beginTransaction, arginfo_class_RdKafka_Producer_beginTransaction, ZEND_ACC_PUBLIC)
	ZEND_ME(RdKafka_Producer, commitTransaction, arginfo_class_RdKafka_Producer_commitTransaction, ZEND_ACC_PUBLIC)
	ZEND_ME(RdKafka_Producer, abortTransaction, arginfo_class_RdKafka_Producer_abortTransaction, ZEND_ACC_PUBLIC)
	ZEND_FE_END
};

static zend_class_entry *register_class_RdKafka(void)
{
	zend_class_entry ce, *class_entry;

	INIT_CLASS_ENTRY(ce, "RdKafka", class_RdKafka_methods);
#if (PHP_VERSION_ID >= 80400)
	class_entry = zend_register_internal_class_with_flags(&ce, NULL, ZEND_ACC_ABSTRACT);
#else
	class_entry = zend_register_internal_class_ex(&ce, NULL);
	class_entry->ce_flags |= ZEND_ACC_ABSTRACT;
#endif

	zval property_error_cb_default_value;
	ZVAL_UNDEF(&property_error_cb_default_value);
	zend_string *property_error_cb_name = zend_string_init("error_cb", sizeof("error_cb") - 1, 1);
	zend_declare_typed_property(class_entry, property_error_cb_name, &property_error_cb_default_value, ZEND_ACC_PRIVATE, NULL, (zend_type) ZEND_TYPE_INIT_MASK(MAY_BE_CALLABLE|MAY_BE_NULL));
	zend_string_release(property_error_cb_name);

	zval property_dr_cb_default_value;
	ZVAL_UNDEF(&property_dr_cb_default_value);
	zend_string *property_dr_cb_name = zend_string_init("dr_cb", sizeof("dr_cb") - 1, 1);
	zend_declare_typed_property(class_entry, property_dr_cb_name, &property_dr_cb_default_value, ZEND_ACC_PRIVATE, NULL, (zend_type) ZEND_TYPE_INIT_MASK(MAY_BE_CALLABLE|MAY_BE_NULL));
	zend_string_release(property_dr_cb_name);

	return class_entry;
}

static zend_class_entry *register_class_RdKafka_Exception(zend_class_entry *class_entry_Exception)
{
	zend_class_entry ce, *class_entry;

	INIT_NS_CLASS_ENTRY(ce, "RdKafka", "Exception", NULL);
#if (PHP_VERSION_ID >= 80400)
	class_entry = zend_register_internal_class_with_flags(&ce, class_entry_Exception, 0);
#else
	class_entry = zend_register_internal_class_ex(&ce, class_entry_Exception);
#endif

	return class_entry;
}

static zend_class_entry *register_class_RdKafka_Consumer(zend_class_entry *class_entry_RdKafka)
{
	zend_class_entry ce, *class_entry;

	INIT_NS_CLASS_ENTRY(ce, "RdKafka", "Consumer", class_RdKafka_Consumer_methods);
#if (PHP_VERSION_ID >= 80400)
	class_entry = zend_register_internal_class_with_flags(&ce, class_entry_RdKafka, 0);
#else
	class_entry = zend_register_internal_class_ex(&ce, class_entry_RdKafka);
#endif

	return class_entry;
}

static zend_class_entry *register_class_RdKafka_Producer(zend_class_entry *class_entry_RdKafka)
{
	zend_class_entry ce, *class_entry;

	INIT_NS_CLASS_ENTRY(ce, "RdKafka", "Producer", class_RdKafka_Producer_methods);
#if (PHP_VERSION_ID >= 80400)
	class_entry = zend_register_internal_class_with_flags(&ce, class_entry_RdKafka, 0);
#else
	class_entry = zend_register_internal_class_ex(&ce, class_entry_RdKafka);
#endif

	return class_entry;
}
