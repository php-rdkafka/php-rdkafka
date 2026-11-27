/* This is a generated file, edit the .stub.php file instead.
 * Stub hash: 18af6e57381129511e9b9da35c0523541e9b476b */

ZEND_BEGIN_ARG_INFO_EX(arginfo_class_RdKafka_Event___construct, 0, 0, 0)
ZEND_END_ARG_INFO()

#if (PHP_VERSION_ID >= 80100)
ZEND_BEGIN_ARG_WITH_TENTATIVE_RETURN_TYPE_INFO_EX(arginfo_class_RdKafka_Event_getType, 0, 0, IS_LONG, 0)
#else
ZEND_BEGIN_ARG_INFO_EX(arginfo_class_RdKafka_Event_getType, 0, 0, 0)
#endif
ZEND_END_ARG_INFO()

#if (PHP_VERSION_ID >= 80100)
ZEND_BEGIN_ARG_WITH_TENTATIVE_RETURN_TYPE_INFO_EX(arginfo_class_RdKafka_Event_getName, 0, 0, IS_STRING, 0)
#else
ZEND_BEGIN_ARG_INFO_EX(arginfo_class_RdKafka_Event_getName, 0, 0, 0)
#endif
ZEND_END_ARG_INFO()

#define arginfo_class_RdKafka_Event_getError arginfo_class_RdKafka_Event_getType

#if (PHP_VERSION_ID >= 80100)
ZEND_BEGIN_ARG_WITH_TENTATIVE_RETURN_TYPE_INFO_EX(arginfo_class_RdKafka_Event_getErrorString, 0, 0, IS_STRING, 1)
#else
ZEND_BEGIN_ARG_INFO_EX(arginfo_class_RdKafka_Event_getErrorString, 0, 0, 0)
#endif
ZEND_END_ARG_INFO()

#if (PHP_VERSION_ID >= 80100)
ZEND_BEGIN_ARG_WITH_TENTATIVE_RETURN_TYPE_INFO_EX(arginfo_class_RdKafka_Event_getCreateTopicsResult, 0, 0, IS_ARRAY, 0)
#else
ZEND_BEGIN_ARG_INFO_EX(arginfo_class_RdKafka_Event_getCreateTopicsResult, 0, 0, 0)
#endif
ZEND_END_ARG_INFO()

#define arginfo_class_RdKafka_Event_getDeleteTopicsResult arginfo_class_RdKafka_Event_getCreateTopicsResult

#define arginfo_class_RdKafka_Event_getCreatePartitionsResult arginfo_class_RdKafka_Event_getCreateTopicsResult

#if defined(HAS_RD_KAFKA_DESCRIBE_TOPICS)
#if (PHP_VERSION_ID >= 80100)
ZEND_BEGIN_ARG_WITH_TENTATIVE_RETURN_TYPE_INFO_EX(arginfo_class_RdKafka_Event_getDescribeTopicsResult, 0, 0, IS_ARRAY, 0)
#else
ZEND_BEGIN_ARG_INFO_EX(arginfo_class_RdKafka_Event_getDescribeTopicsResult, 0, 0, 0)
#endif
ZEND_END_ARG_INFO()
#endif

#define arginfo_class_RdKafka_Event_getDeleteRecordsResult arginfo_class_RdKafka_Event_getCreateTopicsResult

ZEND_METHOD(RdKafka, __construct);
ZEND_METHOD(RdKafka_Event, getType);
ZEND_METHOD(RdKafka_Event, getName);
ZEND_METHOD(RdKafka_Event, getError);
ZEND_METHOD(RdKafka_Event, getErrorString);
ZEND_METHOD(RdKafka_Event, getCreateTopicsResult);
ZEND_METHOD(RdKafka_Event, getDeleteTopicsResult);
ZEND_METHOD(RdKafka_Event, getCreatePartitionsResult);
#if defined(HAS_RD_KAFKA_DESCRIBE_TOPICS)
ZEND_METHOD(RdKafka_Event, getDescribeTopicsResult);
#endif
ZEND_METHOD(RdKafka_Event, getDeleteRecordsResult);

static const zend_function_entry class_RdKafka_Event_methods[] = {
#if (PHP_VERSION_ID >= 80400)
	ZEND_RAW_FENTRY("__construct", zim_RdKafka___construct, arginfo_class_RdKafka_Event___construct, ZEND_ACC_PRIVATE, NULL, NULL)
#else
	ZEND_RAW_FENTRY("__construct", zim_RdKafka___construct, arginfo_class_RdKafka_Event___construct, ZEND_ACC_PRIVATE)
#endif
	ZEND_ME(RdKafka_Event, getType, arginfo_class_RdKafka_Event_getType, ZEND_ACC_PUBLIC)
	ZEND_ME(RdKafka_Event, getName, arginfo_class_RdKafka_Event_getName, ZEND_ACC_PUBLIC)
	ZEND_ME(RdKafka_Event, getError, arginfo_class_RdKafka_Event_getError, ZEND_ACC_PUBLIC)
	ZEND_ME(RdKafka_Event, getErrorString, arginfo_class_RdKafka_Event_getErrorString, ZEND_ACC_PUBLIC)
	ZEND_ME(RdKafka_Event, getCreateTopicsResult, arginfo_class_RdKafka_Event_getCreateTopicsResult, ZEND_ACC_PUBLIC)
	ZEND_ME(RdKafka_Event, getDeleteTopicsResult, arginfo_class_RdKafka_Event_getDeleteTopicsResult, ZEND_ACC_PUBLIC)
	ZEND_ME(RdKafka_Event, getCreatePartitionsResult, arginfo_class_RdKafka_Event_getCreatePartitionsResult, ZEND_ACC_PUBLIC)
#if defined(HAS_RD_KAFKA_DESCRIBE_TOPICS)
	ZEND_ME(RdKafka_Event, getDescribeTopicsResult, arginfo_class_RdKafka_Event_getDescribeTopicsResult, ZEND_ACC_PUBLIC)
#endif
	ZEND_ME(RdKafka_Event, getDeleteRecordsResult, arginfo_class_RdKafka_Event_getDeleteRecordsResult, ZEND_ACC_PUBLIC)
	ZEND_FE_END
};

static zend_class_entry *register_class_RdKafka_Event(void)
{
	zend_class_entry ce, *class_entry;

	INIT_NS_CLASS_ENTRY(ce, "RdKafka", "Event", class_RdKafka_Event_methods);
#if (PHP_VERSION_ID >= 80400)
	class_entry = zend_register_internal_class_with_flags(&ce, NULL, 0);
#else
	class_entry = zend_register_internal_class_ex(&ce, NULL);
#endif

	return class_entry;
}
