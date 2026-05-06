/* This is a generated file, edit the .stub.php file instead.
 * Stub hash: a7bcfe0d5aa01aa33ad6b46e9511a5d66de9f5e3 */

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

ZEND_METHOD(RdKafka, __construct);
ZEND_METHOD(RdKafka_Event, getType);
ZEND_METHOD(RdKafka_Event, getName);
ZEND_METHOD(RdKafka_Event, getError);
ZEND_METHOD(RdKafka_Event, getErrorString);

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
