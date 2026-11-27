/* This is a generated file, edit the .stub.php file instead.
 * Stub hash: 64df69a8354bd85d102e31429cfd798156e73cd0 */

ZEND_BEGIN_ARG_INFO_EX(arginfo_class_RdKafka_Queue___construct, 0, 0, 0)
ZEND_END_ARG_INFO()

#if (PHP_VERSION_ID >= 80100)
ZEND_BEGIN_ARG_WITH_TENTATIVE_RETURN_OBJ_INFO_EX(arginfo_class_RdKafka_Queue_consume, 0, 1, RdKafka\\Message, 1)
#else
ZEND_BEGIN_ARG_INFO_EX(arginfo_class_RdKafka_Queue_consume, 0, 0, 1)
#endif
	ZEND_ARG_TYPE_INFO(0, timeout_ms, IS_LONG, 0)
ZEND_END_ARG_INFO()

#if (PHP_VERSION_ID >= 80100)
ZEND_BEGIN_ARG_WITH_TENTATIVE_RETURN_OBJ_INFO_EX(arginfo_class_RdKafka_Queue_poll, 0, 1, RdKafka\\Event, 1)
#else
ZEND_BEGIN_ARG_INFO_EX(arginfo_class_RdKafka_Queue_poll, 0, 0, 1)
#endif
	ZEND_ARG_TYPE_INFO(0, timeout_ms, IS_LONG, 0)
ZEND_END_ARG_INFO()

ZEND_METHOD(RdKafka, __construct);
ZEND_METHOD(RdKafka_Queue, consume);
ZEND_METHOD(RdKafka_Queue, poll);

static const zend_function_entry class_RdKafka_Queue_methods[] = {
#if (PHP_VERSION_ID >= 80400)
	ZEND_RAW_FENTRY("__construct", zim_RdKafka___construct, arginfo_class_RdKafka_Queue___construct, ZEND_ACC_PRIVATE, NULL, NULL)
#else
	ZEND_RAW_FENTRY("__construct", zim_RdKafka___construct, arginfo_class_RdKafka_Queue___construct, ZEND_ACC_PRIVATE)
#endif
	ZEND_ME(RdKafka_Queue, consume, arginfo_class_RdKafka_Queue_consume, ZEND_ACC_PUBLIC)
	ZEND_ME(RdKafka_Queue, poll, arginfo_class_RdKafka_Queue_poll, ZEND_ACC_PUBLIC)
	ZEND_FE_END
};

static zend_class_entry *register_class_RdKafka_Queue(void)
{
	zend_class_entry ce, *class_entry;

	INIT_NS_CLASS_ENTRY(ce, "RdKafka", "Queue", class_RdKafka_Queue_methods);
#if (PHP_VERSION_ID >= 80400)
	class_entry = zend_register_internal_class_with_flags(&ce, NULL, 0);
#else
	class_entry = zend_register_internal_class_ex(&ce, NULL);
#endif

	return class_entry;
}
