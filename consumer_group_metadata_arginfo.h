/* This is a generated file, edit the .stub.php file instead.
 * Stub hash: 052cdd530530426eea100b285f8de2381bfd3a97 */

ZEND_BEGIN_ARG_INFO_EX(arginfo_class_RdKafka_ConsumerGroupMetadata___construct, 0, 0, 1)
	ZEND_ARG_TYPE_INFO(0, group_id, IS_STRING, 0)
	ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, generation_id, IS_LONG, 0, "-1")
	ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, member_id, IS_STRING, 0, "\"\"")
	ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, group_instance_id, IS_STRING, 1, "null")
ZEND_END_ARG_INFO()

#if (PHP_VERSION_ID >= 80100)
ZEND_BEGIN_ARG_WITH_TENTATIVE_RETURN_TYPE_INFO_EX(arginfo_class_RdKafka_ConsumerGroupMetadata_getGroupId, 0, 0, IS_STRING, 0)
#else
ZEND_BEGIN_ARG_INFO_EX(arginfo_class_RdKafka_ConsumerGroupMetadata_getGroupId, 0, 0, 0)
#endif
ZEND_END_ARG_INFO()

#define arginfo_class_RdKafka_ConsumerGroupMetadata_getMemberId arginfo_class_RdKafka_ConsumerGroupMetadata_getGroupId

#if (PHP_VERSION_ID >= 80100)
ZEND_BEGIN_ARG_WITH_TENTATIVE_RETURN_TYPE_INFO_EX(arginfo_class_RdKafka_ConsumerGroupMetadata_getGroupInstanceId, 0, 0, IS_STRING, 1)
#else
ZEND_BEGIN_ARG_INFO_EX(arginfo_class_RdKafka_ConsumerGroupMetadata_getGroupInstanceId, 0, 0, 0)
#endif
ZEND_END_ARG_INFO()

#if (PHP_VERSION_ID >= 80100)
ZEND_BEGIN_ARG_WITH_TENTATIVE_RETURN_TYPE_INFO_EX(arginfo_class_RdKafka_ConsumerGroupMetadata_getGenerationId, 0, 0, IS_LONG, 0)
#else
ZEND_BEGIN_ARG_INFO_EX(arginfo_class_RdKafka_ConsumerGroupMetadata_getGenerationId, 0, 0, 0)
#endif
ZEND_END_ARG_INFO()


ZEND_METHOD(RdKafka_ConsumerGroupMetadata, __construct);
ZEND_METHOD(RdKafka_ConsumerGroupMetadata, getGroupId);
ZEND_METHOD(RdKafka_ConsumerGroupMetadata, getMemberId);
ZEND_METHOD(RdKafka_ConsumerGroupMetadata, getGroupInstanceId);
ZEND_METHOD(RdKafka_ConsumerGroupMetadata, getGenerationId);


static const zend_function_entry class_RdKafka_ConsumerGroupMetadata_methods[] = {
	ZEND_ME(RdKafka_ConsumerGroupMetadata, __construct, arginfo_class_RdKafka_ConsumerGroupMetadata___construct, ZEND_ACC_PUBLIC)
	ZEND_ME(RdKafka_ConsumerGroupMetadata, getGroupId, arginfo_class_RdKafka_ConsumerGroupMetadata_getGroupId, ZEND_ACC_PUBLIC)
	ZEND_ME(RdKafka_ConsumerGroupMetadata, getMemberId, arginfo_class_RdKafka_ConsumerGroupMetadata_getMemberId, ZEND_ACC_PUBLIC)
	ZEND_ME(RdKafka_ConsumerGroupMetadata, getGroupInstanceId, arginfo_class_RdKafka_ConsumerGroupMetadata_getGroupInstanceId, ZEND_ACC_PUBLIC)
	ZEND_ME(RdKafka_ConsumerGroupMetadata, getGenerationId, arginfo_class_RdKafka_ConsumerGroupMetadata_getGenerationId, ZEND_ACC_PUBLIC)
	ZEND_FE_END
};

static zend_class_entry *register_class_RdKafka_ConsumerGroupMetadata(void)
{
	zend_class_entry ce, *class_entry;

	INIT_NS_CLASS_ENTRY(ce, "RdKafka", "ConsumerGroupMetadata", class_RdKafka_ConsumerGroupMetadata_methods);
	class_entry = zend_register_internal_class_ex(&ce, NULL);
	class_entry->ce_flags |= ZEND_ACC_FINAL;

	return class_entry;
}
