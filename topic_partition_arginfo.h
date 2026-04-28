/* This is a generated file, edit the .stub.php file instead.
 * Stub hash: 7c722b9eb9357157d89a14431ebcfd79cc6f1116 */

ZEND_BEGIN_ARG_INFO_EX(arginfo_class_RdKafka_TopicPartition___construct, 0, 0, 2)
	ZEND_ARG_TYPE_INFO(0, topic, IS_STRING, 0)
	ZEND_ARG_TYPE_INFO(0, partition, IS_LONG, 0)
	ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, offset, IS_LONG, 0, "0")
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_TENTATIVE_RETURN_TYPE_INFO_EX(arginfo_class_RdKafka_TopicPartition_getTopic, 0, 0, IS_STRING, 1)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_TENTATIVE_RETURN_OBJ_INFO_EX(arginfo_class_RdKafka_TopicPartition_setTopic, 0, 1, RdKafka\\TopicPartition, 0)
	ZEND_ARG_TYPE_INFO(0, topic_name, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_TENTATIVE_RETURN_TYPE_INFO_EX(arginfo_class_RdKafka_TopicPartition_getPartition, 0, 0, IS_LONG, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_TENTATIVE_RETURN_OBJ_INFO_EX(arginfo_class_RdKafka_TopicPartition_setPartition, 0, 1, RdKafka\\TopicPartition, 0)
	ZEND_ARG_TYPE_INFO(0, partition, IS_LONG, 0)
ZEND_END_ARG_INFO()

#define arginfo_class_RdKafka_TopicPartition_getOffset arginfo_class_RdKafka_TopicPartition_getPartition

ZEND_BEGIN_ARG_WITH_TENTATIVE_RETURN_OBJ_INFO_EX(arginfo_class_RdKafka_TopicPartition_setOffset, 0, 1, RdKafka\\TopicPartition, 0)
	ZEND_ARG_TYPE_INFO(0, offset, IS_LONG, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_TENTATIVE_RETURN_TYPE_INFO_EX(arginfo_class_RdKafka_TopicPartition_getErr, 0, 0, IS_LONG, 1)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_TENTATIVE_RETURN_TYPE_INFO_EX(arginfo_class_RdKafka_TopicPartition_getMetadata, 0, 0, IS_STRING, 1)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_TENTATIVE_RETURN_OBJ_INFO_EX(arginfo_class_RdKafka_TopicPartition_setMetadata, 0, 1, RdKafka\\TopicPartition, 0)
	ZEND_ARG_TYPE_INFO(0, metadata, IS_STRING, 1)
ZEND_END_ARG_INFO()


ZEND_METHOD(RdKafka_TopicPartition, __construct);
ZEND_METHOD(RdKafka_TopicPartition, getTopic);
ZEND_METHOD(RdKafka_TopicPartition, setTopic);
ZEND_METHOD(RdKafka_TopicPartition, getPartition);
ZEND_METHOD(RdKafka_TopicPartition, setPartition);
ZEND_METHOD(RdKafka_TopicPartition, getOffset);
ZEND_METHOD(RdKafka_TopicPartition, setOffset);
ZEND_METHOD(RdKafka_TopicPartition, getErr);
ZEND_METHOD(RdKafka_TopicPartition, getMetadata);
ZEND_METHOD(RdKafka_TopicPartition, setMetadata);


static const zend_function_entry class_RdKafka_TopicPartition_methods[] = {
	ZEND_ME(RdKafka_TopicPartition, __construct, arginfo_class_RdKafka_TopicPartition___construct, ZEND_ACC_PUBLIC)
	ZEND_ME(RdKafka_TopicPartition, getTopic, arginfo_class_RdKafka_TopicPartition_getTopic, ZEND_ACC_PUBLIC)
	ZEND_ME(RdKafka_TopicPartition, setTopic, arginfo_class_RdKafka_TopicPartition_setTopic, ZEND_ACC_PUBLIC)
	ZEND_ME(RdKafka_TopicPartition, getPartition, arginfo_class_RdKafka_TopicPartition_getPartition, ZEND_ACC_PUBLIC)
	ZEND_ME(RdKafka_TopicPartition, setPartition, arginfo_class_RdKafka_TopicPartition_setPartition, ZEND_ACC_PUBLIC)
	ZEND_ME(RdKafka_TopicPartition, getOffset, arginfo_class_RdKafka_TopicPartition_getOffset, ZEND_ACC_PUBLIC)
	ZEND_ME(RdKafka_TopicPartition, setOffset, arginfo_class_RdKafka_TopicPartition_setOffset, ZEND_ACC_PUBLIC)
	ZEND_ME(RdKafka_TopicPartition, getErr, arginfo_class_RdKafka_TopicPartition_getErr, ZEND_ACC_PUBLIC)
	ZEND_ME(RdKafka_TopicPartition, getMetadata, arginfo_class_RdKafka_TopicPartition_getMetadata, ZEND_ACC_PUBLIC)
	ZEND_ME(RdKafka_TopicPartition, setMetadata, arginfo_class_RdKafka_TopicPartition_setMetadata, ZEND_ACC_PUBLIC)
	ZEND_FE_END
};

static zend_class_entry *register_class_RdKafka_TopicPartition(void)
{
	zend_class_entry ce, *class_entry;

	INIT_NS_CLASS_ENTRY(ce, "RdKafka", "TopicPartition", class_RdKafka_TopicPartition_methods);
	class_entry = zend_register_internal_class_ex(&ce, NULL);

	return class_entry;
}
