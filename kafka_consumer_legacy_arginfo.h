/* This is a generated file, edit the .stub.php file instead.
 * Stub hash: c992beb679f8970a0d6af9fb06592d8c3c4cb232 */

ZEND_BEGIN_ARG_INFO_EX(arginfo_class_RdKafka_KafkaConsumer___construct, 0, 0, 1)
	ZEND_ARG_INFO(0, conf)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_class_RdKafka_KafkaConsumer_assign, 0, 0, 0)
	ZEND_ARG_INFO(0, topic_partitions)
ZEND_END_ARG_INFO()

#if defined(HAS_RD_KAFKA_INCREMENTAL_ASSIGN)
ZEND_BEGIN_ARG_INFO_EX(arginfo_class_RdKafka_KafkaConsumer_incrementalAssign, 0, 0, 1)
	ZEND_ARG_INFO(0, topic_partitions)
ZEND_END_ARG_INFO()
#endif

#if defined(HAS_RD_KAFKA_INCREMENTAL_ASSIGN)
#define arginfo_class_RdKafka_KafkaConsumer_incrementalUnassign arginfo_class_RdKafka_KafkaConsumer_incrementalAssign
#endif

ZEND_BEGIN_ARG_INFO_EX(arginfo_class_RdKafka_KafkaConsumer_getAssignment, 0, 0, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_class_RdKafka_KafkaConsumer_commit, 0, 0, 0)
	ZEND_ARG_INFO(0, message_or_offsets)
ZEND_END_ARG_INFO()

#define arginfo_class_RdKafka_KafkaConsumer_close arginfo_class_RdKafka_KafkaConsumer_getAssignment

#define arginfo_class_RdKafka_KafkaConsumer_commitAsync arginfo_class_RdKafka_KafkaConsumer_commit

ZEND_BEGIN_ARG_INFO_EX(arginfo_class_RdKafka_KafkaConsumer_consume, 0, 0, 1)
	ZEND_ARG_INFO(0, timeout_ms)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_class_RdKafka_KafkaConsumer_subscribe, 0, 0, 1)
	ZEND_ARG_INFO(0, topics)
ZEND_END_ARG_INFO()

#define arginfo_class_RdKafka_KafkaConsumer_getSubscription arginfo_class_RdKafka_KafkaConsumer_getAssignment

#define arginfo_class_RdKafka_KafkaConsumer_unsubscribe arginfo_class_RdKafka_KafkaConsumer_getAssignment

ZEND_BEGIN_ARG_INFO_EX(arginfo_class_RdKafka_KafkaConsumer_getMetadata, 0, 0, 3)
	ZEND_ARG_INFO(0, all_topics)
	ZEND_ARG_INFO(0, only_topic)
	ZEND_ARG_INFO(0, timeout_ms)
ZEND_END_ARG_INFO()

#if defined(HAS_RD_KAFKA_CONTROLLERID)
ZEND_BEGIN_ARG_INFO_EX(arginfo_class_RdKafka_KafkaConsumer_getControllerId, 0, 0, 1)
	ZEND_ARG_INFO(0, timeout_ms)
ZEND_END_ARG_INFO()
#endif

ZEND_BEGIN_ARG_INFO_EX(arginfo_class_RdKafka_KafkaConsumer_newTopic, 0, 0, 1)
	ZEND_ARG_INFO(0, topic_name)
	ZEND_ARG_INFO(0, topic_conf)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_class_RdKafka_KafkaConsumer_getCommittedOffsets, 0, 0, 2)
	ZEND_ARG_INFO(0, topic_partitions)
	ZEND_ARG_INFO(0, timeout_ms)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_class_RdKafka_KafkaConsumer_getOffsetPositions, 0, 0, 1)
	ZEND_ARG_INFO(0, topic_partitions)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_class_RdKafka_KafkaConsumer_queryWatermarkOffsets, 0, 0, 5)
	ZEND_ARG_INFO(0, topic)
	ZEND_ARG_INFO(0, partition)
	ZEND_ARG_INFO(1, low)
	ZEND_ARG_INFO(1, high)
	ZEND_ARG_INFO(0, timeout_ms)
ZEND_END_ARG_INFO()

#define arginfo_class_RdKafka_KafkaConsumer_offsetsForTimes arginfo_class_RdKafka_KafkaConsumer_getCommittedOffsets

#define arginfo_class_RdKafka_KafkaConsumer_pausePartitions arginfo_class_RdKafka_KafkaConsumer_getOffsetPositions

#define arginfo_class_RdKafka_KafkaConsumer_resumePartitions arginfo_class_RdKafka_KafkaConsumer_getOffsetPositions

#define arginfo_class_RdKafka_KafkaConsumer_poll arginfo_class_RdKafka_KafkaConsumer_consume

#if defined(HAS_RD_KAFKA_OAUTHBEARER)
ZEND_BEGIN_ARG_INFO_EX(arginfo_class_RdKafka_KafkaConsumer_oauthbearerSetToken, 0, 0, 3)
	ZEND_ARG_INFO(0, token_value)
	ZEND_ARG_INFO(0, lifetime_ms)
	ZEND_ARG_INFO(0, principal_name)
	ZEND_ARG_INFO(0, extensions)
ZEND_END_ARG_INFO()
#endif

#if defined(HAS_RD_KAFKA_OAUTHBEARER)
ZEND_BEGIN_ARG_INFO_EX(arginfo_class_RdKafka_KafkaConsumer_oauthbearerSetTokenFailure, 0, 0, 1)
	ZEND_ARG_INFO(0, error)
ZEND_END_ARG_INFO()
#endif


ZEND_METHOD(RdKafka_KafkaConsumer, __construct);
ZEND_METHOD(RdKafka_KafkaConsumer, assign);
#if defined(HAS_RD_KAFKA_INCREMENTAL_ASSIGN)
ZEND_METHOD(RdKafka_KafkaConsumer, incrementalAssign);
#endif
#if defined(HAS_RD_KAFKA_INCREMENTAL_ASSIGN)
ZEND_METHOD(RdKafka_KafkaConsumer, incrementalUnassign);
#endif
ZEND_METHOD(RdKafka_KafkaConsumer, getAssignment);
ZEND_METHOD(RdKafka_KafkaConsumer, commit);
ZEND_METHOD(RdKafka_KafkaConsumer, close);
ZEND_METHOD(RdKafka_KafkaConsumer, commitAsync);
ZEND_METHOD(RdKafka_KafkaConsumer, consume);
ZEND_METHOD(RdKafka_KafkaConsumer, subscribe);
ZEND_METHOD(RdKafka_KafkaConsumer, getSubscription);
ZEND_METHOD(RdKafka_KafkaConsumer, unsubscribe);
ZEND_METHOD(RdKafka_KafkaConsumer, getMetadata);
#if defined(HAS_RD_KAFKA_CONTROLLERID)
ZEND_METHOD(RdKafka_KafkaConsumer, getControllerId);
#endif
ZEND_METHOD(RdKafka_KafkaConsumer, newTopic);
ZEND_METHOD(RdKafka_KafkaConsumer, getCommittedOffsets);
ZEND_METHOD(RdKafka_KafkaConsumer, getOffsetPositions);
ZEND_METHOD(RdKafka_KafkaConsumer, queryWatermarkOffsets);
ZEND_METHOD(RdKafka_KafkaConsumer, offsetsForTimes);
ZEND_METHOD(RdKafka_KafkaConsumer, pausePartitions);
ZEND_METHOD(RdKafka_KafkaConsumer, resumePartitions);
ZEND_METHOD(RdKafka_KafkaConsumer, poll);
#if defined(HAS_RD_KAFKA_OAUTHBEARER)
ZEND_METHOD(RdKafka_KafkaConsumer, oauthbearerSetToken);
#endif
#if defined(HAS_RD_KAFKA_OAUTHBEARER)
ZEND_METHOD(RdKafka_KafkaConsumer, oauthbearerSetTokenFailure);
#endif


static const zend_function_entry class_RdKafka_KafkaConsumer_methods[] = {
	ZEND_ME(RdKafka_KafkaConsumer, __construct, arginfo_class_RdKafka_KafkaConsumer___construct, ZEND_ACC_PUBLIC)
	ZEND_ME(RdKafka_KafkaConsumer, assign, arginfo_class_RdKafka_KafkaConsumer_assign, ZEND_ACC_PUBLIC)
#if defined(HAS_RD_KAFKA_INCREMENTAL_ASSIGN)
	ZEND_ME(RdKafka_KafkaConsumer, incrementalAssign, arginfo_class_RdKafka_KafkaConsumer_incrementalAssign, ZEND_ACC_PUBLIC)
#endif
#if defined(HAS_RD_KAFKA_INCREMENTAL_ASSIGN)
	ZEND_ME(RdKafka_KafkaConsumer, incrementalUnassign, arginfo_class_RdKafka_KafkaConsumer_incrementalUnassign, ZEND_ACC_PUBLIC)
#endif
	ZEND_ME(RdKafka_KafkaConsumer, getAssignment, arginfo_class_RdKafka_KafkaConsumer_getAssignment, ZEND_ACC_PUBLIC)
	ZEND_ME(RdKafka_KafkaConsumer, commit, arginfo_class_RdKafka_KafkaConsumer_commit, ZEND_ACC_PUBLIC)
	ZEND_ME(RdKafka_KafkaConsumer, close, arginfo_class_RdKafka_KafkaConsumer_close, ZEND_ACC_PUBLIC)
	ZEND_ME(RdKafka_KafkaConsumer, commitAsync, arginfo_class_RdKafka_KafkaConsumer_commitAsync, ZEND_ACC_PUBLIC)
	ZEND_ME(RdKafka_KafkaConsumer, consume, arginfo_class_RdKafka_KafkaConsumer_consume, ZEND_ACC_PUBLIC)
	ZEND_ME(RdKafka_KafkaConsumer, subscribe, arginfo_class_RdKafka_KafkaConsumer_subscribe, ZEND_ACC_PUBLIC)
	ZEND_ME(RdKafka_KafkaConsumer, getSubscription, arginfo_class_RdKafka_KafkaConsumer_getSubscription, ZEND_ACC_PUBLIC)
	ZEND_ME(RdKafka_KafkaConsumer, unsubscribe, arginfo_class_RdKafka_KafkaConsumer_unsubscribe, ZEND_ACC_PUBLIC)
	ZEND_ME(RdKafka_KafkaConsumer, getMetadata, arginfo_class_RdKafka_KafkaConsumer_getMetadata, ZEND_ACC_PUBLIC)
#if defined(HAS_RD_KAFKA_CONTROLLERID)
	ZEND_ME(RdKafka_KafkaConsumer, getControllerId, arginfo_class_RdKafka_KafkaConsumer_getControllerId, ZEND_ACC_PUBLIC)
#endif
	ZEND_ME(RdKafka_KafkaConsumer, newTopic, arginfo_class_RdKafka_KafkaConsumer_newTopic, ZEND_ACC_PUBLIC)
	ZEND_ME(RdKafka_KafkaConsumer, getCommittedOffsets, arginfo_class_RdKafka_KafkaConsumer_getCommittedOffsets, ZEND_ACC_PUBLIC)
	ZEND_ME(RdKafka_KafkaConsumer, getOffsetPositions, arginfo_class_RdKafka_KafkaConsumer_getOffsetPositions, ZEND_ACC_PUBLIC)
	ZEND_ME(RdKafka_KafkaConsumer, queryWatermarkOffsets, arginfo_class_RdKafka_KafkaConsumer_queryWatermarkOffsets, ZEND_ACC_PUBLIC)
	ZEND_ME(RdKafka_KafkaConsumer, offsetsForTimes, arginfo_class_RdKafka_KafkaConsumer_offsetsForTimes, ZEND_ACC_PUBLIC)
	ZEND_ME(RdKafka_KafkaConsumer, pausePartitions, arginfo_class_RdKafka_KafkaConsumer_pausePartitions, ZEND_ACC_PUBLIC)
	ZEND_ME(RdKafka_KafkaConsumer, resumePartitions, arginfo_class_RdKafka_KafkaConsumer_resumePartitions, ZEND_ACC_PUBLIC)
	ZEND_ME(RdKafka_KafkaConsumer, poll, arginfo_class_RdKafka_KafkaConsumer_poll, ZEND_ACC_PUBLIC)
#if defined(HAS_RD_KAFKA_OAUTHBEARER)
	ZEND_ME(RdKafka_KafkaConsumer, oauthbearerSetToken, arginfo_class_RdKafka_KafkaConsumer_oauthbearerSetToken, ZEND_ACC_PUBLIC)
#endif
#if defined(HAS_RD_KAFKA_OAUTHBEARER)
	ZEND_ME(RdKafka_KafkaConsumer, oauthbearerSetTokenFailure, arginfo_class_RdKafka_KafkaConsumer_oauthbearerSetTokenFailure, ZEND_ACC_PUBLIC)
#endif
	ZEND_FE_END
};

static zend_class_entry *register_class_RdKafka_KafkaConsumer(void)
{
	zend_class_entry ce, *class_entry;

	INIT_NS_CLASS_ENTRY(ce, "RdKafka", "KafkaConsumer", class_RdKafka_KafkaConsumer_methods);
	class_entry = zend_register_internal_class_ex(&ce, NULL);

	zval property_error_cb_default_value;
	ZVAL_NULL(&property_error_cb_default_value);
	zend_string *property_error_cb_name = zend_string_init("error_cb", sizeof("error_cb") - 1, 1);
	zend_declare_property_ex(class_entry, property_error_cb_name, &property_error_cb_default_value, ZEND_ACC_PRIVATE, NULL);
	zend_string_release(property_error_cb_name);

	zval property_rebalance_cb_default_value;
	ZVAL_NULL(&property_rebalance_cb_default_value);
	zend_string *property_rebalance_cb_name = zend_string_init("rebalance_cb", sizeof("rebalance_cb") - 1, 1);
	zend_declare_property_ex(class_entry, property_rebalance_cb_name, &property_rebalance_cb_default_value, ZEND_ACC_PRIVATE, NULL);
	zend_string_release(property_rebalance_cb_name);

	zval property_dr_msg_cb_default_value;
	ZVAL_NULL(&property_dr_msg_cb_default_value);
	zend_string *property_dr_msg_cb_name = zend_string_init("dr_msg_cb", sizeof("dr_msg_cb") - 1, 1);
	zend_declare_property_ex(class_entry, property_dr_msg_cb_name, &property_dr_msg_cb_default_value, ZEND_ACC_PRIVATE, NULL);
	zend_string_release(property_dr_msg_cb_name);

	return class_entry;
}
