/* This is a generated file, edit the .stub.php file instead.
 * Stub hash: d583887405bbe376ebb4f502921cc4021fe9b8e4 */

#if (PHP_VERSION_ID >= 80100)
ZEND_BEGIN_ARG_WITH_TENTATIVE_RETURN_TYPE_INFO_EX(arginfo_class_RdKafka_Admin_AdminOptions_setRequestTimeout, 0, 1, IS_VOID, 0)
#else
ZEND_BEGIN_ARG_INFO_EX(arginfo_class_RdKafka_Admin_AdminOptions_setRequestTimeout, 0, 0, 1)
#endif
	ZEND_ARG_TYPE_INFO(0, timeout_ms, IS_LONG, 0)
ZEND_END_ARG_INFO()

#define arginfo_class_RdKafka_Admin_AdminOptions_setOperationTimeout arginfo_class_RdKafka_Admin_AdminOptions_setRequestTimeout

#if (PHP_VERSION_ID >= 80100)
ZEND_BEGIN_ARG_WITH_TENTATIVE_RETURN_TYPE_INFO_EX(arginfo_class_RdKafka_Admin_AdminOptions_setValidateOnly, 0, 1, IS_VOID, 0)
#else
ZEND_BEGIN_ARG_INFO_EX(arginfo_class_RdKafka_Admin_AdminOptions_setValidateOnly, 0, 0, 1)
#endif
	ZEND_ARG_TYPE_INFO(0, validate_only, _IS_BOOL, 0)
ZEND_END_ARG_INFO()

#if (PHP_VERSION_ID >= 80100)
ZEND_BEGIN_ARG_WITH_TENTATIVE_RETURN_TYPE_INFO_EX(arginfo_class_RdKafka_Admin_AdminOptions_setBrokerId, 0, 1, IS_VOID, 0)
#else
ZEND_BEGIN_ARG_INFO_EX(arginfo_class_RdKafka_Admin_AdminOptions_setBrokerId, 0, 0, 1)
#endif
	ZEND_ARG_TYPE_INFO(0, broker_id, IS_LONG, 0)
ZEND_END_ARG_INFO()

#if defined(HAS_RD_KAFKA_DESCRIBE_TOPICS)
#if (PHP_VERSION_ID >= 80100)
ZEND_BEGIN_ARG_WITH_TENTATIVE_RETURN_TYPE_INFO_EX(arginfo_class_RdKafka_Admin_AdminOptions_setIncludeAuthorizedOperations, 0, 1, IS_VOID, 0)
#else
ZEND_BEGIN_ARG_INFO_EX(arginfo_class_RdKafka_Admin_AdminOptions_setIncludeAuthorizedOperations, 0, 0, 1)
#endif
	ZEND_ARG_TYPE_INFO(0, include, _IS_BOOL, 0)
ZEND_END_ARG_INFO()
#endif

ZEND_BEGIN_ARG_INFO_EX(arginfo_class_RdKafka_Admin_NewTopic___construct, 0, 0, 3)
	ZEND_ARG_TYPE_INFO(0, topic, IS_STRING, 0)
	ZEND_ARG_TYPE_INFO(0, num_partitions, IS_LONG, 0)
	ZEND_ARG_TYPE_INFO(0, replication_factor, IS_LONG, 0)
ZEND_END_ARG_INFO()

#if (PHP_VERSION_ID >= 80100)
ZEND_BEGIN_ARG_WITH_TENTATIVE_RETURN_TYPE_INFO_EX(arginfo_class_RdKafka_Admin_NewTopic_setReplicaAssignment, 0, 2, IS_VOID, 0)
#else
ZEND_BEGIN_ARG_INFO_EX(arginfo_class_RdKafka_Admin_NewTopic_setReplicaAssignment, 0, 0, 2)
#endif
	ZEND_ARG_TYPE_INFO(0, partition, IS_LONG, 0)
	ZEND_ARG_TYPE_INFO(0, broker_ids, IS_ARRAY, 0)
ZEND_END_ARG_INFO()

#if (PHP_VERSION_ID >= 80100)
ZEND_BEGIN_ARG_WITH_TENTATIVE_RETURN_TYPE_INFO_EX(arginfo_class_RdKafka_Admin_NewTopic_setConfig, 0, 2, IS_VOID, 0)
#else
ZEND_BEGIN_ARG_INFO_EX(arginfo_class_RdKafka_Admin_NewTopic_setConfig, 0, 0, 2)
#endif
	ZEND_ARG_TYPE_INFO(0, name, IS_STRING, 0)
	ZEND_ARG_TYPE_INFO(0, value, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_class_RdKafka_Admin_DeleteTopic___construct, 0, 0, 1)
	ZEND_ARG_TYPE_INFO(0, topic, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_class_RdKafka_Admin_NewPartitions___construct, 0, 0, 2)
	ZEND_ARG_TYPE_INFO(0, topic, IS_STRING, 0)
	ZEND_ARG_TYPE_INFO(0, new_total_count, IS_LONG, 0)
ZEND_END_ARG_INFO()

#if (PHP_VERSION_ID >= 80100)
ZEND_BEGIN_ARG_WITH_TENTATIVE_RETURN_TYPE_INFO_EX(arginfo_class_RdKafka_Admin_NewPartitions_setReplicaAssignment, 0, 2, IS_VOID, 0)
#else
ZEND_BEGIN_ARG_INFO_EX(arginfo_class_RdKafka_Admin_NewPartitions_setReplicaAssignment, 0, 0, 2)
#endif
	ZEND_ARG_TYPE_INFO(0, new_partition_index, IS_LONG, 0)
	ZEND_ARG_TYPE_INFO(0, broker_ids, IS_ARRAY, 0)
ZEND_END_ARG_INFO()

#if (PHP_VERSION_ID >= 80100)
ZEND_BEGIN_ARG_WITH_TENTATIVE_RETURN_TYPE_INFO_EX(arginfo_class_RdKafka_Admin_TopicResult_getError, 0, 0, IS_LONG, 0)
#else
ZEND_BEGIN_ARG_INFO_EX(arginfo_class_RdKafka_Admin_TopicResult_getError, 0, 0, 0)
#endif
ZEND_END_ARG_INFO()

#if (PHP_VERSION_ID >= 80100)
ZEND_BEGIN_ARG_WITH_TENTATIVE_RETURN_TYPE_INFO_EX(arginfo_class_RdKafka_Admin_TopicResult_getErrorString, 0, 0, IS_STRING, 1)
#else
ZEND_BEGIN_ARG_INFO_EX(arginfo_class_RdKafka_Admin_TopicResult_getErrorString, 0, 0, 0)
#endif
ZEND_END_ARG_INFO()

#if (PHP_VERSION_ID >= 80100)
ZEND_BEGIN_ARG_WITH_TENTATIVE_RETURN_TYPE_INFO_EX(arginfo_class_RdKafka_Admin_TopicResult_getName, 0, 0, IS_STRING, 0)
#else
ZEND_BEGIN_ARG_INFO_EX(arginfo_class_RdKafka_Admin_TopicResult_getName, 0, 0, 0)
#endif
ZEND_END_ARG_INFO()

ZEND_METHOD(RdKafka_Admin_AdminOptions, setRequestTimeout);
ZEND_METHOD(RdKafka_Admin_AdminOptions, setOperationTimeout);
ZEND_METHOD(RdKafka_Admin_AdminOptions, setValidateOnly);
ZEND_METHOD(RdKafka_Admin_AdminOptions, setBrokerId);
#if defined(HAS_RD_KAFKA_DESCRIBE_TOPICS)
ZEND_METHOD(RdKafka_Admin_AdminOptions, setIncludeAuthorizedOperations);
#endif
ZEND_METHOD(RdKafka_Admin_NewTopic, __construct);
ZEND_METHOD(RdKafka_Admin_NewTopic, setReplicaAssignment);
ZEND_METHOD(RdKafka_Admin_NewTopic, setConfig);
ZEND_METHOD(RdKafka_Admin_DeleteTopic, __construct);
ZEND_METHOD(RdKafka_Admin_NewPartitions, __construct);
ZEND_METHOD(RdKafka_Admin_NewPartitions, setReplicaAssignment);
ZEND_METHOD(RdKafka_Admin_TopicResult, getError);
ZEND_METHOD(RdKafka_Admin_TopicResult, getErrorString);
ZEND_METHOD(RdKafka_Admin_TopicResult, getName);

static const zend_function_entry class_RdKafka_Admin_AdminOptions_methods[] = {
	ZEND_ME(RdKafka_Admin_AdminOptions, setRequestTimeout, arginfo_class_RdKafka_Admin_AdminOptions_setRequestTimeout, ZEND_ACC_PUBLIC)
	ZEND_ME(RdKafka_Admin_AdminOptions, setOperationTimeout, arginfo_class_RdKafka_Admin_AdminOptions_setOperationTimeout, ZEND_ACC_PUBLIC)
	ZEND_ME(RdKafka_Admin_AdminOptions, setValidateOnly, arginfo_class_RdKafka_Admin_AdminOptions_setValidateOnly, ZEND_ACC_PUBLIC)
	ZEND_ME(RdKafka_Admin_AdminOptions, setBrokerId, arginfo_class_RdKafka_Admin_AdminOptions_setBrokerId, ZEND_ACC_PUBLIC)
#if defined(HAS_RD_KAFKA_DESCRIBE_TOPICS)
	ZEND_ME(RdKafka_Admin_AdminOptions, setIncludeAuthorizedOperations, arginfo_class_RdKafka_Admin_AdminOptions_setIncludeAuthorizedOperations, ZEND_ACC_PUBLIC)
#endif
	ZEND_FE_END
};

static const zend_function_entry class_RdKafka_Admin_NewTopic_methods[] = {
	ZEND_ME(RdKafka_Admin_NewTopic, __construct, arginfo_class_RdKafka_Admin_NewTopic___construct, ZEND_ACC_PUBLIC)
	ZEND_ME(RdKafka_Admin_NewTopic, setReplicaAssignment, arginfo_class_RdKafka_Admin_NewTopic_setReplicaAssignment, ZEND_ACC_PUBLIC)
	ZEND_ME(RdKafka_Admin_NewTopic, setConfig, arginfo_class_RdKafka_Admin_NewTopic_setConfig, ZEND_ACC_PUBLIC)
	ZEND_FE_END
};

static const zend_function_entry class_RdKafka_Admin_DeleteTopic_methods[] = {
	ZEND_ME(RdKafka_Admin_DeleteTopic, __construct, arginfo_class_RdKafka_Admin_DeleteTopic___construct, ZEND_ACC_PUBLIC)
	ZEND_FE_END
};

static const zend_function_entry class_RdKafka_Admin_NewPartitions_methods[] = {
	ZEND_ME(RdKafka_Admin_NewPartitions, __construct, arginfo_class_RdKafka_Admin_NewPartitions___construct, ZEND_ACC_PUBLIC)
	ZEND_ME(RdKafka_Admin_NewPartitions, setReplicaAssignment, arginfo_class_RdKafka_Admin_NewPartitions_setReplicaAssignment, ZEND_ACC_PUBLIC)
	ZEND_FE_END
};

static const zend_function_entry class_RdKafka_Admin_TopicResult_methods[] = {
	ZEND_ME(RdKafka_Admin_TopicResult, getError, arginfo_class_RdKafka_Admin_TopicResult_getError, ZEND_ACC_PUBLIC)
	ZEND_ME(RdKafka_Admin_TopicResult, getErrorString, arginfo_class_RdKafka_Admin_TopicResult_getErrorString, ZEND_ACC_PUBLIC)
	ZEND_ME(RdKafka_Admin_TopicResult, getName, arginfo_class_RdKafka_Admin_TopicResult_getName, ZEND_ACC_PUBLIC)
	ZEND_FE_END
};

static zend_class_entry *register_class_RdKafka_Admin_AdminOptions(void)
{
	zend_class_entry ce, *class_entry;

	INIT_NS_CLASS_ENTRY(ce, "RdKafka\\Admin", "AdminOptions", class_RdKafka_Admin_AdminOptions_methods);
#if (PHP_VERSION_ID >= 80400)
	class_entry = zend_register_internal_class_with_flags(&ce, NULL, 0);
#else
	class_entry = zend_register_internal_class_ex(&ce, NULL);
#endif

	return class_entry;
}

static zend_class_entry *register_class_RdKafka_Admin_NewTopic(void)
{
	zend_class_entry ce, *class_entry;

	INIT_NS_CLASS_ENTRY(ce, "RdKafka\\Admin", "NewTopic", class_RdKafka_Admin_NewTopic_methods);
#if (PHP_VERSION_ID >= 80400)
	class_entry = zend_register_internal_class_with_flags(&ce, NULL, 0);
#else
	class_entry = zend_register_internal_class_ex(&ce, NULL);
#endif

	return class_entry;
}

static zend_class_entry *register_class_RdKafka_Admin_DeleteTopic(void)
{
	zend_class_entry ce, *class_entry;

	INIT_NS_CLASS_ENTRY(ce, "RdKafka\\Admin", "DeleteTopic", class_RdKafka_Admin_DeleteTopic_methods);
#if (PHP_VERSION_ID >= 80400)
	class_entry = zend_register_internal_class_with_flags(&ce, NULL, 0);
#else
	class_entry = zend_register_internal_class_ex(&ce, NULL);
#endif

	return class_entry;
}

static zend_class_entry *register_class_RdKafka_Admin_NewPartitions(void)
{
	zend_class_entry ce, *class_entry;

	INIT_NS_CLASS_ENTRY(ce, "RdKafka\\Admin", "NewPartitions", class_RdKafka_Admin_NewPartitions_methods);
#if (PHP_VERSION_ID >= 80400)
	class_entry = zend_register_internal_class_with_flags(&ce, NULL, 0);
#else
	class_entry = zend_register_internal_class_ex(&ce, NULL);
#endif

	return class_entry;
}

static zend_class_entry *register_class_RdKafka_Admin_TopicResult(void)
{
	zend_class_entry ce, *class_entry;

	INIT_NS_CLASS_ENTRY(ce, "RdKafka\\Admin", "TopicResult", class_RdKafka_Admin_TopicResult_methods);
#if (PHP_VERSION_ID >= 80400)
	class_entry = zend_register_internal_class_with_flags(&ce, NULL, 0);
#else
	class_entry = zend_register_internal_class_ex(&ce, NULL);
#endif

	zval property_error_default_value;
	ZVAL_UNDEF(&property_error_default_value);
	zend_string *property_error_name = zend_string_init("error", sizeof("error") - 1, 1);
	zend_declare_typed_property(class_entry, property_error_name, &property_error_default_value, ZEND_ACC_PUBLIC, NULL, (zend_type) ZEND_TYPE_INIT_MASK(MAY_BE_LONG));
	zend_string_release(property_error_name);

	zval property_error_string_default_value;
	ZVAL_UNDEF(&property_error_string_default_value);
	zend_string *property_error_string_name = zend_string_init("error_string", sizeof("error_string") - 1, 1);
	zend_declare_typed_property(class_entry, property_error_string_name, &property_error_string_default_value, ZEND_ACC_PUBLIC, NULL, (zend_type) ZEND_TYPE_INIT_MASK(MAY_BE_STRING|MAY_BE_NULL));
	zend_string_release(property_error_string_name);

	zval property_name_default_value;
	ZVAL_UNDEF(&property_name_default_value);
	zend_string *property_name_name = zend_string_init("name", sizeof("name") - 1, 1);
	zend_declare_typed_property(class_entry, property_name_name, &property_name_default_value, ZEND_ACC_PUBLIC, NULL, (zend_type) ZEND_TYPE_INIT_MASK(MAY_BE_STRING));
	zend_string_release(property_name_name);

	return class_entry;
}

#if defined(HAS_RD_KAFKA_DESCRIBE_TOPICS)
static zend_class_entry *register_class_RdKafka_Admin_Node(void)
{
	zend_class_entry ce, *class_entry;

	INIT_NS_CLASS_ENTRY(ce, "RdKafka\\Admin", "Node", NULL);
#if (PHP_VERSION_ID >= 80400)
	class_entry = zend_register_internal_class_with_flags(&ce, NULL, 0);
#else
	class_entry = zend_register_internal_class_ex(&ce, NULL);
#endif

	zval property_id_default_value;
	ZVAL_UNDEF(&property_id_default_value);
	zend_string *property_id_name = zend_string_init("id", sizeof("id") - 1, 1);
	zend_declare_typed_property(class_entry, property_id_name, &property_id_default_value, ZEND_ACC_PUBLIC, NULL, (zend_type) ZEND_TYPE_INIT_MASK(MAY_BE_LONG));
	zend_string_release(property_id_name);

	zval property_host_default_value;
	ZVAL_UNDEF(&property_host_default_value);
	zend_string *property_host_name = zend_string_init("host", sizeof("host") - 1, 1);
	zend_declare_typed_property(class_entry, property_host_name, &property_host_default_value, ZEND_ACC_PUBLIC, NULL, (zend_type) ZEND_TYPE_INIT_MASK(MAY_BE_STRING));
	zend_string_release(property_host_name);

	zval property_port_default_value;
	ZVAL_UNDEF(&property_port_default_value);
	zend_string *property_port_name = zend_string_init("port", sizeof("port") - 1, 1);
	zend_declare_typed_property(class_entry, property_port_name, &property_port_default_value, ZEND_ACC_PUBLIC, NULL, (zend_type) ZEND_TYPE_INIT_MASK(MAY_BE_LONG));
	zend_string_release(property_port_name);

	zval property_rack_default_value;
	ZVAL_UNDEF(&property_rack_default_value);
	zend_string *property_rack_name = zend_string_init("rack", sizeof("rack") - 1, 1);
	zend_declare_typed_property(class_entry, property_rack_name, &property_rack_default_value, ZEND_ACC_PUBLIC, NULL, (zend_type) ZEND_TYPE_INIT_MASK(MAY_BE_STRING|MAY_BE_NULL));
	zend_string_release(property_rack_name);

	return class_entry;
}
#endif

#if defined(HAS_RD_KAFKA_DESCRIBE_TOPICS)
static zend_class_entry *register_class_RdKafka_Admin_TopicPartitionInfo(void)
{
	zend_class_entry ce, *class_entry;

	INIT_NS_CLASS_ENTRY(ce, "RdKafka\\Admin", "TopicPartitionInfo", NULL);
#if (PHP_VERSION_ID >= 80400)
	class_entry = zend_register_internal_class_with_flags(&ce, NULL, 0);
#else
	class_entry = zend_register_internal_class_ex(&ce, NULL);
#endif

	zval property_partition_default_value;
	ZVAL_UNDEF(&property_partition_default_value);
	zend_string *property_partition_name = zend_string_init("partition", sizeof("partition") - 1, 1);
	zend_declare_typed_property(class_entry, property_partition_name, &property_partition_default_value, ZEND_ACC_PUBLIC, NULL, (zend_type) ZEND_TYPE_INIT_MASK(MAY_BE_LONG));
	zend_string_release(property_partition_name);

	zval property_leader_default_value;
	ZVAL_UNDEF(&property_leader_default_value);
	zend_string *property_leader_name = zend_string_init("leader", sizeof("leader") - 1, 1);
	zend_string *property_leader_class_RdKafka_Admin_Node = zend_string_init("RdKafka\\Admin\\\116ode", sizeof("RdKafka\\Admin\\\116ode")-1, 1);
	zend_declare_typed_property(class_entry, property_leader_name, &property_leader_default_value, ZEND_ACC_PUBLIC, NULL, (zend_type) ZEND_TYPE_INIT_CLASS(property_leader_class_RdKafka_Admin_Node, 0, MAY_BE_NULL));
	zend_string_release(property_leader_name);

	zval property_isr_default_value;
	ZVAL_UNDEF(&property_isr_default_value);
	zend_string *property_isr_name = zend_string_init("isr", sizeof("isr") - 1, 1);
	zend_declare_typed_property(class_entry, property_isr_name, &property_isr_default_value, ZEND_ACC_PUBLIC, NULL, (zend_type) ZEND_TYPE_INIT_MASK(MAY_BE_ARRAY));
	zend_string_release(property_isr_name);

	zval property_replicas_default_value;
	ZVAL_UNDEF(&property_replicas_default_value);
	zend_string *property_replicas_name = zend_string_init("replicas", sizeof("replicas") - 1, 1);
	zend_declare_typed_property(class_entry, property_replicas_name, &property_replicas_default_value, ZEND_ACC_PUBLIC, NULL, (zend_type) ZEND_TYPE_INIT_MASK(MAY_BE_ARRAY));
	zend_string_release(property_replicas_name);

	return class_entry;
}
#endif

#if defined(HAS_RD_KAFKA_DESCRIBE_TOPICS)
static zend_class_entry *register_class_RdKafka_Admin_TopicDescription(void)
{
	zend_class_entry ce, *class_entry;

	INIT_NS_CLASS_ENTRY(ce, "RdKafka\\Admin", "TopicDescription", NULL);
#if (PHP_VERSION_ID >= 80400)
	class_entry = zend_register_internal_class_with_flags(&ce, NULL, 0);
#else
	class_entry = zend_register_internal_class_ex(&ce, NULL);
#endif

	zval property_name_default_value;
	ZVAL_UNDEF(&property_name_default_value);
	zend_string *property_name_name = zend_string_init("name", sizeof("name") - 1, 1);
	zend_declare_typed_property(class_entry, property_name_name, &property_name_default_value, ZEND_ACC_PUBLIC, NULL, (zend_type) ZEND_TYPE_INIT_MASK(MAY_BE_STRING));
	zend_string_release(property_name_name);

	zval property_topic_id_default_value;
	ZVAL_UNDEF(&property_topic_id_default_value);
	zend_string *property_topic_id_name = zend_string_init("topic_id", sizeof("topic_id") - 1, 1);
	zend_declare_typed_property(class_entry, property_topic_id_name, &property_topic_id_default_value, ZEND_ACC_PUBLIC, NULL, (zend_type) ZEND_TYPE_INIT_MASK(MAY_BE_STRING|MAY_BE_NULL));
	zend_string_release(property_topic_id_name);

	zval property_is_internal_default_value;
	ZVAL_UNDEF(&property_is_internal_default_value);
	zend_string *property_is_internal_name = zend_string_init("is_internal", sizeof("is_internal") - 1, 1);
	zend_declare_typed_property(class_entry, property_is_internal_name, &property_is_internal_default_value, ZEND_ACC_PUBLIC, NULL, (zend_type) ZEND_TYPE_INIT_MASK(MAY_BE_BOOL));
	zend_string_release(property_is_internal_name);

	zval property_error_default_value;
	ZVAL_UNDEF(&property_error_default_value);
	zend_string *property_error_name = zend_string_init("error", sizeof("error") - 1, 1);
	zend_declare_typed_property(class_entry, property_error_name, &property_error_default_value, ZEND_ACC_PUBLIC, NULL, (zend_type) ZEND_TYPE_INIT_MASK(MAY_BE_LONG));
	zend_string_release(property_error_name);

	zval property_error_string_default_value;
	ZVAL_UNDEF(&property_error_string_default_value);
	zend_string *property_error_string_name = zend_string_init("error_string", sizeof("error_string") - 1, 1);
	zend_declare_typed_property(class_entry, property_error_string_name, &property_error_string_default_value, ZEND_ACC_PUBLIC, NULL, (zend_type) ZEND_TYPE_INIT_MASK(MAY_BE_STRING|MAY_BE_NULL));
	zend_string_release(property_error_string_name);

	zval property_partitions_default_value;
	ZVAL_UNDEF(&property_partitions_default_value);
	zend_string *property_partitions_name = zend_string_init("partitions", sizeof("partitions") - 1, 1);
	zend_declare_typed_property(class_entry, property_partitions_name, &property_partitions_default_value, ZEND_ACC_PUBLIC, NULL, (zend_type) ZEND_TYPE_INIT_MASK(MAY_BE_ARRAY));
	zend_string_release(property_partitions_name);

	return class_entry;
}
#endif
