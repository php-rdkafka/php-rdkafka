/* This is a generated file, edit the .stub.php file instead.
 * Stub hash: 3b18492a98cb2d3fa8f7f36c92abbd48e6d2d9de */

#if (PHP_VERSION_ID >= 80100)
ZEND_BEGIN_ARG_WITH_TENTATIVE_RETURN_TYPE_INFO_EX(arginfo_class_RdKafka_Message_errstr, 0, 0, IS_STRING, 1)
#else
ZEND_BEGIN_ARG_INFO_EX(arginfo_class_RdKafka_Message_errstr, 0, 0, 0)
#endif
ZEND_END_ARG_INFO()

#if (PHP_VERSION_ID >= 80100)
ZEND_BEGIN_ARG_WITH_TENTATIVE_RETURN_TYPE_INFO_EX(arginfo_class_RdKafka_Message_getHeaderPairs, 0, 0, IS_ARRAY, 0)
#else
ZEND_BEGIN_ARG_INFO_EX(arginfo_class_RdKafka_Message_getHeaderPairs, 0, 0, 0)
#endif
ZEND_END_ARG_INFO()

ZEND_METHOD(RdKafka_Message, errstr);
ZEND_METHOD(RdKafka_Message, getHeaderPairs);

static const zend_function_entry class_RdKafka_Message_methods[] = {
	ZEND_ME(RdKafka_Message, errstr, arginfo_class_RdKafka_Message_errstr, ZEND_ACC_PUBLIC)
	ZEND_ME(RdKafka_Message, getHeaderPairs, arginfo_class_RdKafka_Message_getHeaderPairs, ZEND_ACC_PUBLIC)
	ZEND_FE_END
};

static zend_class_entry *register_class_RdKafka_Message(void)
{
	zend_class_entry ce, *class_entry;

	INIT_NS_CLASS_ENTRY(ce, "RdKafka", "Message", class_RdKafka_Message_methods);
#if (PHP_VERSION_ID >= 80400)
	class_entry = zend_register_internal_class_with_flags(&ce, NULL, 0);
#else
	class_entry = zend_register_internal_class_ex(&ce, NULL);
#endif

	zval property_err_default_value;
	ZVAL_UNDEF(&property_err_default_value);
	zend_string *property_err_name = zend_string_init("err", sizeof("err") - 1, 1);
	zend_declare_typed_property(class_entry, property_err_name, &property_err_default_value, ZEND_ACC_PUBLIC, NULL, (zend_type) ZEND_TYPE_INIT_MASK(MAY_BE_LONG));
	zend_string_release(property_err_name);

	zval property_topic_name_default_value;
	ZVAL_NULL(&property_topic_name_default_value);
	zend_string *property_topic_name_name = zend_string_init("topic_name", sizeof("topic_name") - 1, 1);
	zend_declare_typed_property(class_entry, property_topic_name_name, &property_topic_name_default_value, ZEND_ACC_PUBLIC, NULL, (zend_type) ZEND_TYPE_INIT_MASK(MAY_BE_STRING|MAY_BE_NULL));
	zend_string_release(property_topic_name_name);

	zval property_timestamp_default_value;
	ZVAL_NULL(&property_timestamp_default_value);
	zend_string *property_timestamp_name = zend_string_init("timestamp", sizeof("timestamp") - 1, 1);
	zend_declare_typed_property(class_entry, property_timestamp_name, &property_timestamp_default_value, ZEND_ACC_PUBLIC, NULL, (zend_type) ZEND_TYPE_INIT_MASK(MAY_BE_LONG|MAY_BE_NULL));
	zend_string_release(property_timestamp_name);

	zval property_partition_default_value;
	ZVAL_UNDEF(&property_partition_default_value);
	zend_string *property_partition_name = zend_string_init("partition", sizeof("partition") - 1, 1);
	zend_declare_typed_property(class_entry, property_partition_name, &property_partition_default_value, ZEND_ACC_PUBLIC, NULL, (zend_type) ZEND_TYPE_INIT_MASK(MAY_BE_LONG));
	zend_string_release(property_partition_name);

	zval property_payload_default_value;
	ZVAL_NULL(&property_payload_default_value);
	zend_string *property_payload_name = zend_string_init("payload", sizeof("payload") - 1, 1);
	zend_declare_typed_property(class_entry, property_payload_name, &property_payload_default_value, ZEND_ACC_PUBLIC, NULL, (zend_type) ZEND_TYPE_INIT_MASK(MAY_BE_STRING|MAY_BE_NULL));
	zend_string_release(property_payload_name);

	zval property_len_default_value;
	ZVAL_NULL(&property_len_default_value);
	zend_string *property_len_name = zend_string_init("len", sizeof("len") - 1, 1);
	zend_declare_typed_property(class_entry, property_len_name, &property_len_default_value, ZEND_ACC_PUBLIC, NULL, (zend_type) ZEND_TYPE_INIT_MASK(MAY_BE_LONG|MAY_BE_NULL));
	zend_string_release(property_len_name);

	zval property_key_default_value;
	ZVAL_NULL(&property_key_default_value);
	zend_string *property_key_name = zend_string_init("key", sizeof("key") - 1, 1);
	zend_declare_typed_property(class_entry, property_key_name, &property_key_default_value, ZEND_ACC_PUBLIC, NULL, (zend_type) ZEND_TYPE_INIT_MASK(MAY_BE_STRING|MAY_BE_NULL));
	zend_string_release(property_key_name);

	zval property_offset_default_value;
	ZVAL_UNDEF(&property_offset_default_value);
	zend_string *property_offset_name = zend_string_init("offset", sizeof("offset") - 1, 1);
	zend_declare_typed_property(class_entry, property_offset_name, &property_offset_default_value, ZEND_ACC_PUBLIC, NULL, (zend_type) ZEND_TYPE_INIT_MASK(MAY_BE_LONG));
	zend_string_release(property_offset_name);

	zval property_headers_default_value;
	ZVAL_UNDEF(&property_headers_default_value);
	zend_string *property_headers_name = zend_string_init("headers", sizeof("headers") - 1, 1);
	zend_declare_typed_property(class_entry, property_headers_name, &property_headers_default_value, ZEND_ACC_PUBLIC, NULL, (zend_type) ZEND_TYPE_INIT_MASK(MAY_BE_ARRAY));
	zend_string_release(property_headers_name);

	zval property_opaque_default_value;
	ZVAL_NULL(&property_opaque_default_value);
	zend_string *property_opaque_name = zend_string_init("opaque", sizeof("opaque") - 1, 1);
	zend_declare_typed_property(class_entry, property_opaque_name, &property_opaque_default_value, ZEND_ACC_PUBLIC, NULL, (zend_type) ZEND_TYPE_INIT_MASK(MAY_BE_STRING|MAY_BE_NULL));
	zend_string_release(property_opaque_name);

	zval property_native_headers_default_value;
	ZVAL_NULL(&property_native_headers_default_value);
	zend_string *property_native_headers_name = zend_string_init("native_headers", sizeof("native_headers") - 1, 1);
	zend_declare_typed_property(class_entry, property_native_headers_name, &property_native_headers_default_value, ZEND_ACC_PRIVATE, NULL, (zend_type) ZEND_TYPE_INIT_MASK(MAY_BE_ARRAY|MAY_BE_NULL));
	zend_string_release(property_native_headers_name);

	return class_entry;
}
