%%----Type Spec Define----------------------------------------------------------
-define(sbyte_spec, 8/little-signed-integer).
-define(sshort_spec, 16/little-signed-integer).
-define(sint_spec, 32/little-signed-integer).
-define(slong_spec, 64/little-signed-integer).
-define(sfloat_spec, 32/little-float).
-define(sdouble_spec, 64/little-float).
-define(char_spec, /little-utf16).
-define(bool_spec, ?sbyte_spec).


%%----Type Code Define----------------------------------------------------------
-define(byte_code, 1).
-define(short_code, 2).
-define(int_code, 3).
-define(long_code, 4).
-define(float_code, 5).
-define(double_code, 6).
-define(char_code, 7).
-define(bool_code, 8).
-define(null_code, 101).
-define(string_code, 9).
-define(uuid_code, 10).
-define(timestamp_code, 33).
-define(date_code, 11).
-define(time_code, 36).
%% I don't need Decimal
-define(enum_code, 28).
-define(byte_array_code, 12).
-define(short_array_code, 13).
-define(int_array_code, 14).
-define(long_array_code, 15).
-define(float_array_code, 16).
-define(double_array_code, 17).
-define(char_array_code, 18).
-define(bool_array_code, 19).
-define(string_array_code, 20).
-define(uuid_array_code, 21).
-define(timestamp_array_code, 34).
-define(date_array_code, 22).
-define(time_array_code, 37).
%% I don't need Decimal Array
-define(object_array_code, 23).
-define(collection_code, 24).
-define(map_code, 25).
-define(wrapped_data_code, 27).
-define(enum_array_code, 29).
-define(binary_enum_code, 38).
-define(complex_object_code, 103).

%%----Complex Object Flags----------------------------------------------------------
-define(USER_TYPE, 16#0001).
-define(HAS_SCHEMA, 16#0002).
-define(HAS_RAW_DATA, 16#0004).
-define(OFFSET_ONE_BYTE, 16#0008).
-define(OFFSET_TWO_BYTES, 16#0010).
-define(COMPACT_FOOTER, 16#0020).
