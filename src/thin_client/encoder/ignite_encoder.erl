-module(ignite_encoder).

-include("type_binary_spec.hrl").
-include("schema.hrl").

-export([write/2]).

%%----Write -------------------------------------------------------------------
write({byte, Byte}, Bin) ->
    <<Bin/binary, ?byte_code:?sbyte_spec, Byte:?sbyte_spec>>;

write({short, Short}, Bin) ->
    <<Bin/binary, ?short_code:?sbyte_spec, Short:?sshort_spec>>;

write({int, Int}, Bin) ->
    <<Bin/binary, ?int_code:?sbyte_spec, Int:?sint_spec>>;

write({long, Long}, Bin) ->
    <<Bin/binary, ?long_code:?sbyte_spec, Long:?slong_spec>>;

write({float, Float}, Bin) ->
    <<Bin/binary, ?float_code:?sbyte_spec, Float:?sfloat_spec>>;

write({double, Double}, Bin) ->
    <<Bin/binary, ?double_code:?sbyte_spec, Double:?sdouble_spec>>;

write({char, Char}, Bin) ->
    <<Bin/binary, ?char_code:?sbyte_spec, Char?char_spec>>;

write({bool, Bool}, Bin) ->
    Value = if Bool -> 1;
               true -> 0
            end,
    <<Bin/binary, ?bool_code:?sbyte_spec, Value:?bool_spec>>;

write(undefined, Bin) ->
    <<Bin/binary, ?null_code:?sbyte_spec>>;

write({bin_string, String}, Bin) ->
    Len = erlang:byte_size(String),
    <<Bin/binary, ?string_code:?sbyte_spec, Len:?sint_spec, String/binary>>;

write({string, String}, Bin) ->
    BinString = unicode:characters_to_binary(String),
    write({bin_string, BinString}, Bin);

write({uuid, UUID}, Bin) ->
    <<Bin/binary, ?uuid_code:?sbyte_spec, UUID/binary>>;

%% I think msec_fraction_in_nsecs is unnecessary
write({timestamp, Date}, Bin) ->
    Msecs = qdate:to_unixtime(Date) * 1000,
    <<Bin/binary, ?timestamp_code:?sbyte_spec, Msecs:?slong_spec, 0:?sint_spec>>;

write({date, Date}, Bin) ->
    Msecs = qdate:to_unixtime(Date) * 1000,
    <<Bin/binary, ?date_code:?sbyte_spec, Msecs:?slong_spec>>;

write({time, Time}, Bin) ->
    Value = calendar:time_to_seconds(Time) * 1000,
    <<Bin/binary, ?time_code:?sbyte_spec, Value:?slong_spec>>;

write({enum, TypeName, Value}, Bin) ->
    TypeId = utils:hash(TypeName),
    <<Bin/binary, ?enum_code:?sbyte_spec, TypeId:?sint_spec, Value:?sint_spec>>;

%% for performance, byte array shoud be binary, not byte list
write({byte_array, ByteArray}, Bin) ->
    Len = erlang:byte_size(ByteArray),
    <<Bin/binary, ?byte_array_code:?sbyte_spec, Len:?sint_spec, ByteArray/binary>>;

write({short_array, ShortArray}, Bin) ->
    write_array(ShortArray, short, <<Bin/binary, ?short_array_code:?sbyte_spec>>);

write({int_array, IntArray}, Bin) ->
    write_array(IntArray, int, <<Bin/binary, ?int_array_code:?sbyte_spec>>);

write({long_array, LongArray}, Bin) ->
    write_array(LongArray, long, <<Bin/binary, ?long_array_code:?sbyte_spec>>);

write({float_array, FloatArray}, Bin) ->
    write_array(FloatArray, float, <<Bin/binary, ?float_array_code:?sbyte_spec>>);

write({double_array, DoubletArray}, Bin) ->
    write_array(DoubletArray, double, <<Bin/binary, ?double_array_code:?sbyte_spec>>);

write({char_array, CharArray}, Bin) ->
    write_array(CharArray, char, <<Bin/binary, ?char_array_code:?sbyte_spec>>);

write({bool_array, BoolArray}, Bin) ->
    write_array(BoolArray, bool, <<Bin/binary, ?bool_array_code:?sbyte_spec>>);

write({bin_string_array, StringArray}, Bin) ->
    write_nullable_object_array(StringArray, bin_string, <<Bin/binary, ?string_array_code:?sbyte_spec>>);

write({string_array, StringArray}, Bin) ->
    write_nullable_object_array(StringArray, string, <<Bin/binary, ?string_array_code:?sbyte_spec>>);

write({uuid_array, UUIDArray}, Bin) ->
    write_nullable_object_array(UUIDArray, uuid, <<Bin/binary, ?uuid_array_code:?sbyte_spec>>);

write({timestamp_array, DateArray}, Bin) ->
    write_nullable_object_array(DateArray, timestamp, <<Bin/binary, ?timestamp_array_code:?sbyte_spec>>);

write({date_array, DateArray}, Bin) ->
    write_nullable_object_array(DateArray, date, <<Bin/binary, ?date_array_code:?sbyte_spec>>);

write({time_array, TimeArray}, Bin) ->
    write_nullable_object_array(TimeArray, time, <<Bin/binary, ?time_array_code:?sbyte_spec>>);

write({object_array, TypeName, Array}, Bin) ->
    TypeId = utils:hash_name(TypeName),
    Len = erlang:length(Array),
    lists:foldl(fun(Value, Acc) ->
                    write({complex_object, TypeName, Value}, Acc)
                end,
                <<?object_array_code:?sbyte_spec, TypeId:?sint_spec, Len:?sint_spec>>,
               Array);

write({map, Map}, Bin) ->
    Bin;

write({enum_array, TypeName, EnumArray}, Bin) ->
    TypeId = utils:hash(TypeName),
    write_nullable_object_array(EnumArray, enum, <<Bin/binary, ?enum_array_code:?sbyte_spec, TypeId:?sint_spec>>);

write({complex_object, TypeName, Value}, Bin) ->
    #type_schema{type_id = TypeId,
                 schema_id = SchemaId, 
                 version = Version,
                 field_ids = FieldIds,
                 schema_format = SchemaFormat,
                 to_spec = ToSpec} = schema_manager:get_type(TypeName),
    Fields = ToSpec(Value),
    BaseOffset = 24,
    BaseFlag = ?USER_TYPE,
    {FlagT, BodyT, HashCodeT, SchemaOffsetT} =
    if Fields =:= [] -> {BaseFlag, <<>>, 0, 0};
       true ->
           {_, MaxOffset, FieldData, Offsets} = 
           lists:foldl(fun(Field, {LastOffsetAcc, MaxOffsetAcc, FieldDataAcc, OffsetAcc}) ->
                               FieldDataAcc2 = write(Field, FieldDataAcc),
                               LastOffsetAcc2 = erlang:byte_size(FieldDataAcc) + BaseOffset, 
                               OffsetsAcc2 = [LastOffsetAcc2 | OffsetAcc],
                               MaxOffset2 = erlang:max(LastOffsetAcc2 - LastOffsetAcc, MaxOffsetAcc),
                               {LastOffsetAcc2, MaxOffset2, FieldDataAcc2, OffsetsAcc2}
                       end, {24, 24, <<>>, [24]}, Fields),
           {OffsetType, OffsetFlag} = get_offset_type(MaxOffset),
           Offsets2 = lists:reverse(Offsets),
           Flag2 = BaseFlag bor OffsetFlag bor ?HAS_SCHEMA bor ?COMPACT_FOOTER,
           SchemaData = lists:foldl(fun(Offset, Acc) -> write({OffsetType, Offset}, Acc) end, 
                                    <<>>,
                                    Offsets2),
           Body = <<FieldData/binary, SchemaData/binary>>,
           {Flag2, Body, utils:hash_data(Body), BaseOffset + erlang:byte_size(FieldData)}
    end,
    <<Bin/binary,
      ?complex_object_code:?sbyte_spec,
      Version:?sbyte_spec,
      FlagT:?sshort_spec,
      TypeId:?sint_spec,
      HashCodeT:?sint_spec,
      (BaseOffset + erlang:byte_size(BodyT)):?sint_spec,
      SchemaId:?sint_spec,
      SchemaOffsetT:?sint_spec,
      BodyT/binary>>;

write({wrapped, Binary}, Bin) ->
    Len = erlang:byte_size(Binary),
    <<Bin/binary, ?wrapped_data_code:?sbyte_spec, Len:?sint_spec, Binary/binary, 0:?sint_spec>>;

write({binary_enum, TypeName, Value}, Bin) ->
    TypeId = utils:hash(TypeName),
    <<Bin/binary, ?binary_enum_code:?sbyte_spec, TypeId:?sint_spec, Value:?sint_spec>>.

%%----Write With Type Info-------------------------------------------------------------------
write_direct({short, Short}, Bin) -> <<Bin/binary, Short:?sshort_spec>>;

write_direct({int, Int}, Bin) -> <<Bin/binary, Int:?sint_spec>>;

write_direct({long, Long}, Bin) -> <<Bin/binary, Long:?slong_spec>>;

write_direct({float, Float}, Bin) -> <<Bin/binary, Float:?sfloat_spec>>;

write_direct({double, Double}, Bin) -> <<Bin/binary, Double:?sdouble_spec>>;

write_direct({char, Char}, Bin) -> <<Bin/binary, Char?char_spec>>;

write_direct({bool, Bool}, Bin) ->
    Value = if Bool -> 1;
               true -> 0
            end,
    <<Bin/binary, Value:?bool_spec>>.

write_array(List, Type, Bin) ->
    Len = erlang:length(List),
    write_array2(List, Type, <<Bin/binary, Len:?sint_spec>>).

write_array2([], _, Bin) -> Bin;
write_array2([H|T], Type, Bin) -> 
    Bin2 = write_direct({Type, H}, Bin),
    write_array2(T, Type, Bin2).

write_nullable_object_array(List, Type, Bin) ->
    Len = erlang:length(List),
    write_nullable_object_array2(List, Type, <<Bin/binary, Len:?sint_spec>>).

write_nullable_object_array2([], _, Bin) -> Bin;
write_nullable_object_array2([H|T], Type, Bin) -> 
    case H of
        undefined -> write(undefined, Bin);
        _ ->
            Bin2 = write({Type, H}, Bin),
            write_nullable_object_array2(T, Type, Bin2)
    end.

get_offset_type(MaxOffset) ->
    if MaxOffset < 16#100 -> {byte, ?OFFSET_ONE_BYTE};
       MaxOffset < 16#10000 -> {short, ?OFFSET_TWO_BYTES};
       true -> {int, 0}
    end.
