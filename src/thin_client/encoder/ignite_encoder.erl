-module(ignite_encoder).

-include("schema.hrl").
-include("type_spec.hrl").
-include("type_binary_spec.hrl").

-export([write/2, write/3]).

%%----Write -------------------------------------------------------------------
inner_write({byte, Byte}, Bin, _) ->
    <<Bin/binary, ?byte_code:?sbyte_spec, Byte:?sbyte_spec>>;

inner_write({short, Short}, Bin, _) ->
    <<Bin/binary, ?short_code:?sbyte_spec, Short:?sshort_spec>>;

inner_write({int, Int}, Bin, _) ->
    <<Bin/binary, ?int_code:?sbyte_spec, Int:?sint_spec>>;

inner_write({long, Long}, Bin, _) ->
    <<Bin/binary, ?long_code:?sbyte_spec, Long:?slong_spec>>;

inner_write({float, Float}, Bin, _) ->
    <<Bin/binary, ?float_code:?sbyte_spec, Float:?sfloat_spec>>;

inner_write({double, Double}, Bin, _) ->
    <<Bin/binary, ?double_code:?sbyte_spec, Double:?sdouble_spec>>;

inner_write({char, Char}, Bin, _) ->
    <<Bin/binary, ?char_code:?sbyte_spec, Char?char_spec>>;

inner_write({bool, Bool}, Bin, _) ->
    Value = utils:to_raw_bool(Bool),
    <<Bin/binary, ?bool_code:?sbyte_spec, Value:?bool_spec>>;

inner_write(undefined, Bin, _) ->
    <<Bin/binary, ?null_code:?sbyte_spec>>;

inner_write({bin_string, String}, Bin, Option) ->
    case String of
        undefined -> inner_write(undefined, Bin, Option);
        _ ->
            Len = erlang:byte_size(String), 
            <<Bin/binary, ?string_code:?sbyte_spec, Len:?sint_spec, String/binary>>
    end;

inner_write({string, String}, Bin, Option) ->
    case String of
        undefined -> inner_write(undefined, Bin, Option);
        _ ->
            BinString = unicode:characters_to_binary(String),
            Len = erlang:byte_size(BinString), 
            <<Bin/binary, ?string_code:?sbyte_spec, Len:?sint_spec, BinString/binary>>
    end;

inner_write({uuid, UUID}, Bin, _) ->
    <<Bin/binary, ?uuid_code:?sbyte_spec, UUID/binary>>;

%% I think msec_fraction_in_nsecs is unnecessary
inner_write({timestamp, Date}, Bin, _) ->
    Msecs = qdate:to_unixtime(Date) * 1000,
    <<Bin/binary, ?timestamp_code:?sbyte_spec, Msecs:?slong_spec, 0:?sint_spec>>;

inner_write({date, Date}, Bin, _) ->
    Msecs = qdate:to_unixtime({Date, {0, 0, 0}}) * 1000,
    <<Bin/binary, ?date_code:?sbyte_spec, Msecs:?slong_spec>>;

inner_write({time, Time}, Bin, _) ->
    Value = calendar:time_to_seconds(Time) * 1000,
    <<Bin/binary, ?time_code:?sbyte_spec, Value:?slong_spec>>;

inner_write({{enum, TypeName}, Value}, Bin, _) ->
    TypeId = utils:hash(TypeName),
    RawValue = 
    case schema_manager:get_type(TypeId) of
        undefined -> Value;
        #enum_schema{values = Values} ->
            Tuple = lists:keyfind(Value, ?ENUM_ATOM_POS, Values),
            erlang:element(?ENUM_VALUE_POS, Tuple)
    end,
    <<Bin/binary, ?enum_code:?sbyte_spec, TypeId:?sint_spec, RawValue:?sint_spec>>;

%% for performance, byte array shoud be binary, not byte list
inner_write({byte_array, ByteArray}, Bin, _) ->
    Len = erlang:byte_size(ByteArray),
    <<Bin/binary, ?byte_array_code:?sbyte_spec, Len:?sint_spec, ByteArray/binary>>;

inner_write({short_array, ShortArray}, Bin, Option) ->
    write_array(ShortArray, short, <<Bin/binary, ?short_array_code:?sbyte_spec>>, Option);

inner_write({int_array, IntArray}, Bin, Option) ->
    write_array(IntArray, int, <<Bin/binary, ?int_array_code:?sbyte_spec>>, Option);

inner_write({long_array, LongArray}, Bin, Option) ->
    write_array(LongArray, long, <<Bin/binary, ?long_array_code:?sbyte_spec>>, Option);

inner_write({float_array, FloatArray}, Bin, Option) ->
    write_array(FloatArray, float, <<Bin/binary, ?float_array_code:?sbyte_spec>>, Option);

inner_write({double_array, DoubletArray}, Bin, Option) ->
    write_array(DoubletArray, double, <<Bin/binary, ?double_array_code:?sbyte_spec>>, Option);

inner_write({char_array, CharArray}, Bin, Option) ->
    write_array(CharArray, char, <<Bin/binary, ?char_array_code:?sbyte_spec>>, Option);

inner_write({bool_array, BoolArray}, Bin, Option) ->
    write_array(BoolArray, bool, <<Bin/binary, ?bool_array_code:?sbyte_spec>>, Option);

inner_write({bin_string_array, StringArray}, Bin, Option) ->
    write_nullable_object_array(StringArray, bin_string, <<Bin/binary, ?string_array_code:?sbyte_spec>>, Option);

inner_write({string_array, StringArray}, Bin, Option) ->
    write_nullable_object_array(StringArray, string, <<Bin/binary, ?string_array_code:?sbyte_spec>>, Option);

inner_write({uuid_array, UUIDArray}, Bin, Option) ->
    write_nullable_object_array(UUIDArray, uuid, <<Bin/binary, ?uuid_array_code:?sbyte_spec>>, Option);

inner_write({timestamp_array, DateArray}, Bin, Option) ->
    write_nullable_object_array(DateArray, timestamp, <<Bin/binary, ?timestamp_array_code:?sbyte_spec>>, Option);

inner_write({date_array, DateArray}, Bin, Option) ->
    write_nullable_object_array(DateArray, date, <<Bin/binary, ?date_array_code:?sbyte_spec>>, Option);

inner_write({time_array, TimeArray}, Bin, Option) ->
    write_nullable_object_array(TimeArray, time, <<Bin/binary, ?time_array_code:?sbyte_spec>>, Option);

inner_write({{object_array, TypeName}, Array}, Bin, Option) ->
    TypeId = utils:hash_string(TypeName),
    write_nullable_object_array(Array, 
                                {complex_object, TypeName},
                                <<Bin/binary, ?object_array_code:?sbyte_spec, TypeId:?sint_spec>>,
                                Option);

inner_write({{collection, Type, ElementType}, Collection}, Bin, Option) ->
    case Type of
        array ->
            RawType = 1,
            Values = array:to_list(Collection);
        list ->
            RawType = 2,
            Values = Collection;
        sets ->
            RawType = 3,
            Values = sets:to_list(Collection);
        ordsets ->
            RawType = 4,
            Values = ordsets:to_list(Collection)
    end,
    Len = erlang:length(Values),
    lists:foldl(fun(Value, BinAcc) ->
                    case Value of
                        undefined -> inner_write(undefined, BinAcc, Option);
                        _ -> inner_write({ElementType, Value}, BinAcc, Option)
                    end
                end,
                <<Bin/binary, ?collection_code:?sbyte_spec, Len:?sint_spec, RawType:?sbyte_spec>>,
                Values);

inner_write({{map, KeyType, ValueType}, Map}, Bin, Option) ->
    KeyValues = maps:to_list(Map),
    write_map(KeyValues, 1, KeyType, ValueType, Bin, Option);

inner_write({{orddict, KeyType, ValueType}, Orddict}, Bin, Option) ->
    KeyValues = orddict:to_list(Orddict),
    write_map(KeyValues, 2, KeyType, ValueType, Bin, Option);
    
inner_write({{enum_array, TypeName}, EnumArray}, Bin, Option) ->
    TypeId = utils:hash(TypeName),
    write_nullable_object_array(EnumArray, enum, <<Bin/binary, ?enum_array_code:?sbyte_spec, TypeId:?sint_spec>>, Option);

inner_write({{complex_object, TypeName}, Value}, Bin, Option) ->
    #type_schema{type_id = TypeId,
                 type_type = TypeType,
                 schema_id = SchemaId, 
                 version = Version,
                 field_types = FieldTypes,
                 field_keys = FieldKeys,
                 field_id_order = FieldIdOrder,
                 schema_format = SchemaFormat} = schema_manager:get_type(TypeName),
    FieldPairs = get_field_pairs(FieldTypes, TypeType, FieldKeys, Value),
    BaseOffset = ?COMPLEX_OBJECT_HEAD_OFFSET,
    BaseFlag = ?USER_TYPE,
    {FlagT, BodyT, HashCodeT, SchemaOffsetT} =
    if FieldPairs =:= [] -> {BaseFlag, <<>>, 0, 0};
       true ->
           {_, MaxOffset, FieldData, OffsetsR} = 
           lists:foldl(fun(FieldPair,
                           {LastOffsetAcc, MaxOffsetAcc, FieldDataAcc, OffsetAcc}) ->
                               LastOffsetAcc2 = erlang:byte_size(FieldDataAcc) + BaseOffset, 
                               FieldDataAcc2 = inner_write(FieldPair, FieldDataAcc, Option),
                               OffsetsAcc2 = [LastOffsetAcc2 | OffsetAcc],
                               MaxOffset2 = erlang:max(LastOffsetAcc2 - LastOffsetAcc, MaxOffsetAcc),
                               {LastOffsetAcc2, MaxOffset2, FieldDataAcc2, OffsetsAcc2}
                       end, 
                       {0, 0, <<>>, []}, 
                       FieldPairs),
           {OffsetType, OffsetFlag} = get_offset_type(MaxOffset),
           Offsets = lists:reverse(OffsetsR),
           Flag2 = BaseFlag bor OffsetFlag bor ?HAS_SCHEMA,  
           case SchemaFormat of
               full ->
                   Flag3 = Flag2,
                   SchemaData = lists:foldl(fun({FieldId, Offset}, Acc) -> 
                                                    Acc1 = write_direct({int, FieldId}, Acc, Option),
                                                    write_direct({OffsetType, Offset}, Acc1, Option) 
                                            end, 
                                            <<>>,
                                            lists:zip(FieldIdOrder, Offsets));
               _ ->
                   Flag3 = Flag2 bor ?COMPACT_FOOTER,
                   SchemaData = lists:foldl(fun(Offset, Acc) -> write_direct({OffsetType, Offset}, Acc, Option) end, 
                                            <<>>,
                                            Offsets)
           end,
           Body = <<FieldData/binary, SchemaData/binary>>,
           {Flag3, Body, utils:hash_data(Body), BaseOffset + erlang:byte_size(FieldData)}
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

inner_write({fast_term, Term}, Bin, Option) ->
    Data = erlang:term_to_binary(Term),
    inner_write({byte_array, Data}, Bin, Option);

inner_write({term, Term}, Bin, Option) ->
    Data = erlang:term_to_binary(Term),
    inner_write({{complex_object, "ErlangTerm"}, {term, Data}}, Bin, Option);

inner_write({wrapped, Binary, Offset}, Bin, _) ->
    Len = erlang:byte_size(Binary),
    <<Bin/binary, ?wrapped_data_code:?sbyte_spec, Len:?sint_spec, Binary/binary, Offset:?sint_spec>>;

inner_write({{binary_enum, TypeName}, Value}, Bin, _) ->
    TypeId = utils:hash(TypeName),
    <<Bin/binary, ?binary_enum_code:?sbyte_spec, TypeId:?sint_spec, Value:?sint_spec>>.

%%----Write With Type Info-------------------------------------------------------------------
write_direct({byte, Byte}, Bin, _) -> <<Bin/binary, Byte:?sbyte_spec>>;
write_direct({short, Short}, Bin, _) -> <<Bin/binary, Short:?sshort_spec>>;
write_direct({int, Int}, Bin, _) -> <<Bin/binary, Int:?sint_spec>>;
write_direct({long, Long}, Bin, _) -> <<Bin/binary, Long:?slong_spec>>;
write_direct({float, Float}, Bin, _) -> <<Bin/binary, Float:?sfloat_spec>>;
write_direct({double, Double}, Bin, _) -> <<Bin/binary, Double:?sdouble_spec>>;
write_direct({char, Char}, Bin, _) -> <<Bin/binary, Char?char_spec>>;
write_direct({bool, Bool}, Bin, _) ->
    Value = utils:to_raw_bool(Bool),
    <<Bin/binary, Value:?bool_spec>>.

write_array(List, Type, Bin, Option) ->
    Len = erlang:length(List),
    lists:foldl(fun(Value, BinAcc) ->
                    write_direct({Type, Value}, BinAcc, Option)
                end,
                <<Bin/binary, Len:?sint_spec>>,
                List).

write_nullable_object_array(List, Type, Bin, Option) ->
    Len = erlang:length(List),
    lists:foldl(fun(Value, BinAcc) ->
                    case Value of
                        undefined -> write(undefined, BinAcc, Option);
                        _ -> write({Type, Value}, BinAcc, Option)
                    end
                end,
                <<Bin/binary, Len:?sint_spec>>,
                List).

write_map(Pairs, MapType, KeyType, ValueType, Bin, Option) -> 
    Len = erlang:length(Pairs),
    lists:foldl(fun({Key, Value}, Acc) ->
                    Acc1 = write({KeyType, Key}, Acc, Option),
                    write({ValueType, Value}, Acc1, Option)
                end, 
                <<Bin/binary, ?map_code:?sbyte_spec, Len:?sint_spec, MapType:?sbyte_spec>>,
                Pairs).

%%---- Internal functions-------------------------------------------------------------------
get_offset_type(MaxOffset) ->
    if MaxOffset < 16#100 -> {byte, ?OFFSET_ONE_BYTE};
       MaxOffset < 16#10000 -> {short, ?OFFSET_TWO_BYTES};
       true -> {int, 0}
    end.

get_field_pairs([], _, _, _) -> [];

get_field_pairs(Types, map, FieldKeys, Map) ->
    Values = [maps:get(OrderKey, Map) || OrderKey <- FieldKeys],
    lists:zip(Types, Values);

get_field_pairs(Types, tuple, _, Value) ->
    Values = erlang:tuple_to_list(Value),
    lists:zip(Types, Values);

get_field_pairs(Types, record, _, Value) ->
    [_ | Values] = erlang:tuple_to_list(Value),
    lists:zip(Types, Values).

%%---- API functions-------------------------------------------------------------------
write(Value, Bin) -> write(Value, Bin, []).

write(Value, Bin, #write_option{} = Option) -> 
    inner_write(Value, Bin, Option);

write(Value, Bin, Options) -> 
    Option = utils:parse_write_options(Options),
    inner_write(Value, Bin, Option).
