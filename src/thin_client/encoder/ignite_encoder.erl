-module(ignite_encoder).

-export([write/2, calc_max_offset/3]).

-include("type_binary_spec.hrl").

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
%% I think this is unnecessary
%% -define(collection_code, 24).
-define(map_code, 25).
-define(enum_array_code, 29).
-define(complex_object_code, 103).

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

%% TODO
write({object_array, TypeName, Array}, Bin) ->
    Bin;

write({map, Map}, Bin) ->
    Bin;

write({linked_hash_map, Map}, Bin) ->
    Bin;

write({enum_array, TypeName, EnumArray}, Bin) ->
    TypeId = utils:hash(TypeName),
    write_nullable_object_array(EnumArray, enum, <<Bin/binary, ?enum_array_code:?sbyte_spec, TypeId:?sint_spec>>);

write({complex_object, TypeName, Value}, Bin) ->
    %% TODO SPEC
    {spec, Fields} = Value,
    BaseOffset = 24,
    {FieldData, [Last|OffsetTail] = Offsets} = 
    lists:foldl(fun(Field, {FieldDataAcc, OffsetAcc}) ->
                    FieldDataAcc2 = write(Field, FieldDataAcc),
                    OffsetsAcc2 = [erlang:byte_size(FieldDataAcc) + BaseOffset | OffsetAcc],
                    {FieldDataAcc2, OffsetsAcc2}
                end, {<<>>, [24]}, Fields),
    MaxOffset = calc_max_offset(OffsetTail, Last, 0),
    OffsetType = get_offset_type(MaxOffset),
    SchemaData = lists:foldl(fun(Offset, Acc) -> write({OffsetType, Offset}, Acc) end, 
                             <<>>,
                             lists:reverse(Offsets)),
    Bin.

write_array(List, Type, Bin) ->
    Len = erlang:length(List),
    write_array2(List, Type, <<Bin/binary, Len:?sint_spec>>).

write_array2([], _, Bin) -> Bin;
write_array2([H|T], Type, Bin) -> 
    Bin2 = write({Type, H}, Bin),
    write_array(T, Type, Bin2).

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

calc_max_offset([], _, Max) -> Max;
calc_max_offset([H|T], Last, Max) ->
    calc_max_offset(T, H, erlang:max(Last - H, Max)).

get_offset_type(MaxOffset) ->
    if MaxOffset < 16#100 -> byte;
       MaxOffset < 16#10000 -> short;
       true -> int
    end.
