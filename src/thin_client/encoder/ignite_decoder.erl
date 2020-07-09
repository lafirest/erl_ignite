-module(ignite_decoder).

-export([read/1, read_value/1]).

-include("schema.hrl").
-include("type_binary_spec.hrl").

%%----read -------------------------------------------------------------------
read_value(Bin) ->
    {Value, _} = read(Bin),
    Value.

read(<<Code:?sbyte_spec, Bin/binary>>) -> read(Code, Bin).

read(?byte_code, <<Byte:?sbyte_spec, Bin/binary>>) -> {Byte, Bin};

read(?short_code, <<Short:?sshort_spec, Bin/binary>>) -> {Short, Bin};

read(?int_code, <<Int:?sint_spec, Bin/binary>>) -> {Int, Bin};

read(?long_code, <<Long:?slong_spec, Bin/binary>>) -> {Long, Bin};

read(?float_code, <<Float:?sfloat_spec, Bin/binary>>) -> {Float, Bin};

read(?double_code, <<Double:?sdouble_spec, Bin/binary>>) -> {Double, Bin};

read(?char_code, <<Char?char_spec, Bin/binary>>) -> {Char, Bin};

read(?bool_code, <<Value:?bool_spec, Bin/binary>>) -> 
    {utils:from_raw_bool(Value), Bin};

read(?null_code, Bin) -> {undefined, Bin};

read(?string_code, <<Len:?sint_spec, String:Len/binary, Bin/binary>>) ->
    {String, Bin};

read(?uuid_code, <<UUID:16/binary, Bin/binary>>) -> {UUID, Bin};

%% I think msec_fraction_in_nsecs is unnecessary
read(?timestamp_code, <<Msecs:?slong_spec, _:?sint_spec, Bin/binary>>) -> {qdate:to_date(Msecs div 1000), Bin};

read(?date_code, <<Msecs:?slong_spec, Bin/binary>>) -> {qdate:to_date(Msecs div 1000), Bin};

read(?time_code, <<Value:?slong_spec, Bin/binary>>) -> {calendar:seconds_to_time(Value div 1000), Bin};

read(?enum_code, <<_:?sint_spec, Value:?sint_spec, Bin/binary>>) -> {Value, Bin};

%% for performance, byte array shoud be binary, not byte list
read(?byte_array_code, <<Len:?sint_spec, ByteArray:Len/binary, Bin/binary>>) -> {ByteArray, Bin};

read(?short_array_code, Bin) -> read_array_with_type(short, Bin); 

read(?int_array_code, Bin) -> read_array_with_type(int, Bin); 

read(?long_array_code, Bin) -> read_array_with_type(long, Bin); 

read(?float_array_code, Bin) -> read_array_with_type(float, Bin); 

read(?double_array_code, Bin) -> read_array_with_type(double, Bin); 

read(?char_array_code, Bin) -> read_array_with_type(char, Bin); 

read(?bool_array_code, Bin) -> read_array_with_type(bool, Bin); 

read(?string_array_code, Bin) -> read_array(Bin);

read(?uuid_array_code, Bin) -> read_array(Bin);

read(?timestamp_code, Bin) -> read_array(Bin);

read(?date_array_code, Bin) -> read_array(Bin);

read(?time_array_code, Bin) ->  read_array(Bin);

read(?object_array_code, <<_:?sint_spec, Bin/binary>>) -> read_array(Bin);

read(?map_code, <<Len:?sint_spec, MapType:?sbyte_spec, Bin/binary>>) ->
    {Pairs, Bin2} = 
    loop:dotimes(fun({PairAcc, BinAcc}) ->
                    {Key, BinAcc2} = read(BinAcc),
                    {Value, BinAcc3} = read(BinAcc2),
                    Pair = {Key, Value},
                    {[Pair | PairAcc], BinAcc3}
                 end, 
                 Len,
                 {[], Bin}),

    {if MapType =:= 1 -> maps:from_list(Pairs);
        true -> orddict:from_list(lists:reverse(Pairs))
     end,
     Bin2};

read(?collection_code, <<Len:?sint_spec, Type:?sbyte_spec, Bin/binary>>) ->
    {ValueR, Bin2} = 
    loop:dotimes(fun({ValueAcc, BinAcc}) -> 
                         {Value, BinAcc2} = read(BinAcc),
                         {[Value | ValueAcc], BinAcc2}
                 end,
                 Len,
                 {[], Bin}),
    Values = lists:reverse(ValueR),

    case Type of
        1 -> Value = array:from_list(Values);
        2 -> Value = Values;
        3 -> Value = sets:from_list(Values);
        4 -> Value = ordsets:from_list(Values)
    end,
    {Value, Bin2};

read(?enum_array_code, <<_:?sint_spec, Bin/binary>>) -> read_array(Bin);

read(?complex_object_code, 
     <<Version:?sbyte_spec, 
       Flag:?sshort_spec,
       TypeId:?sint_spec,
       _:?sint_spec,
       Len:?sint_spec,
       _:?sint_spec,
       SchemaOffsetT:?sint_spec,
       Body:(Len - 24)/binary,
       Bin/binary>>) ->
    SchemaOffset = SchemaOffsetT - 24,
    #type_schema{version = NoVersion,
                 type_type = TypeType,
                 type_tag = TypeTag,
                 field_keys = FieldKeys,
                 constructor = Constructor,
                 on_upgrades = OnUpgrades} = schema_manager:get_type(TypeId),
    case has_schema(Flag) of
        false ->  OriginValues = [];
        _ ->
            %% read all values
            <<ValueBody:SchemaOffset/binary, _/binary>> = Body, 
            OriginValues = loop:while(fun({ValueAcc, BinAcc}) -> 
                                              {Value, BinAcc2} = read(BinAcc),
                                              ValueAcc2 = [Value | ValueAcc],
                                              if BinAcc2 =:= <<>> -> lists:reverse(ValueAcc2);
                                                 true -> {true, {ValueAcc2, BinAcc2}}
                                              end
                                      end,
                                      {[], ValueBody})
    end,

    %% upgrade
    if Version =:= NoVersion -> Values = OriginValues;
       true -> 
           UpgradHooks = lists:nthtail(Version, OnUpgrades),
           Values = lists:foldl(fun(UpgradHook, ValueAcc) -> 
                                        UpgradHook(ValueAcc) 
                                end,
                                OriginValues,
                                UpgradHooks)
    end,

    %% make object 
    Object = make_object(Constructor, TypeType, TypeTag, FieldKeys, Values),
    {Object, Bin};

read(?wrapped_data_code, <<Len:?sint_spec, Binary:Len/binary, Offset:?sint_spec, Bin/binary>>) ->
    <<_:Offset/binary, Body/binary>> = Binary,
    Value = read_value(Body),
    {Value, Bin};
%    {{wrapped, Binary, Offset}, Bin};

read(?binary_enum_code, <<_:?sint_spec, Value:?sint_spec, Bin/binary>>) -> {Value, Bin};

%%----Specified read -------------------------------------------------------------------
read(byte, <<Byte:?sbyte_spec, Bin/binary>>) -> {Byte, Bin};
read(short, <<Short:?sshort_spec, Bin/binary>>) -> {Short, Bin};
read(int, <<Int:?sint_spec, Bin/binary>>) -> {Int, Bin};
read(long, <<Long:?slong_spec, Bin/binary>>) -> {Long, Bin};
read(float, <<Float:?sfloat_spec, Bin/binary>>) -> {Float, Bin};
read(double, <<Double:?sdouble_spec, Bin/binary>>) -> {Double, Bin};
read(char, <<Char?char_spec, Bin/binary>>) -> {Char, Bin};
read(bool, <<Bool:?bool_spec, Bin/binary>>) -> {utils:from_raw_bool(Bool), Bin}.

read_array_with_type(Type, <<Len:?sint_spec, Bin/binary>>) -> 
    {Values, Bin2} =
    loop:dotimes(fun({ValueAcc, BinAcc}) -> 
                    {Value, BinAcc2} = read(Type, BinAcc),
                    {[Value | ValueAcc], BinAcc2}
                 end,
                 Len,
                 {[], Bin}),
    {lists:reverse(Values), Bin2}.

read_array(<<Len:?sint_spec, Bin/binary>>) -> 
    {Values, Bin2} =
    loop:dotimes(fun({ValueAcc, BinAcc}) -> 
                    {Value, BinAcc2} = read(BinAcc),
                    {[Value | ValueAcc], BinAcc2}
                 end,
                 Len,
                 {[], Bin}),
    {lists:reverse(Values), Bin2}.

has_schema(Flag) -> Flag band ?HAS_SCHEMA.

get_offset_type(Flag) ->
    if Flag band ?OFFSET_ONE_BYTE =/= 0 -> byte;
       Flag band ?OFFSET_TWO_BYTES =/= 0 -> short;
       true -> int
    end.

is_compact_footer(Flag) -> Flag band ?COMPACT_FOOTER =/= 0.

make_object(undefined, tuple, Tag, _, Values) ->
    erlang:list_to_tuple([Tag | Values]);

make_object(undefined, map, _, Keys, Values) ->
    Pairs = lists:zip(Keys, Values),
    maps:from_list(Pairs);

make_object(Constructor, _, _, _, Values) ->
    Constructor(Values).

