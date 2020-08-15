-module(ignite_decoder).

-export([read/1, 
         read/2, 
         read_value/1,
         read_value/2]).

-include("schema.hrl").
-include("type_spec.hrl").
-include("type_binary_spec.hrl").

%%----read -------------------------------------------------------------------
inner_read(<<Code:?sbyte_spec, Bin/binary>>, Option) -> inner_read(Code, Bin, Option).

inner_read(?byte_code, <<Byte:?sbyte_spec, Bin/binary>>, _) -> {Byte, Bin};

inner_read(?short_code, <<Short:?sshort_spec, Bin/binary>>, _) -> {Short, Bin};

inner_read(?int_code, <<Int:?sint_spec, Bin/binary>>, _) -> {Int, Bin};

inner_read(?long_code, <<Long:?slong_spec, Bin/binary>>, _) -> {Long, Bin};

inner_read(?float_code, <<Float:?sfloat_spec, Bin/binary>>, _) -> {Float, Bin};

inner_read(?double_code, <<Double:?sdouble_spec, Bin/binary>>, _) -> {Double, Bin};

inner_read(?char_code, <<Char?char_spec, Bin/binary>>, _) -> {Char, Bin};

inner_read(?bool_code, <<Value:?bool_spec, Bin/binary>>, _) -> 
    {utils:from_raw_bool(Value), Bin};

inner_read(?null_code, Bin, _) -> {undefined, Bin};

inner_read(?string_code, <<Len:?sint_spec, String:Len/binary, Bin/binary>>, _) ->
    {String, Bin};

inner_read(?uuid_code, <<UUID:16/binary, Bin/binary>>, _) -> {UUID, Bin};

%% I think msec_fraction_in_nsecs is unnecessary
inner_read(?timestamp_code, <<Msecs:?slong_spec, _:?sint_spec, Bin/binary>>, _) -> {qdate:to_date(Msecs div 1000), Bin};

inner_read(?date_code, <<Msecs:?slong_spec, Bin/binary>>, _) -> 
    {Date, _} = qdate:to_date(Msecs div 1000),
    {Date, Bin};

inner_read(?time_code, <<Value:?slong_spec, Bin/binary>>, _) -> {calendar:seconds_to_time(Value div 1000), Bin};

inner_read(?enum_code, <<TypeId:?sint_spec, RawValue:?sint_spec, Bin/binary>>, _) -> 
    Value =
    case schema_manager:get_type(TypeId) of 
        undefined -> RawValue;
        #enum_schema{values = Values} ->
            Tuple = lists:keyfind(RawValue, ?ENUM_VALUE_POS, Values),
            erlang:element(?ENUM_ATOM_POS, Tuple)
    end,
    {Value, Bin};

%% for performance, byte array shoud be binary, not byte list
inner_read(?byte_array_code, <<Len:?sint_spec, ByteArray:Len/binary, Bin/binary>>, #read_option{fast_term = FaseTerm}) -> 
    case FaseTerm of
        false -> {ByteArray, Bin};
        _ -> {erlang:binary_to_term(ByteArray), Bin}
    end;

inner_read(?short_array_code, Bin, Option) -> read_array_with_type(short, Bin, Option); 

inner_read(?int_array_code, Bin, Option) -> read_array_with_type(int, Bin, Option); 

inner_read(?long_array_code, Bin, Option) -> read_array_with_type(long, Bin, Option); 

inner_read(?float_array_code, Bin, Option) -> read_array_with_type(float, Bin, Option); 

inner_read(?double_array_code, Bin, Option) -> read_array_with_type(double, Bin, Option); 

inner_read(?char_array_code, Bin, Option) -> read_array_with_type(char, Bin, Option); 

inner_read(?bool_array_code, Bin, Option) -> read_array_with_type(bool, Bin, Option); 

inner_read(?string_array_code, Bin, Option) -> read_array(Bin, Option);

inner_read(?uuid_array_code, Bin, Option) -> read_array(Bin, Option);

inner_read(?timestamp_code, Bin, Option) -> read_array(Bin, Option);

inner_read(?date_array_code, Bin, Option) -> read_array(Bin, Option);

inner_read(?time_array_code, Bin, Option) ->  read_array(Bin, Option);

inner_read(?object_array_code, <<_:?sint_spec, Bin/binary>>, Option) -> read_array(Bin, Option);

inner_read(?map_code, <<Len:?sint_spec, MapType:?sbyte_spec, Bin/binary>>, Option) ->
    {Pairs, Bin2} = 
    loop:dotimes(fun({PairAcc, BinAcc}) ->
                         {Key, BinAcc2} = inner_read(BinAcc, Option),
                         {Value, BinAcc3} = inner_read(BinAcc2, Option),
                         Pair = {Key, Value},
                         {[Pair | PairAcc], BinAcc3}
                 end, 
                 Len,
                 {[], Bin}),

    {if MapType =:= 1 -> maps:from_list(Pairs);
        true -> orddict:from_list(lists:reverse(Pairs))
     end,
     Bin2};

inner_read(?collection_code, <<Len:?sint_spec, Type:?sbyte_spec, Bin/binary>>, Option) ->
    {ValueR, Bin2} = 
    loop:dotimes(fun({ValueAcc, BinAcc}) -> 
                         {Value, BinAcc2} = inner_read(BinAcc, Option),
                         {[Value | ValueAcc], BinAcc2}
                 end,
                 Len,
                 {[], Bin}),
    Values = lists:reverse(ValueR),

    case Type of
        1 -> Value = Values;
        2 -> Value = Values;
        3 -> Value = sets:from_list(Values);
        4 -> Value = ordsets:from_list(Values)
    end,
    {Value, Bin2};

inner_read(?enum_array_code, <<_:?sint_spec, Bin/binary>>, Option) -> read_array(Bin, Option);

inner_read(?complex_object_code, 
     <<Version:?sbyte_spec, 
       Flag:?sshort_spec,
       TypeId:?sint_spec,
       _:?sint_spec,
       Len:?sint_spec,
       _:?sint_spec,
       SchemaOffsetT:?sint_spec,
       Body:(Len - ?COMPLEX_OBJECT_HEAD_OFFSET)/binary,
       Bin/binary>>,
     #read_option{keep_binary_object = KeepBinaryObject} = Option) ->
    SchemaOffset = SchemaOffsetT - ?COMPLEX_OBJECT_HEAD_OFFSET,
    #type_schema{version = NoVersion,
                 type_type = TypeType,
                 type_tag = TypeTag,
                 field_keys = FieldKeys,
                 field_id_order = FieldIds,
                 constructor = Constructor,
                 on_upgrades = OnUpgrades} = schema_manager:get_type(TypeId),
    case KeepBinaryObject of
        false -> Value = read_complex_object(Flag, Body, SchemaOffset, Version, NoVersion, Constructor, OnUpgrades, TypeType, TypeTag, FieldKeys, Option);
        _ -> Value = read_binary_object(Flag, Body, SchemaOffset, FieldIds, Option)
    end,
    {Value, Bin};

inner_read(?wrapped_data_code, <<Len:?sint_spec, Binary:Len/binary, Offset:?sint_spec, Bin/binary>>, #read_option{keep_wrapped = KeepWrapped} = Option) ->
    case KeepWrapped of
        false ->
            <<_:Offset/binary, Body/binary>> = Binary,
            Value = read_value(Body, Option),
            {Value, Bin};
        _ ->
            {#wrapped{binary = Binary, offset = Offset}, Bin}
    end;

inner_read(?binary_enum_code, <<_:?sint_spec, Value:?sint_spec, Bin/binary>>, _) -> {Value, Bin};

%%----Specified read -------------------------------------------------------------------
inner_read(byte, <<Byte:?sbyte_spec, Bin/binary>>, _) -> {Byte, Bin};
inner_read(short, <<Short:?sshort_spec, Bin/binary>>, _) -> {Short, Bin};
inner_read(int, <<Int:?sint_spec, Bin/binary>>, _) -> {Int, Bin};
inner_read(long, <<Long:?slong_spec, Bin/binary>>, _) -> {Long, Bin};
inner_read(float, <<Float:?sfloat_spec, Bin/binary>>, _) -> {Float, Bin};
inner_read(double, <<Double:?sdouble_spec, Bin/binary>>, _) -> {Double, Bin};
inner_read(char, <<Char?char_spec, Bin/binary>>, _) -> {Char, Bin};
inner_read(bool, <<Bool:?bool_spec, Bin/binary>>, _) -> {utils:from_raw_bool(Bool), Bin}.

read_array_with_type(Type, <<Len:?sint_spec, Bin/binary>>, Option) -> 
    {Values, Bin2} =
    loop:dotimes(fun({ValueAcc, BinAcc}) -> 
                         {Value, BinAcc2} = inner_read(Type, BinAcc, Option),
                         {[Value | ValueAcc], BinAcc2}
                 end,
                 Len,
                 {[], Bin}),
    {lists:reverse(Values), Bin2}.

read_array(<<Len:?sint_spec, Bin/binary>>, Option) -> 
    {Values, Bin2} =
    loop:dotimes(fun({ValueAcc, BinAcc}) -> 
                         {Value, BinAcc2} = inner_read(BinAcc, Option),
                         {[Value | ValueAcc], BinAcc2}
                 end,
                 Len,
                 {[], Bin}),
    {lists:reverse(Values), Bin2}.

read_complex_object(Flag, Body, SchemaOffset, Version, NoVersion, Constructor, OnUpgrades, TypeType, TypeTag, FieldKeys, Option) ->
    case has_schema(Flag) of
        false ->  OriginValues = [];
        _ ->
            %% read all values
            <<ValueBody:SchemaOffset/binary, _/binary>> = Body, 
            OriginValues = loop:while(fun({ValueAcc, BinAcc}) -> 
                                              {Value, BinAcc2} = inner_read(BinAcc, Option),
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
    make_object(Constructor, TypeType, TypeTag, FieldKeys, Values).

read_binary_object(Flag, Body, SchemaOffset, FieldIds, Option) ->
    Len = erlang:byte_size(Body),
    FieldCnt = erlang:length(FieldIds),
    SchemaData = erlang:binary_part(Body, SchemaOffset, Len - SchemaOffset),
    OffsetType = get_offset_type(Flag),
    case is_compact_footer(Flag) of
        true ->
            {OffsetR ,_} = 
            loop:dotimes(fun({OffsetAcc, DataAcc}) -> 
                                 {Offset, DataAc2} = inner_read(OffsetType, DataAcc, Option),
                                 {[Offset - ?COMPLEX_OBJECT_HEAD_OFFSET | OffsetAcc], DataAc2}
                         end,
                         FieldCnt,
                         {[], SchemaData}),
            Offsets = lists:reverse(OffsetR),
            Fields = lists:zip(FieldIds, Offsets),
            Schemas = maps:from_list(Fields);
        _ ->
            {Fields ,_} = 
            loop:dotimes(fun({FieldAcc, <<FieldId:?sint_spec, DataAcc2/binary>>}) -> 
                                 {Offset, DataAc3} = inner_read(OffsetType, DataAcc2, Option),
                                 {[{FieldId, Offset - ?COMPLEX_OBJECT_HEAD_OFFSET} | FieldAcc], DataAc3}
                         end,
                         FieldCnt,
                         {[], SchemaData}),
            Schemas = maps:from_list(Fields)
    end,
    #binary_object{body = Body, schemas = Schemas}.

%%---- Internal functions-------------------------------------------------------------------
has_schema(Flag) -> Flag band ?HAS_SCHEMA.

get_offset_type(Flag) ->
    if Flag band ?OFFSET_ONE_BYTE =/= 0 -> byte;
       Flag band ?OFFSET_TWO_BYTES =/= 0 -> short;
       true -> int
    end.

is_compact_footer(Flag) -> Flag band ?COMPACT_FOOTER =/= 0.

make_object(undefined, map, _, Keys, Values) ->
    Pairs = lists:zip(Keys, Values),
    maps:from_list(Pairs);

make_object(undefined, tuple, _, _, Values) ->
    erlang:list_to_tuple(Values);

make_object(undefined, record, Tag, _, Values) ->
    erlang:list_to_tuple([Tag | Values]);

make_object(Constructor, _, _, _, Values) ->
    Constructor(Values).

%%---- API functions-------------------------------------------------------------------
read_value(Bin) -> read_value(Bin, []).

read_value(Bin, Options) -> erlang:element(1, read(Bin, Options)).

read(Bin) -> read(Bin, []).

read(Bin, #read_option{} = Option) ->
    inner_read(Bin, Option);

read(Bin, Options) ->
    Option = utils:parse_read_options(Options),
    inner_read(Bin, Option).
