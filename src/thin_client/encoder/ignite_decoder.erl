-module(ignite_decoder).

-export([read/1, 
         read/2, 
         read_value/1,
         read_value/2]).

-include("schema.hrl").
-include("type_spec.hrl").
-include("type_binary_spec.hrl").

-type results() :: term().

-type primitive_type() :: byte
                        | short
                        | int
                        | long
                        | float
                        | double 
                        | char
                        | bool.

-type next_jump_state() :: term().
-type jump_state() :: return
                   | {judgment, next_jump_state()}
                   | {primitive_array, primitive_type(), non_neg_integer(), values(), next_jump_state()} 
                   | {array, non_neg_integer(), values(), next_jump_state()}
                   | {map, non_neg_integer(), non_neg_integer(), values(), next_jump_state()}
                   | {to_collection, non_neg_integer(), next_jump_state()}.

-spec jump(jump_state(), binary(), term(),  results()) -> results().
jump(return, <<>>, _,  Values) ->
    erlang:hd(Values);

jump({judgment, Next}, <<Bin/binary>>, Options, Values) ->
    judgement(Bin, Options, Values, Next);

jump({primitive_array, Type, Len, ExternalValues, Next}, <<Bin/binary>>, Options, Values) ->
    Left = Len - 1,
    case Left of
        0 ->
            jump(Next, Bin, Options, [lists:reverse(Values) | ExternalValues]);
        _ ->
            read_primitive(Type, Bin, Options, Values, {primitive_array, Type, Left, ExternalValues, Next})
    end;

jump({array, Len, ExternalValues, Next}, <<Bin/binary>>, Options, Values) ->
    Left = Len - 1,
    case Left of
        0 ->
            jump(Next, Bin, Options, [lists:reverse(Values) | ExternalValues]);
        _ ->
            judgement(Bin, Options, Values, {array, Left, ExternalValues, Next})
    end;

jump({map, Len, MapType, ExternalValues, Next}, <<Bin/binary>>, Options, Values) ->
    Left = Len - 1,
    case Left of
        0 ->
            jump(Next, Bin, Options, [list_to_map(MapType, Values) | ExternalValues]);
        _ ->
            judgement(Bin, Options, Values, {map, Left, ExternalValues, Next})
    end;

jump({to_collection, Type, Next}, <<Bin/binary>>, Options, Values) ->
    [Collections | ExternalValues] = Values,
    case Type of
        1 -> Value = Collections;
        2 -> Value = Collections;
        3 -> Value = sets:from_list(Collections);
        4 -> Value = ordsets:from_list(Collections)
    end,
    jump(Next, Bin, Options, [Value | ExternalValues]).

judgement(<<Code:?sbyte_spec, Bin/binary>>, Options, Values, Next) ->
    do_read(Code, Bin, Options, Values, Next).

do_read(?byte_code, <<Byte:?sbyte_spec, Bin/binary>>, Options, Values, Next) -> 
    jump(Next, Bin, Options, [Byte | Values]);

do_read(?short_code, <<Short:?sshort_spec, Bin/binary>>, Options, Values, Next) ->
    jump(Next, Bin, Options, [Short | Values]);

do_read(?int_code, <<Int:?sint_spec, Bin/binary>>, Options, Values, Next) ->
    jump(Next, Bin, Options, [Int | Values]);

do_read(?long_code, <<Long:?slong_spec, Bin/binary>>, Options, Values, Next) ->
    jump(Next, Bin, Options, [Long | Values]);

do_read(?float_code, <<Float:?sfloat_spec, Bin/binary>>, Options, Values, Next) ->
    jump(Next, Bin, Options, [Float | Values]);

do_read(?double_code, <<Double:?sdouble_spec, Bin/binary>>, Options, Values, Next) ->
    jump(Next, Bin, Options, [Double | Values]);

do_read(?char_code, <<Char?char_spec, Bin/binary>>, Options, Values, Next) ->
    jump(Next, Bin, Options, [Char | Values]);

do_read(?bool_code, <<Value:?bool_spec, Bin/binary>>, Options, Values, Next) ->
    jump(Next, Bin, Options, [utils:from_raw_bool(Value) | Values]);

do_read(?null_code, Bin, Options, Values, Next) -> 
    jump(Next, Bin, Options, [undefined | Values]);

do_read(?string_code, <<Len:?sint_spec, String:Len/binary, Bin/binary>>, Options, Values, Next) ->
    jump(Next, Bin, Options, [String | Values]);

do_read(?uuid_code, <<UUID:16/binary, Bin/binary>>, Options, Values, Next) -> 
    jump(Next, Bin, Options, [UUID | Values]);

%% I think msec_fraction_in_nsecs is unnecessary
do_read(?timestamp_code, <<Msecs:?slong_spec, _:?sint_spec, Bin/binary>>, Options, Values, Next) -> 
    jump(Next, Bin, Options, [qdate:to_date(Msecs div 1000) | Values]);

do_read(?date_code, <<Msecs:?slong_spec, Bin/binary>>, Options, Values, Next) -> 
    {Date, _} = qdate:to_date(Msecs div 1000),
    jump(Next, Bin, Options, [Date | Values]);

do_read(?time_code, <<Value:?slong_spec, Bin/binary>>, Options, Values, Next) -> 
    jump(Next, Bin, Options, [calendar:seconds_to_time(Value div 1000) | Values]);

do_read(?enum_code, <<TypeId:?sint_spec, RawValue:?sint_spec, Bin/binary>>, Options, Values, Next) -> 
    Value =
    case schema_manager:get_type(TypeId) of 
        undefined -> RawValue;
        #enum_schema{values = Values} ->
            Tuple = lists:keyfind(RawValue, ?ENUM_VALUE_POS, Values),
            erlang:element(?ENUM_ATOM_POS, Tuple)
    end,
    jump(Next, Bin, Options, [Value | Values]);

%% for performance, byte array shoud be binary, not byte list
do_read(?byte_array_code, <<Len:?sint_spec, ByteArray:Len/binary, Bin/binary>>, #read_option{fast_term = FaseTerm} = Options, Values, Next) -> 
    Value = 
        case FaseTerm of
            false -> ByteArray;
            _ -> erlang:binary_to_term(ByteArray)
        end,
    jump(Next, Bin, Options, [Value | Values]);

do_read(?short_array_code, Bin, Option, Values, Next) -> read_primitive_array(short, Bin, Option, Values, Next); 

do_read(?int_array_code, Bin, Option, Values, Next) -> read_primitive_array(int, Bin, Option, Values, Next); 

do_read(?long_array_code, Bin, Option, Values, Next) -> read_primitive_array(long, Bin, Option, Values, Next); 

do_read(?float_array_code, Bin, Option, Values, Next) -> read_primitive_array(float, Bin, Option, Values, Next); 

do_read(?double_array_code, Bin, Option, Values, Next) -> read_primitive_array(double, Bin, Option, Values, Next); 

do_read(?char_array_code, Bin, Option, Values, Next) -> read_primitive_array(char, Bin, Option, Values, Next); 

do_read(?bool_array_code, Bin, Option, Values, Next) -> read_primitive_array(bool, Bin, Option, Values, Next); 

do_read(?string_array_code, Bin, Option, Values, Next) -> read_array(Bin, Option, Values, Next);

do_read(?uuid_array_code, Bin, Option, Values, Next) -> read_array(Bin, Option, Values, Next);

do_read(?timestamp_code, Bin, Option, Values, Next) -> read_array(Bin, Option, Values, Next);

do_read(?date_array_code, Bin, Option, Values, Next) -> read_array(Bin, Option, Values, Next);

do_read(?time_array_code, Bin, Option, Values, Next) ->  read_array(Bin, Option, Values, Next);

do_read(?object_array_code, <<_:?sint_spec, Bin/binary>>, Options, Values, Next) -> 
    read_array(Bin, Options, Values, Next);

do_read(?map_code, <<Len:?sint_spec, MapType:?sbyte_spec, Bin/binary>>, Options, Values, Next) ->
    judgement(Bin, Options, [], {map, Len * 2, MapType, Values, Next});

do_read(?collection_code, <<Len:?sint_spec, Type:?sbyte_spec, Bin/binary>>, Options, Values, Next) ->
    judgement(Bin, Options, [], {array, Len, Values, {to_collection, Type, Next}});

do_read(?enum_array_code, <<_:?sint_spec, Bin/binary>>, Options, Values, Next) -> 
    read_array(Bin, Options, Values, Next);

do_read(?complex_object_code, 
        <<Version:?sbyte_spec, 
          Flag:?sshort_spec,
          TypeId:?sint_spec,
          _:?sint_spec,
          Len:?sint_spec,
          _:?sint_spec,
          SchemaOffsetT:?sint_spec,
          Body:(Len - ?COMPLEX_OBJECT_HEAD_OFFSET)/binary,
          Bin/binary>>,
        #read_option{keep_binary_object = KeepBinaryObject} = Options,
        Values,
        Next) ->
    SchemaOffset = SchemaOffsetT - ?COMPLEX_OBJECT_HEAD_OFFSET,
    #type_schema{field_id_order = FieldIds} = Schema = schema_manager:get_type(TypeId),
    case KeepBinaryObject of
        false -> 
            read_complex_object(Bin, Flag, Body, SchemaOffset, Version, Schema, Options, Values, Next);
        _ -> 
            read_binary_object(Bin, Flag, Body, SchemaOffset, FieldIds, Options, Values, Next)
    end;

do_read(?wrapped_data_code, 
        <<Len:?sint_spec, Binary:Len/binary, Offset:?sint_spec, Bin/binary>>, 
        #read_option{keep_wrapped = KeepWrapped} = Options,
        Values,
        Next) ->
    case KeepWrapped of
        false ->
            <<_:Offset/binary, Body/binary>> = Binary,
            Value = judgement(Body, Options, [], return);
        _ ->
            Value = #wrapped{binary = Binary, offset = Offset}
    end,    
    jump(Next, Bin, Options, [Value | Values]);

do_read(?binary_enum_code, <<_:?sint_spec, Value:?sint_spec, Bin/binary>>, Options, Values, Next) -> 
    jump(Next, Bin, Options, [Value | Values]).

%%----Specified read -------------------------------------------------------------------
read_primitive(byte, <<Byte:?sbyte_spec, Bin/binary>>, Options, Values, Next) -> 
    jump(Next, Bin, Options, [Byte | Values]);

read_primitive(short, <<Short:?sshort_spec, Bin/binary>>, Options, Values, Next) -> 
    jump(Next, Bin, Options, [Short | Values]);

read_primitive(int, <<Int:?sint_spec, Bin/binary>>, Options, Values, Next) -> 
    jump(Next, Bin, Options, [Int | Values]);

read_primitive(long, <<Long:?slong_spec, Bin/binary>>, Options, Values, Next) -> 
    jump(Next, Bin, Options, [Long | Values]);

read_primitive(float, <<Float:?sfloat_spec, Bin/binary>>, Options, Values, Next) -> 
    jump(Next, Bin, Options, [Float | Values]);

read_primitive(double, <<Double:?sdouble_spec, Bin/binary>>, Options, Values, Next) -> 
    jump(Next, Bin, Options, [Double | Values]);

read_primitive(char, <<Char?char_spec, Bin/binary>>, Options, Values, Next) -> 
    jump(Next, Bin, Options, [Char | Values]);

read_primitive(bool, <<Bool:?bool_spec, Bin/binary>>, Options, Values, Next) -> 
    jump(Next, Bin, Options, [utils:from_raw_bool(Bool) | Values]).

read_primitive_array(Type, <<Len:?sint_spec, Bin/binary>>, Options, Values, Next) -> 
    read_primitive(Type, Bin, Options, [], {primitive_array, Type, Len, Values, Next}).

read_array(<<Len:?sint_spec, Bin/binary>>, Options, Values, Next) -> 
    judgement(Bin, Options, [], {array, Len, Values, Next}).

list_to_map(MapType, List) ->
    Pairs = list_to_pairs(List, []),
    if MapType =:= 1 ->
            maps:from_list(Pairs);
       true ->
            orddict:from_list(Pairs)
    end.

list_to_pairs([V, K | T], Acc) ->
    list_to_pairs(T, [{K, V} | Acc]);
list_to_pairs([], Acc) ->
    Acc.

read_complex_object(<<Bin/binary>>, Flag, Body, SchemaOffset, Version, Schema, Options, Values, Next) ->
    #type_schema{version = NoVersion,
                 type_type = TypeType,
                 type_tag = TypeTag,
                 field_keys = FieldKeys,
                 constructor = Constructor,
                 on_upgrades = OnUpgrades} = Schema,

    case has_schema(Flag) of
        false ->  OriginValues = [];
        _ ->
            %% read all values
            <<ValueBody:SchemaOffset/binary, _/binary>> = Body, 
            OriginValues = read_array(ValueBody, Options, [], return)            
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
    Value = make_object(Constructor, TypeType, TypeTag, FieldKeys, Values),
    jump(Next, Bin, Options, [Value | Values]).

read_binary_object(<<Bin/binary>>, Flag, Body, SchemaOffset, FieldIds, Options, Values, Next) ->
    Len = erlang:byte_size(Body),
    SchemaData = erlang:binary_part(Body, SchemaOffset, Len - SchemaOffset),
    OffsetType = get_offset_type(Flag),
    case is_compact_footer(Flag) of
        true ->
            OffsetR = read_primitive_array(OffsetType, SchemaData, Options, [], return),
            Offsets = lists:map(fun(E) -> E - ?COMPLEX_OBJECT_HEAD_OFFSET end, OffsetR),
            Fields = lists:zip(FieldIds, Offsets),
            Schemas = maps:from_list(Fields);
        _ ->
            Fields = read_field_footer(SchemaData, OffsetType, []),
            Schemas = maps:from_list(Fields)
    end,
    Value = #binary_object{body = Body, schemas = Schemas},
    jump(Next, Bin, Options, [Value | Values]).

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

read_field_footer(<<>>, _, Acc) ->
    lists:reverse(Acc);

read_field_footer(<<FieldId:?sint_spec, SchemaData/binary>>, OffsetType, Acc) ->
    case OffsetType of
        byte ->
            <<OffsetT:?sbyte_spec, SchemaData2/binary>> = SchemaData,
            read_field_footer(SchemaData2, OffsetType, [{FieldId, OffsetT - ?COMPLEX_OBJECT_HEAD_OFFSET} | Acc]);
        int ->
            <<OffsetT:?sint_spec, SchemaData2/binary>> = SchemaData,
            read_field_footer(SchemaData2, OffsetType, [{FieldId, OffsetT - ?COMPLEX_OBJECT_HEAD_OFFSET} | Acc])
    end.

%%---- API functions-------------------------------------------------------------------
read_value(Bin) -> read_value(Bin, []).

read_value(Bin, Options) -> erlang:element(1, read(Bin, Options)).

read(Bin) -> read(Bin, []).

read(Bin, #read_option{} = Options) ->
    judgement(Bin, Options, [], return);

read(Bin, Option) ->
    Options = utils:parse_read_options(Option),
    judgement(Bin, Options, [], return).
