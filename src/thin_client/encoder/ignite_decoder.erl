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
    Bool = if Value =:= 1 -> true;
              true -> false
            end,
    {Bool, Bin};

read(?null_code, Bin) -> {undefined, Bin};

read(?string_code, <<Len:?sint_spec, Bin/binary>>) ->
    <<String:Len/binary, Bin2/binary>> = Bin,
    {String, Bin2};

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

read(?time_array_code, Bin) -> read_array(Bin);

read(?date_array_code, Bin) -> read_array(Bin);

read(?time_array_code, Bin) ->  read_array(Bin);

read(?object_array_code, <<TypeId:?sint_spec, Len:?sint_spec, Bin/binary>>) ->
    read_array(Bin),
    %% TODO from_spec
    Bin;

read({map, Map}, Bin) ->
    Bin;

read(?enum_array_code, <<_:?sint_spec, Bin/binary>>) -> read_array(Bin);

read(?complex_object_code, 
     <<_:?sbyte_spec, 
       Flag:?sshort_spec,
       TypeId:?sint_spec,
       _:?sint_spec,
       Len:?sint_spec,
       _:?sint_spec,
       SchemaOffsetT:?sint_spec,
       Body:(Len - 24)/binary,
       Bin/binary>>) ->
    SchemaOffset = SchemaOffsetT - 24,
    #type_schema{type_id = TypeId,
                 field_ids = FieldIds,
                 from_reader = FromReader} = schema_manager:get_type(TypeId),
    Values =
    if Flag band ?HAS_SCHEMA =:= 0 -> {[], Bin};
       true ->
           {ValuesR, _} =
           lists:foldl(fun(_, {ValuesAcc, BinAcc}) ->
                         {Value, BinAcc2} = read(BinAcc),
                         {[Value | ValuesAcc], BinAcc2}
                       end,
                       {[], Body},
                       FieldIds),
           {FromReader(lists:reverse(ValuesR)), Bin}
    end;

read(?wrapped_data_code, <<Len:?sint_spec, Binary:Len/binary, Offset:?sint_spec, Bin/binary>>) ->
    Value = read_value(Binary),
    {Value, Bin};
%    {{wrapped, Binary, Offset}, Bin};

read(?binary_enum_code, <<_:?sint_spec, Value:?sint_spec, Bin/binary>>) -> {Value, Bin};

%%----Specified read -------------------------------------------------------------------
read(short, <<Short:?sshort_spec, Bin/binary>>) -> {Short, Bin};
read(int, <<Int:?sint_spec, Bin/binary>>) -> {Int, Bin};
read(long, <<Long:?slong_spec, Bin/binary>>) -> {Long, Bin};
read(float, <<Float:?sfloat_spec, Bin/binary>>) -> {Float, Bin};
read(double, <<Double:?sdouble_spec, Bin/binary>>) -> {Double, Bin};
read(char, <<Char?char_spec, Bin/binary>>) -> {Char, Bin};
read(bool, <<Bool:?bool_spec, Bin/binary>>) -> {Bool, Bin}.

read_array_with_type(Type, <<Len:?sint_spec, Bin/binary>>) -> 
    read_array_with_type2(Type, Bin, Len, []).

read_array_with_type2(_, BinAcc, 0, Acc) -> {lists:reverse(Acc), BinAcc};
read_array_with_type2(Type, Bin, I, Acc) -> 
    {Value, Bin2} = read(Type, Bin),
    read_array_with_type2(Type, Bin2, I - 1, [Value | Acc]).

read_array(<<Len:?sint_spec, Bin/binary>>) -> read_array2(Bin, Len, []).
read_array2(BinAcc, 0, Acc) -> {lists:reverse(Acc), BinAcc};
read_array2(Bin, I, Acc) -> 
    {Value, Bin2} = read(Bin),
    read_array2(Bin2, I - 1, [Value | Acc]).
