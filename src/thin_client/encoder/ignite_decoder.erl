-module(ignite_decoder).

-export([read/1]).

%%----Type Define Spec----------------------------------------------------------
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
%% I think this is unnecessary
%% -define(collection_code, 24).
-define(map_code, 25).
-define(enum_array_code, 29).
-define(complex_object_code, 103).

%%----Write -------------------------------------------------------------------
read(<<Code:?sbyte_spec, Bin>>) -> read(Code, Bin).

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

read(?string_code, <<Len:?sint_spec, Bin>>) ->
    <<String:Len/binary, Bin2/binary>> = Bin,
    {String, Bin2};

read(?uuid_code, <<UUID:16/binary, Bin/binary>>) -> {UUID, Bin};

%% I think msec_fraction_in_nsecs is unnecessary
read(?timestamp_code, <<Msecs:?slong_spec, _:?sint_spec, Bin/binary>>) -> {qdate:to_date(Msecs / 1000), Bin};

read(?date_code, <<Msecs:?slong_spec, Bin/binary>>) -> {qdate:to_date(Msecs / 1000), Bin};

read(?time_code, <<Value:?slong_spec, Bin/binary>>) -> {calendar:second_to_time(Value / 1000), Bin};

read(?enum_code, <<_:?sint_spec, Value:?sint_spec, Bin/binary>>) -> {Value, Bin};

%% for performance, byte array shoud be binary, not byte list
read(?byte_array_code, <<Len:?sint_spec, ByteArray:Len/binary, Bin/binary>>) -> {ByteArray, Bin};

read(?short_array_code, Bin) -> read_array(?short_code, Bin); 

read(?int_array_code, Bin) -> read_array(?int_code, Bin); 

read(?long_array_code, Bin) -> read_array(?long_code, Bin); 

read(?float_array_code, Bin) -> read_array(?float_code, Bin); 

read(?double_array_code, Bin) -> read_array(?double_code, Bin); 

read(?char_array_code, Bin) -> read_array(?char_code, Bin); 

read(?bool_array_code, Bin) -> read_array(?bool_code, Bin); 

read(?string_array_code, Bin) -> read_array(?string_code, Bin);

read(?uuid_array_code, Bin) -> read_array(?uuid_code, Bin);

read(?time_array_code, Bin) -> read_array(?timestamp_code, Bin);

read(?date_code, Bin) -> read_array(?date_code, Bin);

read(?time_array_code, Bin) ->  read_array(?time_code, Bin);

%% TODO
read({object_array, TypeName, Array}, Bin) ->
    Bin;

read({map, Map}, Bin) ->
    Bin;

read({linked_hash_map, Map}, Bin) ->
    Bin;

read(?enum_array_code, <<_:?sint_spec, Bin/binary>>) -> read_array(?enum_code, Bin);

read({complex_object, TypeName, Fields}, Bin) ->
    Bin.

read_array(Type, <<Len:?sint_spec, Bin/binary>>) ->
    read_array2(Type, Bin, Len, 0, []).

read_array2(_, _, Len, Len, Acc) -> lists:reverse(Acc);
read_array2(Type, Bin, Len, I, Acc) -> 
    {Value, Bin2} = read(Type, Bin),
    read_array2(Type, Bin2, Len, I + 1, [Value | Acc]).




    
