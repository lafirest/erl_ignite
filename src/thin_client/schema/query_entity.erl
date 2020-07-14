-module(query_entity).
-compile({parse_transform, category}).

-include("operation.hrl").
-include("type_binary_spec.hrl").

-export([read/2, write/2]).

read(Bin, Option) ->
    {KeyTypeName, Bin2} = ignite_decoder:read(Bin, Option),
    {ValueTypeName, Bin3} = ignite_decoder:read(Bin2, Option),
    {TableName, Bin4} = ignite_decoder:read(Bin3, Option),
    {KeyFieldName, Bin5} = ignite_decoder:read(Bin4, Option),
    {ValueFieldName, Bin6} = ignite_decoder:read(Bin5, Option),
    <<QueryFieldCnt:?sint_spec, Bin7/binary>> = Bin6,

    {QueryFieldR, <<AliasCount:?sint_spec, Bin8/binary>>} = 
    loop:dotimes(fun({QueryFieldAcc, DataAcc}) ->
                         {QFName, DataAcc2} = ignite_decoder:read(DataAcc, Option),
                         {QFTableName, DataAcc3} = ignite_decoder:read(DataAcc2, Option),
                         <<IsKey:?sbyte_spec, IsNotNull:?sbyte_spec, DataAcc4/binary>> = DataAcc3,
                         {DefaultVal, <<Precision:?sint_spec, Scale:?sint_spec, DataAcc5/binary>>} = ignite_decoder:read(DataAcc4, Option),
                         Field = #{name => QFName,
                                   table => QFTableName,
                                   is_key => utils:from_raw_bool(IsKey),
                                   is_not_null => utils:from_raw_bool(IsNotNull),
                                   default_val => DefaultVal,
                                   precision => Precision,
                                   scale => Scale},
                         {[Field | QueryFieldAcc], DataAcc5}
                 end,
                 QueryFieldCnt,
                 {[], Bin7}),
    QueryFields = lists:reverse(QueryFieldR),

    {AliasList, <<QueryIndexCnt:?sint_spec, Bin9/binary>>} =
    loop:dotimes(fun({AliasAcc, DataAcc}) ->
                         {FName, DataAcc2} = ignite_decoder:read(DataAcc, Option),
                         {FAlias, DataAcc3} = ignite_decoder:read(DataAcc2, Option),
                         Alias = {FName, FAlias},
                         {[Alias | AliasAcc], DataAcc3}
                 end,
                 AliasCount,
                 {[], Bin8}),

    {QueryIndexs, BinA} =
    loop:dotimes(fun({QueryIndexAcc, 
                      <<?match_string(INLen, IndexName),
                        RawIndexType:?sbyte_spec,
                        InlineSize:?sint_spec,
                        FieldCnt:?sint_spec,
                        DataAcc/binary>>}) -> 

                         {Fields, DataAcc2} =
                         loop:dotimes(fun({FieldAcc, 
                                           <<?match_string(NameLen, Name), 
                                             RawIsDescensing:?sbyte_spec,
                                             IDataAcc/binary>>}) -> 
                                              Field = #{name => Name,
                                                        is_descensing => utils:from_raw_bool(RawIsDescensing)},
                                              {[Field | FieldAcc], IDataAcc}
                                      end,
                                      FieldCnt,
                                      {[], DataAcc}),

                         Index = #{name        => IndexName,
                                   type        => from_raw_index_type(RawIndexType),
                                   inline_size => InlineSize,
                                   fields      => Fields},

                         {[Index | QueryIndexAcc], DataAcc2}

                 end,
                 QueryIndexCnt,
                 {[], Bin9}),
    {#{key_type_name    => KeyTypeName,
       value_type_name  => ValueTypeName,
       table_name       => TableName,
       key_field_name   => KeyFieldName,
       value_field_name => ValueFieldName,
       query_fields     => QueryFields,
       alias            => AliasList,
       query_indexs     => QueryIndexs},
     BinA}.

write(#{key_type_name    := KeyTypeName,
        value_type_name  := ValueTypeName,
        table_name       := TableName,
        key_field_name   := KeyFieldName,
        value_field_name := ValueFieldName,
        query_fields     := QueryFields,
        alias            := AliasList,
        query_indexs     := QueryIndexs},
      Bin) ->
    QueryFieldCnt = erlang:length(QueryFields),
    AliasCount = erlang:length(AliasList),
    QueryIndexCnt = erlang:length(QueryIndexs),
    [m_identity ||
     ignite_encoder:write({string, KeyTypeName}, Bin),
     ignite_encoder:write({string, ValueTypeName}, _),
     ignite_encoder:write({string, TableName}, _),
     ignite_encoder:write({string, KeyFieldName}, _),
     ignite_encoder:write({string, ValueFieldName}, _),
     lists:foldl(fun(#{name := QFName,
                       table := QFTableName,
                       is_key := IsKey,
                       is_not_null := IsNotNull} = FieldMap, DataAcc) -> 
                        DefulatValue = maps:get(default_val, FieldMap, undefined),
                        Precision = maps:get(precision, FieldMap, -1),
                        Scale = maps:get(scale, FieldMap, -1),
                         [m_identity ||
                          ignite_encoder:write({string, QFName}, DataAcc),
                          ignite_encoder:write({string, QFTableName}, _),
                          ignite_encoder:write(DefulatValue, _),
                          ignite_encoder:write({int, Precision}, _),
                          ignite_encoder:write({int, Scale}, _),
                          unit(<<_/binary, (utils:to_raw_bool(IsKey)):?sbyte_spec, (utils:to_raw_bool(IsNotNull)):?sbyte_spec>>)]
                 end,
                 <<_/binary, QueryFieldCnt:?sint_spec>>,
                 QueryFields),

     lists:foldl(fun({AName, AAlias}, DataAcc) -> 
                         DataAcc2 = ignite_encoder:write({string, AName}, DataAcc),
                         ignite_encoder:write({string, AAlias}, DataAcc2)
                 end,
                 <<_/binary, AliasCount:?sint_spec>>,
                 AliasList),

     lists:foldl(fun(#{name := IndexName,
                       type := Type,
                       inline_size := InlineSize,
                       fields := Fields},
                     DataAcc) -> 
                         FieldCnt = erlang:length(Fields),
                         [m_identity ||
                          ignite_encoder:write({string, IndexName}, DataAcc),
                          unit(<<_/binary, (to_raw_index_type(Type)):?sbyte_spec, InlineSize:?sint_spec, FieldCnt:?sint_spec>>),
                          lists:foldl(fun(#{name := FieldName,
                                            is_descensing := IsDescensing},
                                          IDataAcc) -> 
                                              IDataAcc2 = ignite_encoder:write({string, FieldName}, IDataAcc),
                                              <<IDataAcc2/binary, (utils:to_raw_bool(IsDescensing)):?sbyte_spec>>
                                      end,
                                      _,
                                      Fields)]
                 end,
                 <<_/binary, QueryIndexCnt:?sint_spec>>,
                 QueryIndexs)].

from_raw_index_type(0) -> sorted;
from_raw_index_type(1) -> fulltext;
from_raw_index_type(2) -> geospatial.

to_raw_index_type(sorted) -> 0;
to_raw_index_type(fulltext) -> 1;
to_raw_index_type(geospatial) -> 2.
