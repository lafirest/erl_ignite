-module(query_entity).
-compile({parse_transform, category}).

-include("operation.hrl").
-include("type_binary_spec.hrl").

-export([read/1, write/2]).

read(<<?match_string(KTNLen, KeyTypeName),
       ?match_string(VTNLen, ValueTypeName),
       ?match_string(TNLen, TableName),
       ?match_string(KFNLen, KeyFieldName),
       ?match_string(VFNLen, ValueFieldName),
       QueryFieldCnt:?sint_spec,
       Bin/binary>>) ->
    {QueryFieldR, <<AliasCount:?sint_spec, Bin2/binary>>} = 
    loop:dotimes(fun({QueryFieldAcc, 
                      <<?match_string(NameLen, QFName),
                        ?match_string(QFTableNameLen, QFTableName),
                        IsKey:?sbyte_spec,
                        IsNotNull:?sbyte_spec,
                        DataAcc2>>}) ->
                         Field = #{name => QFName,
                                   table => QFTableName,
                                   is_key => utils:from_raw_bool(IsKey),
                                   is_not_null => utils:from_raw_bool(IsNotNull)},
                         {[Field | QueryFieldAcc], DataAcc2}
                 end,
                 QueryFieldCnt,
                 {[], Bin}),
    QueryFields = lists:reverse(QueryFieldR),

    {AliasList, <<QueryIndexCnt:?sint_spec, Bin3/binary>>} =
    loop:dotimes(fun({AliasAcc, <<?match_string(NameLen, Name), ?match_string(AliasLen, Alias), DataAcc2/binary>>}) -> 
                         Alias = {Name, Alias},
                         {[Alias | AliasAcc], DataAcc2}
                 end,
                 AliasCount,
                 {[], Bin2}),

    {QueryIndexs, _} =
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
                 {[], Bin3}),
    #{key_type_name    => KeyTypeName,
      value_type_name  => ValueTypeName,
      table_name       => TableName,
      key_field_name   => KeyFieldName,
      value_field_name => ValueFieldName,
      query_fields     => QueryFields,
      alias            => AliasList,
      query_indexs     => QueryIndexs}.

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
                       is_not_null := IsNotNull}, DataAcc) -> 
                         [m_identity ||
                          ignite_encoder:write({string, QFName}, DataAcc),
                          ignite_encoder:write({string, QFTableName}, _),
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
