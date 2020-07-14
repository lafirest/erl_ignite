-module(schema).

-compile({parse_transform, category}).

-include("schema.hrl").
-include("type_binary_spec.hrl").

-export([read/2, write/2]).

read(<<Exists:?sbyte_spec, Bin/binary>>, ReadOption) ->
    case Exists of
        0 -> undefined;
        _ ->
            <<TypeId:?sint_spec, Body/binary>> = Bin,
            {TypeName, Body2T} = ignite_decoder:read(Body, ReadOption),
            {AffinityKey, Body2} = ignite_decoder:read(Body2T, ReadOption),
            <<FieldCount:?sint_spec, Body3/binary>> = Body2,
            {BinaryFields, Body4} = 
            loop:dotimes(fun({FieldAcc, DataAcc}) ->
                                 {Name, <<FieldTypeCode:?sint_spec,
                                          FieldNameHash:?sint_spec,
                                          DataAcc3/binary>>} = ignite_decoder:read(DataAcc, ReadOption),
                                 Field = #{name => Name,
                                           type_code => FieldTypeCode,
                                           name_hash => FieldNameHash},
                                 {[Field | FieldAcc], DataAcc3}
                         end,
                         FieldCount,
                         {[], Body3}),
            <<IsEnum:?sbyte_spec, Body5/binary>> = Body4,
            case IsEnum of
                1 -> 
                    <<EnumCount:?sint_spec, Body6>> = Body5,
                    {Enums, _} =
                    loop:dotimes(fun({PairAcc, DataAcc}) -> 
                                         {Name, <<Value:?sint_spec, DataAcc3/binary>>} = ignite_decoder:read(DataAcc, ReadOption),
                                         {[{Name, Value} | PairAcc], DataAcc3}
                                 end,
                                 EnumCount, 
                                 {[], Body6}),
                    #{type_id   => TypeId,
                      type_name => TypeName,
                      enums   => lists:reverse(Enums)};
                _ ->
                    <<SchemaCount:?sint_spec, Body6/binary>> = Body5,
                    {Schemas, _} =
                    loop:dotimes(fun({SchemaAcc, <<SchemaId:?sint_spec, FCount:?sint_spec, DataAcc2/binary>>}) -> 
                                         {Fields, DataAcc3} = 
                                         loop:dotimes(fun({FieldAcc, <<FId:?sint_spec, IDataAcc2/binary>>}) -> 
                                                              {[FId | FieldAcc], IDataAcc2}
                                                      end,
                                                      FCount, 
                                                      {[], DataAcc2}),
                                         Schema = #{schema_id => SchemaId,
                                                    fields => lists:reverse(Fields)},
                                         {[Schema | SchemaAcc], DataAcc3}
                                 end,
                                 SchemaCount,
                                 {[], Body6}),
                    #{type_id      => TypeId,
                      type_name    => TypeName,
                      affinity_key => AffinityKey,
                      fields       => lists:reverse(BinaryFields),
                      schemas      => Schemas}
            end
    end.

write(#enum_schema{type_id = TypeId, type_name = TypeName, values = Values}, WriteOption) ->
    Len = erlang:length(Values),
    [m_identity ||
     ignite_encoder:write({string, TypeName}, <<TypeId:?sint_spec>>),
     unit(<<_/binary, 0:?sint_spec, 1:?sbyte_spec, Len:?sint_spec>>),
     lists:foldl(fun({_Atom, Value, Name}, DataAcc) ->
                         DataAcc2 = ignite_encoder:write({string, Name}, DataAcc, WriteOption),
                         <<DataAcc2/binary, Value:?sint_spec>>
                 end, 
                 _,
                 Values),
     unit(<<_/binary, 0:?sint_spec>>)];

write(#{type_name := TypeName,
        affinity_key := AffinityKey,
        fields := Fields,
        schemas := Schemas},
      WriteOption) ->
    TypeId = utils:hash_name(TypeName),
    FieldCnt = erlang:length(Fields),
    SchemaCnt = erlang:length(Schemas),
    [m_identity ||
     ignite_encoder:write({string, TypeName}, <<TypeId:?sint_spec>>, WriteOption),
     ignite_encoder:write({string, AffinityKey}, _, WriteOption),
     lists:foldl(fun(#{name := Name, type_code := TypeCode, name_hash := NameHash}, DataAcc) ->
                         DataAcc2 = ignite_encoder:write({string, Name}, DataAcc, WriteOption),
                         <<DataAcc2/binary, TypeCode:?sint_spec, NameHash:?sint_spec>>
                 end,
                 <<_/binary, FieldCnt:?sint_spec>>,
                 Fields),
     lists:foldl(fun(FieldNames, DataAcc) -> 
                         SchemaId = utils:calculate_schemaId(FieldNames),
                         FieldIdLen = erlang:length(FieldNames),
                         lists:foldl(fun(FieldName, IDataAcc) -> 
                                             FieldId = utils:hash_name(FieldName),
                                             <<IDataAcc/binary, FieldId:?sint_spec>>
                                     end,
                                     <<DataAcc/binary, SchemaId:?sint_spec, FieldIdLen:?sint_spec>>,
                                     FieldNames)
                 end,
                 <<_/binary, 0:?sbyte_spec, SchemaCnt:?sint_spec>>,
                 Schemas)
    ].
