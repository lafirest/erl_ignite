-module(schema).

-compile({parse_transform, category}).

-include("schema.hrl").
-include("type_binary_spec.hrl").

-export([read/1, write/1]).

read(<<Exists:?sbyte_spec, Bin/binary>>) ->
    case Exists of
        0 -> undefined;
        _ ->
            <<TypeId:?sint_spec, Body/binary>> = Bin,
            {TypeName, Body2T} = ignite_reader:read(Body),
            {AffinityKey, Body2} = ignite_reader:read(Body2T),
            <<FieldCount:?sint_spec, Body3/binary>> = Body2,
            {BinaryFields, Body4} = 
            loop:dotimes(fun({FieldAcc, DataAcc}) ->
                                 {Name, <<FieldTypeHash:?sint_spec,
                                          FieldNameHash:?sint_spec,
                                          DataAcc3/binary>>} = ignite_reader:read(DataAcc),
                                 Field = #{name => Name,
                                           type_hash => FieldTypeHash,
                                           name_hash => FieldNameHash},
                                 {[Field | FieldAcc], DataAcc3}
                         end,
                         FieldCount,
                         {[], Body3}),
            <<IsEnum:?sbyte_spec, Body5>> = Body4,
            case IsEnum of
                1 -> 
                    <<EnumCount:?sint_spec, Body6>> = Body5,
                    {Enums, _} =
                    loop:dotimes(fun({PairAcc, DataAcc}) -> 
                                         {Name, <<Value:?sint_spec, DataAcc3/binary>>} = ignite_reader:read(DataAcc),
                                         {[{Name, Value} | PairAcc], DataAcc3}
                                 end,
                                 EnumCount, 
                                 {[], Body6}),
                    #{type_id   => TypeId,
                      type_name => TypeName,
                      enums   => lists:reverse(Enums)};
                _ ->
                    <<SchemaCount:?sint_spec, Body6>> = Body5,
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

write(#enum_schema{type_id = TypeId, type_name = TypeName, values = Values}) ->
    Len = erlang:length(Values),
    [m_identity ||
     ignite_encoder:write({string, TypeName}, <<TypeId:?sint_spec>>),
     unit(<<_/binary, 0:?sint_spec, 1:?sbyte_spec, Len:?sint_spec>>),
     lists:foldl(fun({_Atom, Value, Name}, DataAcc) ->
                         DataAcc2 = ignite_encoder:write(Name, DataAcc),
                         <<DataAcc2/binary, Value:?sint_spec>>
                 end, 
                 _,
                 Values),
     unit(<<_/binary, 0:?sint_spec>>)];


write(#{type_name := TypeName,
        affinity_key := AffinityKey,
        fields := Fields,
        schemas := Schemas}) ->
    TypeId = utils:hash_name(TypeName),
    FieldCnt = erlang:length(Fields),
    SchemaCnt = erlang:length(Schemas),
    [m_identity ||
     ignite_encoder:write({string, TypeName}, <<TypeId:?sint_spec>>),
     ignite_encoder:write({string, AffinityKey}, _),
     lists:foldl(fun(#{name := Name, type_hash := TypeHash, name_hash := NameHash}, DataAcc) ->
                         DataAcc2 = ignite_encoder:write({string, Name}, DataAcc),
                         <<DataAcc2/binary, TypeHash:?sint_spec, NameHash:?sint_spec>>
                 end,
                 <<_/binary, FieldCnt:?sint_spec>>,
                 Fields),
     lists:foldl(fun(#{schema_id := SchemaId, fields := FieldIds}, DataAcc) -> 
                         FieldIdLen = erlang:length(FieldIds),
                         lists:foldl(fun(FieldId, IDataAcc) -> 
                                             <<IDataAcc/binary, FieldId:?sint_spec>>
                                     end,
                                     <<DataAcc/binary, SchemaId:?sint_spec, FieldIdLen:?sint_spec>>,
                                     Fields)
                 end,
                 <<_/binary, 0:?sbyte_spec, SchemaCnt:?sint_spec>>,
                 Schemas)
    ].
