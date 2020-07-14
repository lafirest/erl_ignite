-module(configuration).
-compile({parse_transform, category}).

-include("operation.hrl").
-include("type_binary_spec.hrl").

-export([read/2, write/2]).

read(<<RawAtomicityMode:?sint_spec, Backups:?sint_spec, RawMode:?sint_spec, RawCopyOnRead:?sbyte_spec, Bin/binary>>, Option) ->
    {DataRegionName, Bin2} = ignite_decoder:read(Bin, Option),
    <<RawEagerTTL:?sbyte_spec,
      RawStatisticsEnabled:?sbyte_spec, 
      Bin3/binary>> = Bin2,

    {GroupName, Bin4} = ignite_decoder:read(Bin3, Option),

    <<DefaultLockTimeout:?slong_spec,
      MaxConcurrentAsyncOperations:?sint_spec,
      MaxQueryIterators:?sint_spec, 
      Bin5/binary>> = Bin4,

    {Name, Bin6} = ignite_decoder:read(Bin5, Option),

    <<RawIsOnheapCacheEnabled:?sbyte_spec,
      RawPartitionLossPolicy:?sint_spec,
      QueryDetailMetricsSize:?sint_spec,
      QueryParellelism:?sint_spec,
      RawReadFromBackup:?sbyte_spec,
      RebalanceBatchSize:?sint_spec,
      RebalanceBatchesPrefetchCount:?slong_spec,
      RebalanceDelay:?slong_spec,
      RawRebalanceMode:?sint_spec,
      RebalanceOrder:?sint_spec,
      RebalanceThrottle:?slong_spec,
      RebalanceTimeout:?slong_spec,
      RawSqlEscapeAll:?sbyte_spec,
      SqlIndexInlineMaxSize:?sint_spec, 
      Bin7/binary>> = Bin6,

    {SqlSchema, Bin8} = ignite_decoder:read(Bin7, Option),

    <<RawWriteSynchronizationMode:?sint_spec,
      CacheKeyConfigurationCnt:?sint_spec,
      Bin9/binary>> = Bin8,

    {KeyCfgs, BinA} =
    loop:dotimes(fun({KeyCfgAcc, DataAcc}) -> 
                         <<?match_string(TypeNameLen, TypeName),
                           ?match_string(AffinityKeyLen, AffinityKey),
                           DataAcc2/binary>> = DataAcc,
                         {[{TypeName, AffinityKey} | KeyCfgAcc], DataAcc2}
                 end,
                 CacheKeyConfigurationCnt,
                 {[], Bin9}),

    <<QueryEntityCnt:?sint_spec, BinB/binary>> = BinA,
    {Entities ,_} =
    loop:dotimes(fun({EntityAcc, DataAcc}) ->
                         {Entity, DataAcc2} = query_entity:read(DataAcc, Option),
                         {[Entity | EntityAcc], DataAcc2}
                 end,
                 QueryEntityCnt,
                 {[], BinB}),
    #{atomicity_mode                   => from_raw_atomicity_mode(RawAtomicityMode),
      backups                          => Backups,
      mode                             => from_raw_cache_mode(RawMode),
      copy_on_read                     => utils:from_raw_bool(RawCopyOnRead),
      data_region                      => DataRegionName,
      eager_ttl                        => utils:from_raw_bool(RawEagerTTL),
      statistics_enabled               => utils:from_raw_bool(RawStatisticsEnabled),
      group                            => GroupName,
      %      invalidate                       => utils:from_raw_bool(RawInvalidate),
      default_lock_timeout             => DefaultLockTimeout,
      max_concurrent_async_operations  => MaxConcurrentAsyncOperations,
      max_query_iterators              => MaxQueryIterators,
      name                             => Name,
      is_onheap_cache_enabled          => utils:from_raw_bool(RawIsOnheapCacheEnabled),
      partition_loss_policy            => from_raw_partition_loss_policy(RawPartitionLossPolicy),
      query_detail_metrics_size        => QueryDetailMetricsSize,
      query_parellelism                => QueryParellelism,
      read_from_backup                 => utils:from_raw_bool(RawReadFromBackup),
      rebalance_batch_size             => RebalanceBatchSize,
      rebalance_batches_prefetch_count => RebalanceBatchesPrefetchCount,
      rebalance_delay                  => RebalanceDelay,
      rebalance_mode                   => from_raw_rebalance_mode(RawRebalanceMode),
      rebalance_order                  => RebalanceOrder,
      rebalance_throttle               => RebalanceThrottle,
      rebalance_timeout                => RebalanceTimeout,
      sql_escape_all                   => utils:from_raw_bool(RawSqlEscapeAll),
      sql_index_inline_maxSize         => SqlIndexInlineMaxSize,
      sql_schema                       => SqlSchema,
      write_synchronization_mode       => from_raw_write_synchronization_mode(RawWriteSynchronizationMode),
      cache_key_configuration          => KeyCfgs,
      query_entities                   => Entities
     }.

write(Config, _) ->
    lists:foldl(fun({Key, Value}, Acc) ->
                        write(Key, Value, Acc)
                end,
                <<>>,
                maps:to_list(Config)).

write(mode, Value, Bin) ->
    <<Bin/binary, 1:?sshort_spec, (to_raw_cache_mode(Value)):?sint_spec>>;

write(atomicity_mode, Value, Bin) ->
    <<Bin/binary, 2:?sshort_spec, (to_raw_atomicity_mode(Value)):?sint_spec>>;

write(backups, Value, Bin) ->
    <<Bin/binary, 3:?sshort_spec, Value:?sint_spec>>;

write(copy_on_read, Value, Bin) ->
    <<Bin/binary, 5:?sshort_spec, (utils:to_raw_bool(Value)):?sbyte_spec>>;

write(data_region, Value, Bin) ->
    ignite_encoder:write({string, Value}, <<Bin/binary, 100:?sshort_spec>>);

write(eager_ttl, Value, Bin) ->
    <<Bin/binary, 405:?sshort_spec, (utils:to_raw_bool(Value)):?sbyte_spec>>;

write(statistics_enabled, Value, Bin) ->
    <<Bin/binary, 406:?sshort_spec, (utils:to_raw_bool(Value)):?sbyte_spec>>;

write(group, Value, Bin) ->
    ignite_encoder:write({string, Value}, <<Bin/binary, 400:?sshort_spec>>);

write(default_lock_timeout, Value, Bin) ->
    <<Bin/binary, 402:?sshort_spec, Value:?slong_spec>>;

write(max_concurrent_async_operations, Value, Bin) ->
    <<Bin/binary, 403:?sshort_spec, Value:?sint_spec>>;

write(max_query_iterators, Value, Bin) ->
    <<Bin/binary, 206:?sshort_spec, Value:?sint_spec>>;

write(name, Value, Bin) ->
    ignite_encoder:write({string, Value}, <<Bin/binary, 0:?sshort_spec>>);

write(is_onheap_cache_enabled, Value, Bin) ->
    <<Bin/binary, 101:?sshort_spec, (utils:to_raw_bool(Value)):?sbyte_spec>>;

write(partition_loss_policy, Value, Bin) ->
    <<Bin/binary, 404:?sshort_spec, (to_raw_partition_loss_policy(Value)):?sint_spec>>;

write(query_detail_metrics_size, Value, Bin) ->
    <<Bin/binary, 202:?sshort_spec, Value:?sint_spec>>;

write(query_parellelism, Value, Bin) ->
    <<Bin/binary, 201:?sshort_spec, Value:?sint_spec>>;

write(read_from_backup, Value, Bin) ->
    <<Bin/binary, 6:?sshort_spec, (utils:to_raw_bool(Value)):?sbyte_spec>>;

write(rebalance_batch_size, Value, Bin) ->
    <<Bin/binary, 303:?sshort_spec, Value:?sint_spec>>;

write(rebalance_batches_prefetch_count, Value, Bin) ->
    <<Bin/binary, 304:?sshort_spec, Value:?slong_spec>>;

write(rebalance_delay, Value, Bin) ->
    <<Bin/binary, 301:?sshort_spec, Value:?slong_spec>>;

write(rebalance_mode, Value, Bin) ->
    <<Bin/binary, 300:?sshort_spec, (to_raw_rebalance_mode(Value)):?sint_spec>>;

write(rebalance_order, Value, Bin) ->
    <<Bin/binary, 305:?sshort_spec, Value:?sint_spec>>;

write(rebalance_throttle, Value, Bin) ->
    <<Bin/binary, 306:?sshort_spec, Value:?slong_spec>>;

write(rebalance_timeout, Value, Bin) ->
    <<Bin/binary, 302:?sshort_spec, Value:?slong_spec>>;

write(sql_escape_all, Value, Bin) ->
    <<Bin/binary, 205:?sshort_spec, (utils:to_raw_bool(Value)):?sbyte_spec>>;

write(sql_index_inline_maxSize, Value, Bin) ->
    <<Bin/binary, 204:?sshort_spec, Value:?sint_spec>>;

write(sql_schema, Value, Bin) ->
    ignite_encoder:write({string, Value}, <<Bin/binary, 203:?sshort_spec>>);

write(write_synchronization_mode, Value, Bin) ->
    <<Bin/binary, 4:?sshort_spec, (to_raw_write_synchronization_mode(Value)):?sint_spec>>;

write(cache_key_configuration, Value, Bin) -> 
    Len = erlang:length(Value),
    lists:foldl(fun({TypeName, AffinityKey}, DataAcc) -> 
                        DataAcc2 = ignite_encoder:write({string, TypeName}, DataAcc),
                        ignite_encoder:write({string, AffinityKey}, DataAcc2)
                end,
                <<Bin/binary, 401:?sshort_spec, Len:?sint_spec>>,
                Value);

write(query_entities, Value, Bin) -> 
    Len = erlang:length(Value),
    lists:foldl(fun(Entity, DataAcc) -> 
                        query_entity:write(Entity, DataAcc)
                end,
                <<Bin/binary, 200:?sshort_spec, Len:?sint_spec>>).

to_raw_atomicity_mode(transactional) -> 0;
to_raw_atomicity_mode(atomic) -> 1.

from_raw_atomicity_mode(0) -> transactional;
from_raw_atomicity_mode(1) -> atomic.

from_raw_cache_mode(0) -> local;
from_raw_cache_mode(1) -> replicated;
from_raw_cache_mode(2) -> partitioned.

to_raw_cache_mode(local) -> 0;
to_raw_cache_mode(replicated) -> 1;
to_raw_cache_mode(partitioned) -> 2.

from_raw_partition_loss_policy(0) -> read_only_safe;
from_raw_partition_loss_policy(1) -> read_only_all;
from_raw_partition_loss_policy(2) -> read_write_safe;
from_raw_partition_loss_policy(3) -> read_write_all;
from_raw_partition_loss_policy(4) -> ignore.

to_raw_partition_loss_policy(read_only_safe) -> 0;
to_raw_partition_loss_policy(read_only_all) -> 1;
to_raw_partition_loss_policy(read_write_safe) -> 2;
to_raw_partition_loss_policy(read_write_all) -> 3;
to_raw_partition_loss_policy(ignore) -> 4.

from_raw_rebalance_mode(0) -> sync;
from_raw_rebalance_mode(1) -> async;
from_raw_rebalance_mode(2) -> none.

to_raw_rebalance_mode(sync) -> 0;
to_raw_rebalance_mode(async) -> 1;
to_raw_rebalance_mode(none) -> 2.

from_raw_write_synchronization_mode(0) -> full_sync;
from_raw_write_synchronization_mode(1) -> full_async;
from_raw_write_synchronization_mode(2) -> primary_sync.

to_raw_write_synchronization_mode(full_sync) -> 0;
to_raw_write_synchronization_mode(full_async) -> 1;
to_raw_write_synchronization_mode(primary_sync) -> 2.
