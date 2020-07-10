%%----KV Cache Operation----------------------------------------------------------
-define(OP_CACHE_GET, 1000).
-define(OP_CACHE_PUT, 1001).
-define(OP_CACHE_PUT_IF_ABSENT, 1002).
-define(OP_CACHE_GET_ALL, 1003).
-define(OP_CACHE_PUT_ALL, 1004).
-define(OP_CACHE_GET_AND_PUT, 1005).
-define(OP_CACHE_GET_AND_REPLACE, 1006).
-define(OP_CACHE_GET_AND_REMOVE, 1007).
-define(OP_CACHE_GET_AND_PUT_IF_ABSENT, 1008).
-define(OP_CACHE_REPLACE, 1009).
-define(OP_CACHE_REPLACE_IF_EQUALS, 1010).
-define(OP_CACHE_CONTAINS_KEY, 1011).
-define(OP_CACHE_CONTAINS_KEYS, 1012).
-define(OP_CACHE_CLEAR, 1013).
-define(OP_CACHE_CLEAR_KEY, 1014).
-define(OP_CACHE_CLEAR_KEYS, 1015).
-define(OP_CACHE_REMOVE_KEY, 1016).
-define(OP_CACHE_REMOVE_IF_EQUALS, 1017).
-define(OP_CACHE_REMOVE_KEYS, 1018).
-define(OP_CACHE_REMOVE_ALL, 1019).
-define(OP_CACHE_GET_SIZE, 1020).

%%----SQL Operation----------------------------------------------------------
-define(OP_QUERY_SQL, 2002).
-define(OP_QUERY_SQL_CURSOR_GET_PAGE, 2003).
-define(OP_QUERY_SQL_FIELDS, 2004).
-define(OP_QUERY_SQL_FIELDS_CURSOR_GET_PAGE, 2005).
-define(OP_QUERY_SCAN, 2000).
-define(OP_QUERY_SCAN_CURSOR_GET_PAGE, 2001).
-define(OP_RESOURCE_CLOSE, 0).

%%----Binary Type Operation----------------------------------------------------------
-define(OP_GET_BINARY_TYPE_NAME, 3000).
-define(OP_REGISTER_BINARY_TYPE_NAME, 3001).
-define(OP_GET_BINARY_TYPE, 3002).
-define(OP_PUT_BINARY_TYPE, 3003).
