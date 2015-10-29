-module(drob_socket).

-export([connect/3,
	 close/1,
	 socket/1,
	 send_sync/2]).

-record(s, { socket  :: gen_tcp:socket(),
	     corr_id = 0:: integer(),
	     client_id :: binary(),
	     outstanding :: map(),
	     tail = <<>> :: binary() }).

-opaque socket() ::  #s{}.

-export_type([socket/0]).

-spec connect(Host, Port, ClientId) ->
		     {ok, socket()} when
      Host :: inet:ip_address()|inet:hostname(),
      Port :: inet:port_number(),
      ClientId :: binary().
connect(Host, Port, ClientId) ->
    case gen_tcp:connect(Host, Port, [{packet, raw},
				      {active , false},
				      binary]) of
	{ok, Socket} ->
	    {ok, #s{socket = Socket,
		    client_id = ClientId}};
	{error, _Reason} = Error ->
	    Error
    end.

-spec socket(socket()) -> gen_tcp:socket().
socket(#s{socket=Socket}) ->
    Socket.

-spec close(socket()) -> ok.
close(#s{socket=Socket}) ->
    gen_tcp:close(Socket).

send_sync(Request, #s{corr_id = CorrId,
		      socket = Socket,
		      client_id = ClientId,
		      tail = _Tail}) ->
    case send(Socket, Request, CorrId, ClientId) of
	{ok, ApiKey, CorrId1} ->
	    {ok, <<Size:32/signed-integer>>} = gen_tcp:recv(Socket, 4),
	    {ok, <<CorrId1:32/signed-integer, Bin/binary>>} =
		gen_tcp:recv(Socket, Size),
	    {ok, brod_kafka:decode(ApiKey, Bin)};
	Error ->
	    Error
    end.

send(Socket, Request, CorrId, ClientId) ->
    CorrId1 = next_curr_id(CorrId),
    ApiKey = brod_kafka:api_key(Request),
    RequestBin = brod_kafka:encode(ClientId, CorrId1, Request),
    case gen_tcp:send(Socket, RequestBin) of
	ok ->
	    {ok, ApiKey, CorrId1};
	{error, _Reason} = Error ->
	    Error
    end.

%% Internal
next_curr_id(2147483647) ->
    0;
next_curr_id(CurrId) ->
    CurrId + 1.

