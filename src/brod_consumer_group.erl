-module(brod_consumer_group).

-behaviour(gen_fsm).

%% This is WIP for consumer groups for Brod. It's designed agains the upcoming
%% 0.9.0 API. It is process heavy and should be refactored in the future to
%% be a single process managing a number of consumer sockets, instead of the
%% current design of a single process managing a number of consumer processes

% gen_fsm callbacks
-export([init/1,
	 handle_event/3,
	 handle_sync_event/4,
	 handle_info/3,
	 code_change/4,
	 terminate/3
	]).

% States
-export([member/2,
	 member/3,
	 joining_new/2,
	 joining_new/3,
	 illegal_generation/2,
	 illegal_generation/3,
	 rejoin/2,
	 rejoin/3,
	 rediscover_coordinator/2,
	 rediscover_coordinator/3
	]).

% API
-export([start/1,
	 start_test/1,
	 commit_offset]).

-export([consumer_group_coordinator/2]).

%%%_* Includes -----------------------------------------------------------------
-include("brod.hrl").
-include("brod_int.hrl").

%%%_* Macros -------------------------------------------------------------------
-define(RETRIES, 5).

%%%_* Records ------------------------------------------------------------------
-record(state, { group_id            :: string(),
		 topics :: [string()],
		 brokers :: [#broker_metadata{}],
		 psa :: atom(),
		 coordinator         :: #broker_metadata{},
		 ggi :: integer(),
		 consumer_id         :: string(),
		 consumers           :: [{pid(), {binary(), integer()}}],
		 session_timeout     :: integer(),
		 liveliness_ref      :: reference()
	       }).

%% API
start_test(GroupId) ->
    start(#{ hosts => [{"localhost", 9092}],
	     topics => [<<"test">>],
	     group_id => GroupId,
	     session_timeout => 30000,
	     partition_assignment_strategy => range}).
    
start(Opts) ->
    gen_fsm:start(?MODULE, [Opts], []).

init([#{ hosts := Hosts,
	 topics := Topics,
	 group_id := GroupId,
	 session_timeout := SessionTimeout,
	 partition_assignment_strategy := PSA}]) ->
    erlang:process_flag(trap_exit, true),
    Brokers = brokers(Hosts),
    {ok, joining_new, #state{group_id = GroupId,
			     topics = Topics,
			     brokers = Brokers,
			     psa = PSA,
			     session_timeout = SessionTimeout
			    }, 0}.

joining_new(timeout, #state{group_id = GroupId,
			    topics = Topics,
			    brokers = Brokers,
			    psa = PSA,
			    session_timeout = SessionTimeout} = State) ->
    case join_group(GroupId, Brokers, #{topics => Topics,
					session_timeout => SessionTimeout,
					psa => PSA,
					consumer_id => <<>>}) of
	{error, _Err} ->
	    % @todo define errors which do not allow us to continue
	    {next_state, joining_new, State, 0};
	{ok, #{ggi := GGI,
	       consumer_id := ConsumerId,
	       pto := PTO,
	       brokers := Brokers1,
	       coordinator := Coordinator}} ->
	    Consumers = start_consumers(PTO, Brokers1),
	    Ref = set_timer(SessionTimeout),
	    {next_state, member, State#state{consumers = Consumers,
					     brokers = Brokers1,
					     liveliness_ref = Ref,
					     coordinator = Coordinator,
					     ggi = GGI,
					     consumer_id = ConsumerId}}
    end.

joining_new(_Event, _From, State) ->
    {reply, not_ready, joining_new, State}.


rejoin(timeout, #state{group_id = GroupId,
		       topics = Topics,
		       brokers = Brokers,
		       psa = PSA,
		       session_timeout = SessionTimeout,
		       consumer_id = ConsumerId}=State) ->
    case join_group(GroupId, Brokers, #{topics => Topics,
					session_timeout => SessionTimeout,
					psa => PSA,
					consumer_id => ConsumerId}) of
	{error, 'UnknownConsumerId'} ->
	    % Coordinator doesn't know who we are. Join as new
	    {next_state, joining_new, State};
	{error, _Err} ->
	    % @todo define errors which do not allow us to continue
	    {next_state, rejoin, State, 0};
	{ok, #{ggi := GGI,
	       consumer_id := ConsumerId1,
	       pto := PTO,
	       brokers := Brokers1,
	       coordinator := Coordinator}} ->
	    Consumers = start_consumers(PTO, Brokers1),
	    Ref = set_timer(SessionTimeout),
	    {next_state, member, State#state{consumers = Consumers,
					     brokers = Brokers1,
					     liveliness_ref = Ref,
					     coordinator = Coordinator,
					     ggi = GGI,
					     consumer_id = ConsumerId1}}
    end.

rejoin(_Event, _From, State) ->    
    {reply, not_ready, State}.

illegal_generation(timeout, #state{consumers=Consumers}=State) ->
    ok = stop_consumers(Consumers),
    {next_state, rejoin, State#state{consumers = []}, 0}.

illegal_generation(_Event, _From, State) ->
    {reply, not_ready, illegal_generation, State}.

rediscover_coordinator(timeout, #state{group_id = GroupId,
				       session_timeout = SessionTimeout,
				       brokers = Brokers}=State) ->
    Brokers1 = brokers(Brokers),
    case consumer_group_coordinator(GroupId, Brokers1) of
	{ok, Coordinator} ->
	    Ref = set_timer(SessionTimeout),
	    {next_state, member, State#state{coordinator = Coordinator,
					     liveliness_ref = Ref}};
	{error, _} = Error ->
	    Error
    end.

rediscover_coordinator(_Event, _From, State) ->
    {reply, not_ready, rediscover_coordinator, State}.

member(_Event, State) ->
    {next_state, State}.

member(_Event, _From, State) ->
    {next_state, member, State}.

handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

handle_sync_event(_Event, _From, StateName, State) ->
    {next_state, StateName, State}.

handle_info(liveliness, member, #state{ coordinator = Coordinator,
					group_id = GroupId,
					consumer_id = ConsumerId,
					ggi = GGI,
					session_timeout = ST} = State) ->
    %% Member needs to heartbeat to keep the connection alive
    case send_heartbeat(Coordinator, ConsumerId, GroupId, GGI, ST) of
	{ok, TimerRef} ->
	    {next_state, member, State#state{
				   liveliness_ref = TimerRef
				  }};
	{error, 'IllegalGeneration'} ->
	    {next_state, illegal_generation, State#state{
					       liveliness_ref = undefined
					      }, 0};
	{error, 'NotCoordinatorForConsumerCode'} ->
	    {next_state, rediscover_coordinator, State#state{
						   liveliness_ref = undefined
						  }, 0};
	{error, {unable_to_connect, _Error}} ->
	    {next_state, rediscover_coordinator, State#state{
						   liveliness_ref = undefined
						  }, 0}
    end;
handle_info(_Event, StateName, State) ->
    {next_state, StateName, State}.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

terminate(_Reason, _StateName, _State) ->
    ok.

%% Consumer Group Initializer
join_group(GroupId, Brokers, JoinCommand) ->
    Brokers1 = brokers(Brokers),
    case consumer_group_coordinator(GroupId, Brokers1) of
	{ok, Coordinator} ->
	    join_consumer_group(GroupId, Coordinator, Brokers, JoinCommand);
	{error, _} = Error ->
	    Error
    end.

join_consumer_group(GroupId, Coordinator, Brokers,
		    #{topics := Topics,
		      session_timeout := SessionTimeout,
		      psa := PSA,
		      consumer_id := ConsumerId}) ->
    Request = #join_consumer_group_request{ consumer_group = GroupId
				          , session_timeout = SessionTimeout
					  , topics = Topics
					  , consumer_id = ConsumerId
					  , partition_assignment_strategy = 
						PSA},
    {ok, Pid} = brod_utils:connect(Coordinator),
    case brod_sock:send_sync(Pid, Request, 10000) of
	{ok,
	 #join_consumer_group_response{error_code = no_error,
				       group_generation_id = GGI,
				       consumer_id = ConsumerId1,
				       partitions_to_own = PTO}} ->
	    ok = brod_sock:stop(Pid),
	    {ok, #{ggi => GGI,
		   consumer_id => ConsumerId1,
		   pto => PTO,
		   brokers => Brokers,
		   coordinator => Coordinator}};
	{ok, #join_consumer_group_response{error_code = ErrorCode}} ->
	    ok = brod_sock:stop(Pid),
	    {error, ErrorCode};
	{error, _} = Error ->
	    ok = brod_sock:stop(Pid),
	    Error
    end.

stop_consumers([]) ->
    ok;
stop_consumers([{Pid, _}|Rest]) ->
    brod_consumer:stop(Pid),
    stop_consumers(Rest).

send_heartbeat(Coordinator, ConsumerId, GroupId, GGI, ST) ->
    Request = #heartbeat_request{ group_id = GroupId,
				  group_generation_id = GGI,
				  consumer_id = ConsumerId },
    {ok, Pid} = brod_utils:connect(Coordinator),
    case brod_sock:send_sync(Pid, Request, 10000) of
	{ok, #heartbeat_response{ error_code = no_error }} ->
	    {ok, set_timer(ST)};
	{ok, #heartbeat_response{ error_code = 'IllegalGeneration' }} ->
	    {error, 'IllegalGeneration'};
	{ok, Response} ->
	    {error, Response};
	{error, {error, Error}} ->
	    {error, {unable_to_connect, Error}}
    end.

set_timer(Timeout) ->
    % Fuzz the value and set a timer. @todo make sure this isn't 0.
    Timeout1 = Timeout - random:uniform(Timeout),
    erlang:send_after(Timeout1, self(), liveliness).

start_consumers(PartitionsToOwn, Brokers) ->
    start_topic_consumers(PartitionsToOwn, Brokers, []).

start_topic_consumers([], _, Acc) ->
    Acc;
start_topic_consumers([{Topic, Partitions}|PartitionsToOwn], Brokers, Acc) ->
    Acc1 = start_partition_consumers(Topic, Partitions, Brokers, Acc),
    start_topic_consumers(PartitionsToOwn, Brokers, Acc1).

start_partition_consumers(_, [], _, Acc) ->
    Acc;
start_partition_consumers(Topic, [Partition|Partitions], Brokers, Acc) ->
    {ok, Pid} = brod_consumer:start_link(Brokers, Topic, Partition, 1000),
    start_partition_consumers(Topic, Partitions, Brokers,
			      [{Pid, {Topic, Partition}}|Acc]).

brokers(Hosts) ->
    {ok, #metadata_response{brokers=Brokers}} = brod_utils:get_metadata(Hosts),
    Brokers.

-spec consumer_group_coordinator(string(), [Broker]) ->
					{ok, Coordinator} when
      Broker :: #broker_metadata{},
      Coordinator :: #broker_metadata{}.
consumer_group_coordinator(GroupName, Brokers) ->
    consumer_group_coordinator(GroupName, Brokers, ?RETRIES).
    
%% Internals
consumer_group_coordinator(_GroupName, _Brokers, 0) ->
    {error, timeout};
consumer_group_coordinator(GroupName, Brokers, Retries) ->
    Request = #consumer_metadata_request{ consumer_group = GroupName },
    {ok, Pid} = brod_utils:try_connect(Brokers),
    case brod_sock:send_sync(Pid, Request, 10000) of
	{ok, #consumer_metadata_response{coordinator = Coordinator }} ->
	    ok = brod_sock:stop(Pid),
	    {ok, Coordinator};
	{error, timeout} ->
	    ok = brod_sock:stop(Pid),
	    consumer_group_coordinator(GroupName, Brokers, Retries - 1)
    end.
