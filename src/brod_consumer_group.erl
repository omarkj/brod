-module(brod_consumer_group).
-behaviour(gen_server).

%% This is WIP for consumer groups for Brod. It's designed agains the upcoming
%% 0.9.0 API. It is process heavy and should be refactored in the future to
%% be a single process managing a number of consumer sockets, instead of the
%% current design of a single process managing a number of consumer processes

% gen_server callbacks
-export([init/1,
	 handle_cast/2,
	 handle_call/3,
	 handle_info/2,
	 terminate/2,
	 code_change/3]).

-export([consumer_group_coordinator/2,
	 join_consumer_group/3]).

%%%_* Includes -----------------------------------------------------------------
-include("brod.hrl").
-include("brod_int.hrl").

%%%_* Macros -------------------------------------------------------------------
-define(RETRIES, 5).

%%%_* Records ------------------------------------------------------------------
-record(consumer_group, { group_name          :: string()
			, coordinator         :: #broker_metadata{}
			, group_generation_id :: integer()
			, consumer_id         :: string()
			, consumers          
			  :: [{pid(), {binary(), integer()}}]
			}).

init([#{ hosts := Hosts,
	 topics := RequestedTopics,
	 group_name := GroupName,
	 session_timeout := SessionTimeout,
	 partition_assignment_strategy := PartitionAssignmentStrategy}]) ->
    erlang:process_flag(trap_exit, true),
    #metadata_response{brokers = Brokers,
		       topics  = AvailableTopics} = cluster_metadata(Hosts),
    io:format("AvailableTopics ~p", [AvailableTopics]),
    validate_topics(RequestedTopics, [T || #topic_metadata{name=T}
					       <- AvailableTopics]),
    Opts = #{ topics => RequestedTopics
	    , session_timeout => SessionTimeout
	    , partition_assignment_strategy => PartitionAssignmentStrategy
	    , consumer_id => <<>>  },
    case join_consumer_group(GroupName, Brokers, Opts) of
	{ok, PartitionsToOwn, ConsumerGroup} ->
	    % Loop through given partitions this consumer group is responsible
	    % for and spawn a brod_consumer for each one.
	    Consumers = start_consumers(PartitionsToOwn, Brokers),
	    {ok, ConsumerGroup#consumer_group{consumers = Consumers}};
	Error ->
	    {stop, Error}
    end.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_call(_Msg, _From, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

%% Consumer Group Initializer
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

cluster_metadata(Hosts) ->
    {ok, Metadata} = brod_utils:get_metadata(Hosts),
    Metadata.

join_consumer_group(GroupName, Brokers, Opts) ->
    case consumer_group_coordinator(GroupName, Brokers) of
	{ok, Coordinator} ->
	    join_group(GroupName, Coordinator, Opts);
	{error, _} = Error ->
	    Error
    end.

join_group(GroupName, Coordinator, #{ topics := Topics
				    , session_timeout := SessionTimeout
				    , partition_assignment_strategy := PSA
				    , consumer_id := ConsumerId }) ->
    case join_group(GroupName, Topics, Coordinator, SessionTimeout,
		    PSA, ConsumerId) of
	{ok, #join_consumer_group_response{ group_generation_id = GGI 
					  , consumer_id = ConsumerId1
					  , partitions_to_own = PTO }} ->
	    {ok, PTO, #consumer_group{ group_name = GroupName
				     , coordinator = Coordinator
				     , group_generation_id = GGI
				     , consumer_id = ConsumerId1
				     }};
	{error, _} = Error ->
	    Error
    end.

-spec consumer_group_coordinator(string(), [Broker]) ->
					{ok, Coordinator} when
      Broker :: #broker_metadata{},
      Coordinator :: #broker_metadata{}.
consumer_group_coordinator(GroupName, Brokers) ->
    consumer_group_coordinator(GroupName, Brokers, ?RETRIES).

join_group(GroupName, Topics, GroupCoordinator, SessionTimeout,
	   PartitionAssignmentStrategy, ConsumerId) ->
    join_group(GroupName, Topics, GroupCoordinator, SessionTimeout,
	       PartitionAssignmentStrategy, ConsumerId, ?RETRIES).
    
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

join_group(_, _, _, _, _, _, 0) ->
    {error, timeout};
join_group(GroupName, Topics, GroupCoordinator, SessionTimeout,
	   PartitionAssignmentStrategy, ConsumerId, Retries) ->
    Request = #join_consumer_group_request{ consumer_group = GroupName
				          , session_timeout = SessionTimeout
					  , topics = Topics
					  , consumer_id = ConsumerId
					  , partition_assignment_strategy = 
						PartitionAssignmentStrategy
					  },
    {ok, Pid} = brod_utils:connect(GroupCoordinator),
    case brod_sock:send_sync(Pid, Request, 10000) of
	{ok, #join_consumer_group_response{} = Response} ->
	    ok = brod_sock:stop(Pid),
	    {ok, Response};
	{error, timeout} ->
	    ok = brod_sock:stop(Pid),
	    join_group(GroupName, Topics, GroupCoordinator, SessionTimeout,
		       PartitionAssignmentStrategy, ConsumerId, Retries - 1)
    end.

validate_topics([], _) -> true;
validate_topics([RequestedTopic | RequestedTopics], AvailableTopics) ->
    true = lists:member(RequestedTopic, AvailableTopics),
    validate_topics(RequestedTopics, AvailableTopics).
