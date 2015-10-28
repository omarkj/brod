-module(brod_consumer_group).

%% This is WIP for consumer groups for Brod. It's designed agains the upcoming
%% 0.9.0 API. It is process heavy and should be refactored in the future to
%% be a single process managing a number of consumer sockets, instead of the
%% current design of a single process managing a number of consumer processes

-export([init/1,
	 consumer_group_coordinator/2,
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
			, partitions_to_own   :: [{binary(), integer()}]
			, consumers           :: [pid()]
			}).

init([#{ hosts := Hosts,
	 topics := RequestedTopics,
	 group_name := GroupName,
	 session_timeout := SessionTimeout,
	 partition_assignment_strategy := PartitionAssignmentStrategy}]) ->
    erlang:process_flag(trap_exit, true),
    #metadata_response{brokers = Brokers,
		       topics  = AvailableTopics} = cluster_metadata(Hosts),
    validate_topics(RequestedTopics, AvailableTopics),
    Opts = #{ topics => RequestedTopics
	    , session_timeout => SessionTimeout
	    , partition_assignment_strategy => PartitionAssignmentStrategy
	    , consumer_id => <<>>  },
    case join_consumer_group(GroupName, Brokers, Opts) of
	{ok, ConsumerGroup} ->
	    
	Error ->
	    Error
    end,
    {ok, #consumer_group{}}.

%% Consumer Group Initializer
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
	    {ok, #consumer_group{ group_name = GroupName
				, coordinator = Coordinator
				, group_generation_id = GGI
				, consumer_id = ConsumerId1
				, partitions_to_own = PTO }};
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
