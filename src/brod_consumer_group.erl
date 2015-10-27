-module(brod_consumer_group).

%% This is WIP for consumer groups for Brod. It's designed agains the upcoming
%% 0.9.0 API. It is process heavy and should be refactored in the future to
%% be a single process managing a number of consumer sockets, instead of the
%% current design of a single process managing a number of consumer processes

-export([init/1,
	 consumer_group_coordinator/2]).

%%%_* Includes -----------------------------------------------------------------
-include("brod.hrl").
-include("brod_int.hrl").

%%%_* Records ------------------------------------------------------------------
-record(consumer_group, { group_name          :: string()
			, coordinator         :: {string(), integer()}
			, group_generation_id :: integer()
			, consumer_id         :: string()
			, consumers           :: [pid()]
			}).

init([#{hosts := Hosts, topics := RequestedTopics,
	group_name := GroupName, session_timeout := SessionTimeout,
	partition_assignment_strategy := PartitionAssignmentStrategy}]) ->
    erlang:process_flag(trap_exit, true),
    #metadata_response{brokers = Brokers,
		       topics  = AvailableTopics} = cluster_metadata(Hosts),
    validate_topics(RequestedTopics, AvailableTopics),
    GroupCoordinator = consumer_group_coordinator(GroupName, Brokers),
    _GroupInformation = join_group(GroupName, RequestedTopics, GroupCoordinator,
				  SessionTimeout, PartitionAssignmentStrategy),
    {ok, #consumer_group{}}.

%% Consumer Group Initializer
cluster_metadata(Hosts) ->
    {ok, Metadata} = brod_utils:get_metadata(Hosts),
    Metadata.

-spec consumer_group_coordinator(string(), [Broker]) ->
					{ok, Coordinator} when
      Broker :: #broker_metadata{},
      Coordinator :: #broker_metadata{}.
consumer_group_coordinator(GroupName, Brokers) ->
    {ok, Pid} = brod_utils:try_connect(Brokers),
    Request = #consumer_metadata_request{ consumer_group = GroupName },
    #consumer_metadata_response{coordinator = Coordinator } = 
	brod_sock:send_sync(Pid, Request, 10000),
    ok = brod_sock:stop(Pid),
    {ok, Coordinator}.
    
validate_topics([], _) -> true;
validate_topics([RequestedTopic | RequestedTopics], AvailableTopics) ->
    true = lists:member(RequestedTopic, AvailableTopics),
    validate_topics(RequestedTopics, AvailableTopics).
