%%%
%%%   Copyright (c) 2014, 2015, Klarna AB
%%%
%%%   Licensed under the Apache License, Version 2.0 (the "License");
%%%   you may not use this file except in compliance with the License.
%%%   You may obtain a copy of the License at
%%%
%%%       http://www.apache.org/licenses/LICENSE-2.0
%%%
%%%   Unless required by applicable law or agreed to in writing, software
%%%   distributed under the License is distributed on an "AS IS" BASIS,
%%%   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%%   See the License for the specific language governing permissions and
%%%   limitations under the License.
%%%

%%%=============================================================================
%%% @doc
%%% @copyright 2014, 2015 Klarna AB
%%% @end
%%%=============================================================================

-module(brod_utils).

%% Exports
-export([ fetch_response_to_message_set/1
        , get_metadata/1
        , get_metadata/2
        , try_connect/1
        , connect/1
        ]).

%%%_* Includes -----------------------------------------------------------------
-include("brod_int.hrl").

%%%_* Code ---------------------------------------------------------------------
%% try to connect to any of bootstrapped nodes and fetch metadata
get_metadata(Hosts) ->
  get_metadata(Hosts, []).

get_metadata(Hosts, Topics) ->
  {ok, Pid} = try_connect(Hosts),
  Request = #metadata_request{topics = Topics},
  Response = drob_socket:send_sync(Request, Pid),
  drob_socket:close(Pid),
  Response.

try_connect(Hosts) ->
  try_connect(Hosts, []).

try_connect([], LastError) ->
  LastError;
try_connect([Host | Hosts], _) ->
  %% Do not 'start_link' to avoid unexpected 'EXIT' message.
  %% Should be ok since we're using a single blocking request which
  %% monitors the process anyway.
  case connect(Host) of
    {ok, Pid} -> {ok, Pid};
    Error     -> try_connect(Hosts, Error)
  end.

fetch_response_to_message_set(#fetch_response{topics = [TopicFetchData]}) ->
  #topic_fetch_data{topic = Topic, partitions = [PM]} = TopicFetchData,
  #partition_messages{ partition = Partition
                     , high_wm_offset = HighWmOffset
                     , messages = Messages} = PM,
  #message_set{ topic = Topic
              , partition = Partition
              , high_wm_offset = HighWmOffset
              , messages = Messages}.

-spec try_connect({string(), integer()} | #broker_metadata{}) ->
		     {ok, pid()} | {error, term()}.
connect({Host, Port}) ->
  drob_socket:connect(Host, Port, ?DEFAULT_CLIENT_ID);
connect(#broker_metadata{host = Host, port = Port}) ->
  connect({Host, Port}).

%%% Local Variables:
%%% erlang-indent-level: 2
%%% End:
