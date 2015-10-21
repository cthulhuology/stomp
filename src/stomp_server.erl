-module(stomp_server).
-author({ "David J Goehrig", "dave@dloh.org" }).
-copyright(<<"© 2015 David J. Goehrig"/utf8>>).
-behavior(gen_server).

%% server state
-define(SELF, list_to_atom(?MODULE_STRING ++ "_" ++ integer_to_list(Port))).
-record(stomp_server, { port, module, function, socket }).

%% public interface
-export([ start_link/3, bind/3, listen/1, accept/1 ]).

%% gen_server interface
-export([ init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3 ]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% public api

start_link( Port, Module, Function ) ->
	gen_server:start_link({ local, ?SELF }, ?MODULE, #stomp_server{ 
		port = Port, module = Module, function = Function }).

listen(Port) ->
	gen_server:cast(?SELF,listen).

accept(Port) ->
	gen_server:cast(?SELF,accept).

bind(Port,Module,Function) ->
	gen_server:cast(?SELF,{ bind, Module, Function }).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% gen_server api

init(Server = #stomp_server{ port = Port }) ->
	listen(Port),
	{ ok, Server }.

handle_call(_Message,_From,Server) ->
	{ reply, ok, Server }.

%% starts listening on the port in question
handle_cast(listen, Server = #stomp_server{ port = Port }) ->
	case gen_tcp:listen(Port,[binary,{packet,0},{reuseaddr,true},{active,true}]) of
		{ ok, Socket } ->
			accept(Port),
			{ noreply, Server#stomp_server{ socket = Socket } };
		{ error, Reason } ->
			io:format("Failed to listen on port ~p, because ~p~n", [ Port, Reason ]),
			{ stop, closed }
	end;

%% accepts a stomp connection and resumes accepting connections
handle_cast(accept,  Server = #stomp_server{ 
	port = Port, module = Module, function = Function, socket = Socket }) ->
	stomp:start_link(Socket,Module,Function),
	accept(Port),
	{ noreply, Server };

%% binds the Module:Function to all incoming stomp connections
handle_cast({ bind, Module, Function }, Server = #stomp_server{}) ->
	{ noreply, Server#stomp_server{ module = Module, function = Function } };

%% catch all for unknown casts
handle_cast(_Message,Server) ->
	{ noreply, Server}.

handle_info(_Message,Server) ->
	{ noreply, Server }.

terminate(_Reason,_Server) ->
	ok.

code_change(_Old, Server, _Extra) ->
	{ ok, Server }.
