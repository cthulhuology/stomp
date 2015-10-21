-module(stomp).
-author({ "David J Goehrig", "dave@dloh.org" }).
-copyright(<<"© 2015 David J. Goehrig"/utf8>>).
-behavior(gen_server).

%% stomp socket interface
-export([ start_link/2, start_link/3, wait_headers/2, socket/1, protocol/1, headers/1, uuid/1, bind/3, parse/1 ]).

%% stomp protocol interface
-export([ stomp/4, connect/4, connected/3, send/4, subscribe/4, unsubscribe/3, 
	ack/3, nack/3, transaction/3, commit/3, abort/3, disconnect/2, message/3, receipt/3, error/3 ]).

%% gen_server interface
-export([ init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3 ]).

%% a record for managing stomp connection state
-record(stomp, { uuid, protocol, socket, headers, data, module, function, connecting, disconnecting }).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Public Methods

start_link(Module,Function) ->
	gen_server:start_link(?MODULE, { client, Module, Function }, []).

start_link(Listen,Module,Function) ->
	gen_server:start_link(?MODULE, { server, Listen, Module, Function }, []).

wait_headers(Stomp,Data) ->
	gen_server:cast(Stomp,{ wait_headers, Data }).

socket(Stomp) ->
	gen_server:call(Stomp,socket).

protocol(Stomp) ->
	gen_server:call(Stomp,protocol).

headers(Stomp) ->
	gen_server:call(Stomp,headers).

uuid(Stomp) ->
	gen_server:call( Stomp, uuid ).

bind(Stomp,Module,Function) ->
	gen_server:cast( Stomp, { bind, Module, Function }).

connect(Stomp,Host,Version,Headers) ->
	gen_server:cast( Stomp, { connect, Host, Version, Headers }).

stomp(Stomp,Host,Version,Headers) ->
	gen_server:cast( Stomp, { stomp, Host, Version, Headers }).

connected(Stomp,Version, Headers) ->
	gen_server:cast( Stomp, { connected, Version, Headers }).

send(Stomp,Destination,Headers,Data) ->
	gen_server:cast( Stomp, { send, Destination, Headers, Data }).

subscribe(Stomp,Id,Destination,Headers) ->
	gen_server:cast( Stomp, { subscribe, Id, Destination, Headers }).

unsubscribe(Stomp,Id,Headers) ->
	gen_server:cast( Stomp, { unsubscribe, Id, Headers }).

ack(Stomp,Id,Headers) ->
	gen_server:cast( Stomp, { ack, Id, Headers }).

nack(Stomp,Id,Headers) ->
	gen_server:cast( Stomp, { nack, Id, Headers }).

%% transaction handles the begin case
transaction(Stomp,Transaction,Headers) ->
	gen_server:cast( Stomp, { transaction, Transaction, Headers }).

commit(Stomp,Transaction,Headers) ->
	gen_server:cast( Stomp, { commit, Transaction, Headers }).

abort(Stomp,Transaction,Headers) ->
	gen_server:cast( Stomp, { abort, Transaction, Headers }).

disconnect(Stomp,Headers) ->
	gen_server:cast( Stomp, { disconnect, Headers }).

message(Stomp,Headers,Data) ->
	gen_server:cast(Stomp, { message, Headers, Data }).

receipt(Stomp,Id,Headers) ->
	gen_server:cast(Stomp, { receipt, Id, Headers }).

error(Stomp,Message,Headers) ->
	gen_server:cast(Stomp, { error, Message, Headers }).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% gen_server behavior

%% creates a bare Stomp connector, which we will call stomp or connect 
%% to connect to the server, for now we have no socket
init({ client, Module, Function }) ->
	{ ok, #stomp{ 
		uuid = uuid:new(),
		headers = [],
		data = <<>>,
		module = Module,
		function = Function,
		connecting = true,
		disconnecting = false
	}};

%% accept the socket connection and then start processing the connection headers
%% once we're connected we'll handle messages as they come
init({ server, Listen, Module, Function }) ->
	case gen_tcp:accept(Listen) of
		{ ok, Socket } ->
			wait_headers(self(), <<>>),
			{ ok, #stomp{
				uuid = uuid:new(),
				socket = Socket,
				headers = [],
				data = <<>>,
				module = Module,
				function = Function,
				connecting = true,
				disconnecting = false
			}};
		{ error, closed } ->
			{ stop, closed }
	end.
				
%% we use calls to return information about the Stomp connection to the program.
%% the public API calls that are arity 1 are all calls

handle_call(socket, _From, Stomp = #stomp{ socket = Socket }) ->
	{ reply, Socket, Stomp };

handle_call(protocol, _From, Stomp = #stomp{ protocol = Protocol }) ->
	{ reply, Protocol, Stomp };

handle_call(headers, _From, Stomp = #stomp{ headers = Headers }) ->
	{ reply, Headers, Stomp };

handle_call(uuid, _From, Stomp = #stomp{ uuid = UUID }) ->
	{ reply, UUID, Stomp };

handle_call(Message,_From,Stomp = #stomp{}) ->
	io:format("Unhandled call ~p~n", [ Message ]),
	{ reply, ok, Stomp }.

%% we use casts to mutate the state of the connection, this includes sending
%% messages or rebinding the behavior associated with the connection

handle_cast({ connect, Host, Version, Headers}, Stomp = #stomp{}) ->
	[ Hostname, Port ] = binary:split(Host,<<":">>),
	case gen_tcp:connect(binary:bin_to_list(Hostname),list_to_integer(binary:bin_to_list(Port)),[]) of
		{ ok, Socket }  ->
			Head = join_headers([ 
				{ <<"accept-version">>, Version },
				{ <<"host">>, Hostname }
				| Headers ], <<>>),
			gen_tcp:send(Socket,<<"CONNECT\n", Head/binary, "\n", 0>>),
			{ noreply, Stomp#stomp{ socket = Socket } };
		{ error, Reason } ->
			io:format("Failed to connect to ~p because ~p~n", [ Host, Reason ]),
			{ stop, closed }
	end;

handle_cast({ stomp, Host, Version, Headers}, Stomp = #stomp{}) ->
	[ Hostname, Port ] = binary:split(Host,<<":">>),
	case gen_tcp:connect(binary:bin_to_list(Hostname),list_to_integer(binary:bin_to_list(Port)),[]) of
		{ ok, Socket }  ->
			Head = join_headers([ 
				{ <<"accept-version">>, Version },
				{ <<"host">>, Hostname }
				| Headers ], <<>>),
			gen_tcp:send(Socket,<<"STOMP\n", Head/binary, "\n", 0>>),
			{ noreply, Stomp#stomp{ socket = Socket } };
		{ error, Reason } ->
			io:format("Failed to connect to ~p because ~p~n", [ Host, Reason ]),
			{ stop, closed }
	end;

handle_cast({ connected, Version, Headers }, Stomp = #stomp{ socket = Socket }) ->
	Head = join_headers([ { <<"version">>, Version } | Headers ], <<>>),
	gen_tcp:send(Socket, <<"CONNECTED\n", Head/binary >>, "\n", 0 ),
	{ noreply, Stomp };

handle_cast({ send, Destination, Headers, Data }, Stomp = #stomp{ socket = Socket }) ->
	Head = join_headers([ { <<"destination">>, Destination } | Headers], <<>>),
	gen_tcp:send(Socket,<<"SEND\n", Head/binary, "\n", Data, 0>>),
	{ noreply, Stomp };

handle_cast({ subscribe, Id, Destination, Headers }, Stomp = #stomp{ socket = Socket }) ->
	Head = join_headers([ { <<"id">>, Id }, { <<"destination">>, Destination } | Headers ], <<>>),
	gen_tcp:send(Socket,<<"SUBSCRIBE\n", Head/binary, "\n", 0>>),
	{ noreply, Stomp };

handle_cast({ unsubscribe, Id, Headers }, Stomp = #stomp{ socket = Socket }) ->
	Head = join_headers([ { <<"id">>, Id } | Headers ], <<>>),
	gen_tcp:send(Socket,<<"UNSUBSCRIBE\n", Head/binary, "\n", 0>>),
	{ noreply, Stomp };

handle_cast({ ack, Id, Headers }, Stomp = #stomp{ socket = Socket }) ->
	Head = join_headers([ { <<"id">>, Id } | Headers ], <<>>),
	gen_tcp:send(Socket,<<"ACK\n",Head/binary, "\n", 0>>),
	{ noreply, Stomp };

handle_cast({ nack, Id, Headers }, Stomp = #stomp{ socket = Socket }) ->
	Head = join_headers([ { <<"id">>, Id } | Headers ], <<>>),
	gen_tcp:send(Socket,<<"NACK\n",Head/binary, "\n", 0>>),
	{ noreply, Stomp };

handle_cast({ transaction, Transaction, Headers },  Stomp = #stomp{ socket = Socket }) ->
	Head = join_headers([ { <<"transaction">>, Transaction } | Headers ],<<>>),
	gen_tcp:send(Socket,<<"BEGIN\n", Head/binary, "\n", 0>>),
	{ noreply, Stomp };

handle_cast({ commit, Transaction, Headers },  Stomp = #stomp{ socket = Socket }) ->
	Head = join_headers([ { <<"transaction">>, Transaction } | Headers ],<<>>),
	gen_tcp:send(Socket,<<"COMMIT\n", Head/binary, "\n", 0>>),
	{ noreply, Stomp };

handle_cast({ abort, Transaction, Headers },  Stomp = #stomp{ socket = Socket }) ->
	Head = join_headers([ { <<"transaction">>, Transaction } | Headers ],<<>>),
	gen_tcp:send(Socket,<<"ABORT\n", Head/binary, "\n", 0>>),
	{ noreply, Stomp };

handle_cast({ disconnect, Headers },  Stomp = #stomp{ socket = Socket }) ->
	Head = join_headers(Headers,<<>>),
	gen_tcp:send(Socket,<<"DISCONNECT\n", Head/binary, "\n", 0>>),
	{ noreply, Stomp#stomp{ disconnecting = true }};	

handle_cast({ message, Headers, Data }, Stomp = #stomp{ socket = Socket }) ->
	Head = join_headers(Headers, <<>>),
	gen_tcp:send(Socket,<<"MESSAGE\n", Head/binary, "\n", Data, 0>>),
	{ noreply, Stomp };

handle_cast({ receipt, Id, Headers }, Stomp = #stomp{ socket = Socket }) ->
	Head = join_headers([ { <<"receipt-id">>, Id } | Headers ], <<>> ),
	gen_tcp:send(Socket,<<"RECEIPT\n", Head/binary, "\n", 0>>),
	{ noreply, Stomp };

handle_cast({ error, Message, Headers }, Stomp = #stomp{ socket = Socket }) ->
	Head = join_headers([ { <<"message">>, Message } | Headers ], <<>> ),
	gen_tcp:send(Socket,<<"ERROR\n", Head/binary, "\n", 0>>),
	{ stop, Stomp };

handle_cast({ wait_headers, Data }, Stomp = #stomp{ socket = Socket }) ->
	io:format("waiting for data on ~p~n", [ Socket ]),
	{ noreply, Stomp#stomp{ data = Data } };	

handle_cast({ bind, Module, Function }, Stomp = #stomp{}) ->
	{ noreply, Stomp#stomp{ module = Module, function = Function }};

handle_cast(Message,Stomp = #stomp{}) ->
	io:format("Unhandled cast ~p~n", [ Message ]),
	{ noreply, Stomp }.

handle_info(Message,Stomp = #stomp{}) ->
	io:format("Unhandled info ~p~n", [ Message ]),
	{ noreply, Stomp }.

terminate(normal, #stomp{ socket = Socket }) ->
	gen_tcp:close(Socket),
	ok;

terminate(Reason, #stomp{ socket = Socket }) ->
	io:format("Terminating because ~p~n", [ Reason ]),
	gen_tcp:close(Socket),
	ok.

code_change( _Old, Stomp, _Extra ) ->
	{ ok, Stomp }.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% private functions

join_headers([],Data) ->
	Data;

join_headers([ { K, V } | Headers], Data) ->
	join_headers(Headers, << K/binary, ":", V/binary, "\n", Data/binary>>).			
	
%% returns [ { K, V } | ... ]
parse_headers(Lines) ->
	Trimmed = [ binary:replace(X,<<"\r">>, <<>>) || X <- Lines ],
	[ { K, V } || [ K, V ] <- [ binary:split(Y,<<":">>) || Y <- Trimmed ]].	

%% returns a message, and any remaining data after 
parse(Data) ->
	[ Message, Remainder ] = binary:split(Data,<<0>>),
	io:format(" message [~p] remainder [~p]~n", [ Message, Remainder]),
	[ Head, Body ] =  binary:split(Message, <<"\n\n">>),
	io:format(" Head [~p] Body [~p]~n", [ Head, Body ]),	
	[ Command | Lines ] = binary:split(Head, <<"\n">>, [ global ]),
	Headers = parse_headers(Lines),
	[ { Command, Headers, Body }, Remainder ]. 
	
