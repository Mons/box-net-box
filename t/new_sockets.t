#!/usr/bin/env perl
use strict;
use warnings;
use Scalar::Util 'weaken';
use Async::Chain;
use Test::Tarantool;
use Test::More;

my $box = Test::Tarantool->new(
	host => '127.123.45.67',
	spaces => 'space[0] = { enabled = 1, index = [ { type = TREE, unique = 1, key_field = [ { fieldno = 0, type = STR }, ], }, ] }',
);

$box->sync_start();

my $lua = "lua
local s = box.socket('PF_INET', 'SOCK_STREAM', 'tcp')
if not s:sysconnect('127.0.0.1', 80) then
	error(\"Can't connect to server '127.0.0.1:80': \" .. s:errstr())
end
	s:write(\"HEAD / HTTP/1.1\r\nHost: mail.ru\r\nConnection: close\r\n\r\n\")
return s:readline({ \"\r\n\r\n\", \"\n\n\" })
";

$lua =~ s/\s+/ /gms;

print $box->sync_admin_cmd($lua);

$box->sync_stop();
