#!/usr/bin/env perl
use strict;
use warnings;
use Async::Chain;
use Test::Tarantool;
use Test::More;
use FindBin qw($Bin);

my $initlua = "
if package ~= nil then
	package.path = package.path..';$Bin/../lib/?.lua'
	package.cpath = package.path..';$Bin/../lib/?.so'
	require 'box.net.box'
end
";

my $box = Test::Tarantool->new(
	host => '127.123.45.67',
	spaces => 'space[0] = { enabled = 1, index = [ { type = TREE, unique = 1, key_field = [ { fieldno = 0, type = STR }, ], }, ] }',
	wal_mode => 'fsync',
	logger => sub { },
	initlua => $initlua,
);

$box->sync_start();

my ($status, $message) = $box->sync_admin_cmd("lua bnb = box.net.box('$box->{host}', $box->{p_port}) print(bnb:call('box.time'))");

ok $status, "Load box.net.box"
	or diag $message;

$box->sync_stop();
