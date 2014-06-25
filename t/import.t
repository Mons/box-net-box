#!/usr/bin/env perl
use strict;
use warnings;
use Scalar::Util 'weaken';
use Async::Chain;
use Test::Tarantool;
use Test::More;

my $initlua = "
if package ~= nil then
	package.path = package.path..';$ENV{PWD}/lib/?.lua'
	package.cpath = package.path..';$ENV{PWD}/lib/?.so'
	require 'box.net.box'
end
";

my $box = Test::Tarantool->new(
	host => '127.123.45.67',
	spaces => 'space[0] = { enabled = 1, index = [ { type = TREE, unique = 1, key_field = [ { fieldno = 0, type = STR }, ], }, ] }',
	wal_mode => 'fsync',
	initlua => $initlua,
);

$box->sync_start();

print $box->sync_admin_cmd("lua bnb = box.net.box('$box->{host}', $box->{p_port}) print(bnb:call('box.time'))");

$box->sync_stop();
