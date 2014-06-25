#!/usr/bin/env perl
use strict;
use warnings;
use Scalar::Util 'weaken';
use Async::Chain;
use AnyEvent;
use Test::Tarantool;
use Test::More;

my $w = AnyEvent->signal (signal => "INT", cb => sub { exit 0 });

my $box = Test::Tarantool->new(
	host => '127.123.45.67',
	spaces => 'space[0] = { enabled = 1, index = [ { type = TREE, unique = 1, key_field = [ { fieldno = 0, type = STR }, ], }, ] }',
);

$box->sync_start();

my $lua = "lua
fiber = box.fiber.create(
	function ()
		box.fiber.name('mortal')
		box.fiber.detach()
		local s = box.socket('PF_INET', 'SOCK_STREAM', 'tcp')
		if not s:sysconnect('127.0.0.1', 12345) then
			error(\"Can't connect to server '127.0.0.1:80': \" .. s:errstr())
		end

		local fiber = box.fiber.create(
			function (s)
				box.fiber.name('immortal')
				box.fiber.detach()
				print 'I am immortal!'
				s:read(12)
				print 'Nooooooooo!'
			end
		)
		box.fiber.resume(fiber, s)
		box.fiber.sleep(1)
		print 'Close socket'
		print(s:shutdown(2))
		print(s:close())
		s = undef
		print('I am gone')
	end
)

box.fiber.resume(fiber)
fiber = nil
box.fiber.sleep(10);
";

$lua =~ s/\s+/ /gms;

{
	my ($status, $message) = $box->sync_admin_cmd($lua);
	die $message unless $status;
}

$box->sync_stop();
