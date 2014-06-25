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

my @lua;
push @lua, "lua
box.fiber.wrap (
	function ()
		box.fiber.name('mortal')
		local s = {}
		s.s = box.socket('PF_INET', 'SOCK_STREAM', 'tcp')
		s.flag = 1
		if not s.s:sysconnect('127.0.0.1', 80) then
			error(\"Can't connect to server '127.0.0.1:80': \" .. s.s:errstr())
		end

		box.fiber.wrap (
			function ()
				box.fiber.name('immortal')
				print 'I am immortal!'
				s.s:write(\"HEAD / HTTP/1.1\\r\\nHost: mail.ru\\r\\nConnection: close\\r\\n\\r\\n\")
				i = 0
				while s.flag ~= nil do
					i = i + 1
					res = s.s:read(12, 1)
					if res:len() > 0 then
						print(res:len(), ':', res)
					elseif res ~= nil then
						print('EOF')
						break
					else
						if s.s:errno() ~= box.errno.EINPROGRESS then
							print(s.s:errno()..' '..box.errno.strerror(s.s:errno()))
							break
						else
							print('Alive!')
						end
					end
					if i > 20 then break end
				end
				print 'Nooooooooo!'
			end,
			s.s
		)
		box.fiber.sleep(3)
		print 'Close socket'
		print(s.s:shutdown(2))
		print(s.s:close())
		s.flag = nil
		print('I am gone')
	end
)
box.fiber.sleep(10);
";

push @lua, "lua
box.fiber.wrap(
	function()
		box.fiber.name('parent')
		local s = {}
		s.s = box.socket('PF_INET', 'SOCK_STREAM', 'tcp')
		s.flag = 1
		if not s.s:sysconnect('127.0.0.1', 80) then
			error(\"Can't connect to server '127.0.0.1:80': \" .. s.s:errstr())
		end
		child = box.fiber.wrap (
			function ()
				box.fiber.name('child')
				print 'I am here!'
				i = 0
				while s.flag ~= nil do
					res = s.s:read(12, 3)
					box.fiber.testcancel()
					print 'Still Alive!'
				end
			end,
			s.s
		)
		box.fiber.sleep(1)
		print('Killing child')
		box.fiber.cancel(child)
		print('Done...')
	end
)
box.fiber.sleep(10);
";

(my $lua = $lua[1]) =~ s/\s+/ /gms;

{
	my ($status, $message) = $box->sync_admin_cmd($lua);
	die $message unless $status;
}

$box->sync_stop();
