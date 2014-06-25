#!/usr/bin/env perl
use strict;
use warnings;
use Scalar::Util 'weaken';
use Data::Dumper;
use AnyEvent::Loop;
use AnyEvent::Tarantool;
use AnyEvent::Socket;
use AnyEvent::Handle;
use Test::More;
use Async::Chain;
use Test::Tarantool;

my $w = AnyEvent->signal (signal => "INT", cb => sub { exit 0 });

my @box;
push @box, Test::Tarantool->new(
	host => '127.123.45.67',
	spaces => 'space[0] = { enabled = 1, index = [ { type = TREE, unique = 1, key_field = [ { fieldno = 0, type = STR }, ], }, ] }',
	wal_mode => 'fsync',
	initlua => "
		if package ~= nil then
			package.path = package.path..';$ENV{PWD}/lib/?.lua'
			package.cpath = package.path..';$ENV{PWD}/lib/?.so'
			require 'box.net.box'
		end
	",
) for 1..2;

{
	print "Starting\n";
	{
		require 'syscall.ph';
		require 'sys/resource.ph';
		my $rstruct = pack "L!L!", 64, 1024;
		syscall(&SYS_setrlimit, &RLIMIT_NOFILE, $rstruct);
		my ($status, $reason) = $box[0]->sync_start();
		die $reason unless $status;
	}
	{
		require 'syscall.ph';
		require 'sys/resource.ph';
		my $rstruct = pack "L!L!", 64, 1024;
		syscall(&SYS_setrlimit, &RLIMIT_NOFILE, $rstruct);
		my ($status, $reason) = $box[1]->sync_start();
		die $reason unless $status;
	}
	print "Started\n";
}


my $tnt1;
my $tnt2;
my $tcon;

for (@box) {
	print "$_->{pid}\n";
}

chain
	sub {
		my $next = shift;
		$tnt1 = AnyEvent::Tarantool->new(
			host => $box[0]->{host},
			port => $box[0]->{port},
			connfail => sub {
				shift if ref $_[0];
				warn "Tarantool connect to $tnt1->{server} failed: @_\n";
			},
			disconnected => sub {
				shift if ref $_[0];
				warn "Tarantool connection to $tnt1->{server} reset: @_\n";
			},
			connected => sub {
				shift if ref $_[0];
				warn "Tarantool connected @_\n";
				$next->();
		});
		$tnt1->connect();
	},
	sub {
		my $next = shift;
		$tnt2 = AnyEvent::Tarantool->new(
			host => $box[1]->{host},
			port => $box[1]->{port},
			connfail => sub {
				shift if ref $_[0];
				warn "Tarantool connect to $tnt2->{server} failed: @_\n";
			},
			disconnected => sub {
				shift if ref $_[0];
				warn "Tarantool connection to $tnt2->{server} reset: @_\n";
			},
			connected => sub {
				shift if ref $_[0];
				warn "Tarantool connected @_\n";
				$next->();
		});
		$tnt2->connect();
	},
	sub {
		my $next = shift;
		open my $rand, "<", "/dev/urandom";
		my ($buf, $key, $value);
		my @errors;
		srand(1);
		for my $i (1..100) {
			warn "$i\n" unless ($i % 100);
			{
				read($rand, $buf, 1024);
				my $cv = AE::cv();
				$key = join('', map { chr(ord('A') + int(rand(26))) } (1..10));
				$value = unpack('H*', $buf);
				$tnt1->lua('box.insert', [
					0,
					$key,
					$value,
				], $cv);
				$cv->recv;
			}
			{
				my $cv = AE::cv();
				$tnt2->lua('box.dostring', [
					"
					local conn = box.net.box('$box[0]->{host}', $box[0]->{port})
					local res = conn:select(0, 0, '$key')
					conn:close()
					return res
					" ], $cv);
				my ($ret, $err) = $cv->recv;
				if ($ret) {
					unless ($ret->{tuples}->[0]->[0] eq $key and $ret->{tuples}->[0]->[1] eq $value) {
						push @errors, "Unexpected data on step $i";
					}
				} else {
					push @errors, $err;
				}
			}
		}
		warn join "\n", @errors if @errors;
		$next->();
	},
	sub {
		my $next = shift;
		my ($status, $reason) = $box[1]->sync_admin_cmd("show fiber");
		die $reason unless $status;
		print "Fiber count" . scalar grep { /fid: / } split "\n", $reason;
		map { /name: / and print "$_\n"; } split "\n", $reason;
		$next->();
	},
	sub {
		my $next = shift;
		done_testing();
		exit 0;
	}
;

AnyEvent::Loop::run();
__END__
space[0] = { enabled = 1, index = [ { type = HASH, unique = 1, key_field = [ { fieldno = 0, type = STR }, ], }, ] }
space[0] = {
    enabled = 1,
    index = [
        {
            type = HASH,
            unique = 1,
            key_field = [
                { fieldno = 0, type = STR },
            ],
        },
    ]
}
