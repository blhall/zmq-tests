#!perl
# This will test the server by sending random file numbers in different counts to see 
# how the server handles them.
use strict;
use warnings;

use ZMQ::LibZMQ2;
use ZMQ::Constants qw (ZMQ_PUSH);
use POSIX;

my $workerID = shift;
my $context = zmq_init();

# Socket to talk back to server.
my $sender = zmq_socket($context, ZMQ_PUSH);
zmq_connect($sender, 'tcp://localhost:5559');

sub within {
  my ($upper) = @_;

  return int(rand($upper)) + 1;
}
print "Sending 100 file numbers to queue\n";
for (1 .. 100) {
  my $fileNo = within(100000);
  zmq_send($sender, $fileNo);
}
print "Sent.\n";
sleep(10);
exit();
