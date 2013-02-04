#!perl
# This is a worker for Mark as Transmit job. It will receive either fileno
# or DONE signal which tells it to terminate itself. Otherwise it will
# wait for jobs.
#
use strict;
use warnings;
use English;

use ZMQ::LibZMQ2;
use ZMQ::Constants qw (ZMQ_PUSH ZMQ_PULL);
use POSIX;

our $workerID = $PID;
our $context = zmq_init();
our $log = "/home/blhall/projects/MAT/$workerID.log";
our $alive = 1;

# Socket to talk back to server.
my $sender = zmq_socket($context, ZMQ_PUSH);
zmq_connect($sender, 'tcp://localhost:5558');

# Socket to talk to receive work from server.
my $receiver = zmq_socket($context, ZMQ_PULL);
zmq_connect($receiver, 'tcp://localhost:5557');

#Send message letting server know we are waiting for a job
zmq_send($sender, "Worker $workerID online");

printLog("Online.");
main();
print "Exited main loop\n";

sub main {
  while($alive) {
    my $msg = zmq_msg_data(zmq_recv($receiver));
    if ($msg =~ /^(\d+)/) {
      processFileNo($1);
    }
    else {
      processCommand($msg);
    }
  }
  done();
}

sub done {
  zmq_close($receiver); # Stop accepting new jobs.
  zmq_close($sender); # Stop talking to server.
  print "Worker: $workerID : Closed.\n";
  exit(0);
}

sub printLog {
  my ($msg) = @_;
#  if (open LOG,">>$log") {
#    print LOG getTimestamp() . " Worker $workerID: $msg\n";
#    close LOG;
#  print "$msg\n";
#  }
}

sub processCommand {
  my ($msg) = @_;

  if($msg =~ /Worker: (\w+)/i) {
    my $cmd = $1;
    if($cmd =~ /(retire|die)/i) {
      # Notify server we are going down.
      zmq_send($sender, "Worker $workerID offline");
      # Give time for msg to deliver
      sleep(2);
      $alive = 0;
    }
    else {
      printLog("Received Unknown command: $msg");
    }
  }
  else {
    printLog("Received Unknown command: $msg");
  }
}

sub processFileNo {
  my ($fileNo) = @_;
  printLog("Marking file $fileNo as transmitted.");
  zmq_send($sender, "Worker $workerID working on $fileNo");
  eval {
    #Run mark job here
    my $rand = rand(45) + 1; 
    sleep($rand); #Pretend to do work as usual.
  };
  if ($@) {
    printLog("Had error marking file $fileNo $@");
    zmq_send($sender, "Worker $workerID Error: $fileNo");
  }
  else {
    printLog("File $fileNo marked.");
    zmq_send($sender, "Worker $workerID Ready");
  }
}

sub getTimestamp {
  return POSIX::strftime("%m/%d/%Y %H:%M:%S :", localtime);
}

sub getdate {
  return POSIX::strftime("%Y%m%d", localtime);
}

END {
  exit(0);
}
