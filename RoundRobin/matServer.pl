#!perl
# This will be the central hub for Mark As Transmitt jobs allowing us to scale
# this process as needed. This program will utilize ZeroMQ Library.
#
# It will spawn and watch workers. The workers will report which fileno they are
# working on and the results to a log. This server's only job is to manage the 
# workers and give them work orders.
#
use strict;
use warnings;
use English;

use ZMQ::LibZMQ2;
use ZMQ::Constants qw (ZMQ_PUSH ZMQ_PULL ZMQ_NOBLOCK ZMQ_RCVMORE ZMQ_POLLIN);
use IO::Select;
use POSIX;

our $worker_count = 4;
our $connected_workers = 0;
our @workQ = ();
our @errors = ();
our $recoveryFile = "/home/blhall/projects/MAT/recovery.list";
our $logdir = "/home/blhall/projects/MAT";
our %children = ();
our $debug = 0;
our $cleanExit = 0;
our $work = 1;

$SIG{'CHLD'} = "reaper";#"IGNORE"; #Let the system just reap children.
sub reaper {
  my $pid;
  $pid = waitpid(-1, &WNOHANG);

  if ($pid == -1) {
    # no child waiting.  Ignore it.
  } elsif (WIFEXITED($?)) {
    print "REAPER: Process $pid exited.\n";
    delete $children{$pid};
  } else {
    print "REAPER: False alarm on $pid.\n";
  }
  $SIG{'CHLD'} = "reaper";          # in case of unreliable signals
}

print "Starting Mark As Transmit Job Queue server.\n";

my $context = zmq_init();

# Socket to talk to send messages to workers 5557.
my $sender = zmq_socket($context, ZMQ_PUSH);
zmq_bind($sender, 'tcp://*:5557');

# Socket to listen for requests 5558.
my $workerTalkBack = zmq_socket($context, ZMQ_PULL);
zmq_bind($workerTalkBack, 'tcp://*:5558');

# Socket to listen for requests 5559 .
my $requests = zmq_socket($context, ZMQ_PULL);
zmq_bind($requests, 'tcp://*:5559');

our $alive = 1;

spawnWorkers($worker_count);
recoverQueue(); # See if there is a recovery file from improper shutdown.
main();

sub main {
  my @poll = (
    {
      socket => $workerTalkBack,
      events => ZMQ_POLLIN,
      callback => sub {
        while (1) {
          my $message = zmq_recv($workerTalkBack, ZMQ_NOBLOCK) || undef;
          my $workerMsg = zmq_msg_data($message) if $message;
          last unless $message;

          if (defined $workerMsg) {
            print "Worker message $workerMsg\n" if $debug;
            workerMessage($workerMsg);
          }
        }
      }
    },
    {
      socket => $requests,
      events => ZMQ_POLLIN,
      callback => sub {
        while (1) {
          # Request Logic
          my $message = zmq_recv($requests, ZMQ_NOBLOCK) || undef;
          my $jobMsg = zmq_msg_data($message) if $message;
          last unless $message;

          if (defined $jobMsg && $jobMsg =~ /^(\d+)/) {
            my $fileno = $1;
            push(@workQ, $fileno);
          }
          else {
            print "\nUnrecognized $jobMsg\n";
          }
        }
      }
    },
  );

  my $s = IO::Select->new();
  $s->add(\*STDIN);

printStatus();
printChildren();

while($alive) {
  if ($s->can_read(.5)) {
    chomp(my $in = <STDIN>);
    checkInput($in);
  }

  if(@workQ > 0 && workerReady() && $work) {
    zmq_send($sender, shift(@workQ));
  }
  zmq_poll(\@poll);
  }
  endServer();
}

sub workerReady {
  foreach my $id (sort keys %children) {
    return 1 if ($children{$id} eq 'Online/Ready');
  }
  return 0;
}

sub checkInput {
  my ($in) = @_;

  if($in =~ /^(q|quit|exit)/i) {
    $alive = 0;
  }
  elsif($in =~ /^(s|stat|l|list)/i) {
    printStatus();
    printChildren();
  }
  elsif($in =~ /^(spawn|a|add) worker ?(\d+)?/i) {
    my $number = $2 || 1;
    spawnWorkers($number);
  }
  elsif($in =~ /^(k|kill|die) worker ?(\d+)?/i) {
    my $number = $2 || 1;
    killWorker($number);
  }
  elsif($in =~ /^(h|help|\?)$/i) {
    printHelp();
  }
  elsif($in =~ /^mark (\d+)$/) {
    push(@workQ, $1);
    print "Added $1 to work queue\n";
  }
  elsif($in =~ /^(print|show) error/i) {
    printErrors();
  }
  elsif($in =~ /^clear error/i) {
    clearErrors();
  }
  elsif($in =~ /^hold$/i) {
    print "WARNING! Holding all jobs until told otherwise.\n";
    $work = 0;
  }
  elsif($in =~ /^unhold$/i) {
    print "Unholding jobs.\n";
    $work = 1;
  }
  else {
    print "Unknown command '$in'\n";
    printHelp();
  }
}

sub printHelp {
  print "Current Commands\n";
  print "'Quit/Exit/Q'         Cleanly Exits program.\n";
  print "'stat'                Prints current stats.\n";
  print "'Spawn/Add worker #'  Adds # workers.\n";
  print "'kill/die worker #'   Kills # workers.\n";
  print "'mark #'              Manually add file to queue.\n";
  print "'list'                Lists PID's of parent and children.\n";
  print "'print/show errors'   Lists current known problem files.\n";
  print "'clear errors'        Clears current known problem files.\n";
  print "'hold' or 'unhold'    Holds current work, will not hand out jobs\n";
  print "'help/?'              Prints this message.\n";
}

sub printStatus {
  my $q = @workQ;
  my $workers = getWorkers();
  print "Current workers: $workers; $q jobs in queue.\n";
  print "!!!!  WARNING: ALL JOBS CURRENTLY ON HOLD  !!!!!\ntype 'unhold' to unhold workers.\n" if !$work;
}

sub printChildren {
  #Print current PID's of known children
  print "Parent PID: $PID\n";
  foreach my $key (sort keys %children) {
    print "---> Worker $key is $children{$key}\n";
  }
}

sub workerMessage {
  # After they spawn we want to wait until they all check in.
  my ($msg) = @_;
  if ($msg =~ /Worker (\d+) (online|offline)/) {
    my $id = $1;
    my $status = $2;
    if ($status eq 'offline') {
      $children{$id} = 'Retired';
    }
    elsif ($status eq 'online') {
      $children{$id} = 'Online/Ready';
    }
  }
  elsif ($msg =~ /Worker (\d+) working on (\d+)/i) {
    my $pid = $1;
    my $file = $2;
    $children{$pid} = "Working on $file";
  }
  elsif ($msg =~ /Worker (\d+) Error: (\d+)/i) {
    my $pid = $1;
    my $file = $2;
    push(@errors, $file);
    # for now if a file errors just re-add it to queue
    push(@workQ, $file);
    $children{$pid} = "Online/Ready";
  }
  elsif ($msg =~ /Worker (\d+) Ready/i) {
    my $pid = $1;
    my $file = $2;
    $children{$pid} = "Online/Ready";
  }
  elsif ($msg =~ /Worker Log: (.+)/) {
    printLog($1);
  }
  else {
    print "Unknown worker message $msg\n";
  }
}

sub printLog {
  my ($msg) = @_;
  my $date = POSIX::strftime("%Y%m%d", localtime);
  my $log = "$logdir/$date.log";

  if (open LOG,">>$log") {
    print LOG getTimestamp() . " $msg\n";
    close LOG;
  }
  else {
    print "Failed to open $log for writing.\n";
  }
}

sub spawnWorkers {
  my ($workers) = @_;
  print "Spawning $workers workers...\n";

  for(my $i = 1; $i < $workers + 1; $i++) {
    my $pid = fork();
    die "Unable to fork: $!" unless defined ($pid);
    if(!$pid) { #child
      exec("perl matWorker.pl");
      die "unable to start worker: $!";
    }
    $children{$pid} = 'Started';
    print "Worker PID: $pid\n";
  }
  print "Workers Spawned.\n";
}

sub killWorker {
  my ($workers) = @_;

  print "Killing $workers workers.\n";
  for(my $i = 0; $i < $workers; $i++) {
    zmq_send($sender, 'Worker: Retire');
  }
}

sub recoverQueue {
  if (-e "$recoveryFile") {
    open FILE,"<$recoveryFile" || print "FAILED TO OPEN RECOVERY FILE $recoveryFile\n $@\n";
    my $cont = '';
    while (<FILE>) {
      $cont .= $_;
    }
    close FILE;
    my @files = split(",", $cont);
    foreach my $fileno (@files) {
      if ($fileno =~ /^\d+/) {
        push(@workQ, $fileno);
        print "Recovered fileno $fileno\n";
      }
      else {
        print "Failed to recover fileno $fileno, did not match pattern.\n";
      }
    }
    print "Successfully recovered Queue. Removing recoveryFile.\n";
    if (unlink $recoveryFile) {
      print "Successfully removed recovery file.\n";
    }
    else {
      print "Error removing recovery file.\n";
    }
  }
  else {
    print "No recovery file found. Continuing\n";
  }
  return;
}

sub saveQueue {
  open FILE,">>$recoveryFile";
  foreach my $fileno (@workQ) {
    print FILE "$fileno,";
  }
  @workQ = ();
  close FILE;
}

sub printErrors {
  print "Current known errors:\n";
  if(@errors > 0) {
    foreach my $error (@errors) {
      print "$error\n";
    }
  }
  else {
    print "No errors.\n";
  }
}

sub clearErrors {
  @errors = ();
  print "Errors cleared.\n";
}

sub workerCleanup {
  zmq_close($workerTalkBack); # Stop listening to workers.
  zmq_close($sender); # Stop talking to workers.
  sleep(5); # Give the workers a few seconds to announce they have closed.
}

sub requestCleanup {
  # Here I want to loop until I have no requests waiting and then stop listening for new requests.
  # This will minimize the amount of lost requests if a server needs to be shut down.
  my $cnt = 0;
  while(1) {
    # Request Logic
    my $message = zmq_recv($requests, ZMQ_NOBLOCK) || undef;
    my $jobMsg = zmq_msg_data($message) if $message;
    last unless $message;

    if (defined $jobMsg && $jobMsg =~ /^(\d+)/) {
      my $fileno = $1;
      push(@workQ, $fileno);
      $cnt += 1;
    }
    else {
      print "\nUnrecognized $jobMsg\n";
    }    
  }
  print "There were $cnt waiting requests. Closing connection now.\n";
  zmq_close($requests); # Stop taking requests
}

sub endServer {
  # First cleanup rquests and then stop taking any more.
  print "Getting any waiting requests.\n";
  requestCleanup();

  if (@workQ > 0) {
    print "Files still in Queue! Attempting to save them to $recoveryFile.\n";
    saveQueue();
  }
  print "Waiting for workers to finish...\n";
  my $workers = getWorkers();

  killWorker($workers); #Send a die message for each connected worker.
  print "\n";
  my $previous = 0;
  while ($workers > 0) {
    print "There are still $workers workers..\n" if $previous ne $workers;
    $previous = $workers;
    my $message = zmq_recv($workerTalkBack, ZMQ_NOBLOCK) || undef;
    my $workerMsg = zmq_msg_data($message) if $message;

    if (defined $workerMsg) {
      workerMessage($workerMsg);
    }
    $workers = getWorkers();
  }
  print "Cleaning up workers...\n";
  workerCleanup();
  $cleanExit = 1;
  print "\nServer Cleanly closed.\n";
  exit();
}

sub getWorkers {
  my $workers = keys %children;
  return $workers;
}
sub getTimestamp {
  return POSIX::strftime("%m/%d/%Y %H:%M:%S :", localtime);
}
END {
  if(!$cleanExit) {
    endServer();
  }
}
