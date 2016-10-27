package ASO::GliteAsync;

use warnings;
use base 'ASO::Glite', 'PHEDEX::Core::Logging';
use POE;
use Data::Dumper;

our %params =
	(
	  SERVICE	=> undef,	# Transfer service URL
	  SPACETOKEN	=> undef,       
	  CHANNEL	=> undef,	# Channel name to match
	  USERDN	=> undef,	# Restrict to specific user DN
	  VONAME	=> undef,	# Restrict to specific VO
	  SSITE		=> undef,	# Specify source site name
	  DSITE		=> undef,	# Specify destination site name
	  ME		=> 'FTS3CLI',	# Arbitrary name for this object
	  PRIORITY	=> 3,		# Default piority configured in FTS channels
	  OPTIONS	=> {},		# Per-command specific options
	  WRAPPER	=> $ENV{PHEDEX_GLITE_WRAPPER} || '', # Command-wrapper
	  DEBUG		=> 0,
	  VERBOSE	=> 0,
	  POCO_DEBUG	=> $ENV{POCO_DEBUG} || 0, # Specially for PoCo::Child
          FTS_USE_JSON  => 1,           # Wheter use json format or not
	);

our %states =
	(
	  Submitted		=> 11,
	  Ready			=> 10,
	  Active		=>  9,
	  Finished		=>  0,
	  FinishedDirty		=>  0,
	  Pending		=> 10,
	  Default		=> 99,
	);

our %weights =
	(
	  Ready	  =>  1 + 0 * 300,
	  Active  =>  1 + 0 * 300,
	  Waiting =>  1 + 0 * 900,
	);

sub new
{
  my $proto = shift;
  my $class = ref($proto) || $proto;
  my $self  = ref($proto) ? $class->SUPER::new(@_) : {};

  my %args = (@_);
  map { 
        $self->{$_} = defined($args{$_}) ? $args{$_} : $params{$_}
      } keys %params;
  $self->{DEBUGGING} = $PHEDEX::Debug::Paranoid || 0;

  bless $self, $class;
  $self->Dbgmsg('Transfer::Backend::Interface::FTS3CLIAsync::new creating instance') if $self->{DEBUG};
  return $self;
}

sub AUTOLOAD
{
  my $self = shift;
  my $attr = our $AUTOLOAD;
  $attr =~ s/.*:://;
  $self->Dbgmsg('Transfer::Backend::Interface::FTS3CLIAsync::AUTOLOAD calling $attr') if $self->{DEBUG};
  if ( exists($params{$attr}) )
  {
    $self->{$attr} = shift if @_;
    return $self->{$attr};
  }
  return unless $attr =~ /[^A-Z]/;  # skip DESTROY and all-cap methods
  my $parent = "SUPER::" . $attr;
  $self->$parent(@_);
}

=head2 ParseListQueue

returns a hashref with information about all the jobs currently in the transfer
queue for the given FTS service . The hash is keyed on the job-ids, the value 
being a subhash of ID, STATE, and (FTS) SERVICE names.

In the event of an error the hash contains a single key, 'ERROR', with the
value being the text of the error message. Not very sophisticated but good
enough for now. Clients need only detect that the 'ERROR' key is present to
know something went wrong, or assume all is well if it isn''t there.

This function is not used by the backend in normal operation, but is useful in
passive monitoring mode.

=cut

sub ParseListQueue
{
  my ($self,$output) = @_;
  my $result = {};

  my $last_id = 0;
  for ( split /\n/, $output )
  {
    push @{$result->{RAW_OUTPUT}}, $_;
    if ( m%^\s*Request ID:\s+([0-9,a-f,-]+)$% ) { 
       print "ParseListQueue: something wrong, last_id is not zero ", Dumper($output) if ( $last_id );
       $last_id = $1; 
       next; 
    }
    if ( m%^\s*Status:\s+(\S+)$% ) { 
       $result->{JOBS}{$last_id} = {ID => $last_id, STATE => $1, SERVICE => $self->{SERVICE}};
       $last_id = 0;
    }
  }
  print 'Transfer::Backend::Interface::FTS3CLIAsync::ParseListQueue ', Dumper($result) if $self->{DEBUG} >=2;
  return $result;
}

sub ParseSubmit
{
  my ($self, $job, $output) = @_;
  my $result = {};

  foreach ( split /\n/, $output )
  {
    push @{$result->{RAW_OUTPUT}}, $_;
    m%^\s*([0-9,a-f,-]+)$% or next;
    $job->ID( $1 );
  }
  if ( !defined($job->ID) )
  {
    my $dump = Data::Dumper->Dump( [\$job, \$result], [ qw / job result / ] );
    $dump =~ s%\n%%g;
    $dump =~ s%\s\s+% %g;
    $dump =~ s%\$% %g;
    push @{$result->{ERROR}}, 'JOBID=undefined, cannot monitor this job: ' . $dump;
  }
  print 'Transfer::Backend::Interface::FTS3CLIAsync::ParseSubmit ', Dumper($result) if $self->{DEBUG} >=2;
  return $result;
}

sub Command
{
  my ($self,$str,$arg) = @_;
  my ($cmd,$opts);
  $cmd = '';
  $cmd = "$self->{WRAPPER} " if $self->{WRAPPER};
  $opts = '';
  $opts = " $self->{OPTIONS}{$str}" if $self->{OPTIONS}{$str};

  if ( $str eq 'ListQueue' )
  {
    $cmd .= 'fts-transfer-list -o cms';
    $cmd .= ' -s ' . $arg->Service; 
    $cmd .= $opts;
    return $cmd;
  }

  if ( $str eq 'ListJob' )
  {
    $cmd .= 'fts-transfer-status -l';
    $cmd .= ' --verbose ' if $arg->VERBOSE;
    $cmd .= ' -s ' . $arg->Service . ' ' . $arg->ID;
    $cmd .= $opts;
    return $cmd;
  }

  if ( $str eq 'SetPriority' )
  {
    my $priority = $arg->Priority;
    return undef unless $priority;
    return undef unless $arg->Service;
#   Save an interaction with the server ?
    return undef if $priority == $self->{PRIORITY};

    $cmd .= 'fts-set-priority';
    $cmd .= ' -s ' . $arg->Service; 
    $cmd .= ' ' . $arg->ID . ' ' . $priority;
    $cmd .= $opts;
    return $cmd;
  }

  if ( $str eq 'Submit' )
  {
     my $spacetoken = $arg->{SPACETOKEN} || $self->{SPACETOKEN};
     my $use_json   = $arg->{FTS_USE_JSON} || $self->{FTS_USE_JSON};
     $cmd .= 'fts-transfer-submit';
     $cmd .= ' -s ' . $arg->Service;
     $cmd .= (defined $spacetoken)  ? ' -t ' . $spacetoken : '';
     $cmd .= ($use_json) ? ' --json-submission -f '. $arg->JsonCopyjob : ' -f ' . $arg->Copyjob; 
     $cmd .= $opts;
     return $cmd;
  }

  return undef;
}

=head2 ParseListJob

Takes two arguments, a reference to a PHEDEX::Transfer::Backend::Job
object, and the output of a fts-transfer-status command.  
Returns a somewhat complex hashref with the result. As with ListQueue,
the hash will contain an 'ERROR' key if something went wrong, or not if the
command succeeded.

The keys returned in the hash are:

=over

RAW_OUTPUT is the unmolested text output of the status command, returned as a
reference to an array of lines. This is needed for debugging purposes.

The FILES key contains a subkey for each file in the job, keyed by destination
PFN. The function explicitly checks that it has keys for the DESTINATION,
DURATION, REASON, RETRIES, SOURCE and STATE of each file, or it throws an
error, assuming that something serious has gone wrong. This may not be correct
behaviour. So, the status of a particular destination PFN will be returned as
C<< $result->{FILES}{$destination}{PFN}{STATE} >>.

The FILES_STATE key contains a hash of { STATE => state-count }, i.e. the
number of times a given file-state is encountered. Only states which are
actually encountered are present in the hash, so the existence of a given key
is not guaranteed.

The ETC key can be ignored for now. It should be set to zero. Eventually this
will be used as a means of estimating the time of completion of a given job,
which will affect its priority for monitoring.

=back

=cut

sub ParseListJob
{
  my ($self,$job,$output) = @_;
  my ($cmd,$state,$dst);
  my ($key,$value);
  my (@hofh,$h,$preamble);

  my $result = {};
  $result->{JOB_STATE} = 'undefined';
  return $result unless $output;

  $preamble=1;
  my $last_key;
  my @raw = split /\n/, $output;
  @{$result->{RAW_OUTPUT}} = @raw;

  foreach ( @raw ) {
     if ( $preamble ) {
       if ( m%^\s*([A-Z,a-z]+)\s*$% ) { # non-verbose case
         $state = $1;
         $preamble = 0;
       }
       if ( m%^\s*Status:\s+([A-Z,a-z]+)\s*$% ) { # verbose case
         $state = $1;
       }
       if ( m%^\s+Source:\s+(.*)\s*$% ) {
         $last_key = uc "Source";
         $h->{$last_key} = $1;
         $preamble = 0;
       }
       push @{$result->{INFO}}, $_ if $preamble;
     } else {
       if ( m%^\s+Source:\s+(.*)\s*$% ) {
#        A 'Source' line is the first in a group for a single src->dst transfer
         push @hofh, $h if $h;
         undef $h;
       }
       if ( m%^\s+(\S+):\s+(.*)\s*$% ) {
         $last_key = uc $1;
         $h->{$last_key} = $2;
       }
       elsif ( m%\S% ) {
         $h->{$last_key} .= ' ' . $_;
       }
     }
  }

  if ( defined($state) )
  {
    chomp $state;
    $result->{JOB_STATE} = $state;
  }

  push @hofh, $h if $h;
  foreach $h ( @hofh ) {
#  Be paranoid about the fields I read!
    foreach ( qw / DESTINATION DURATION REASON RETRIES SOURCE STATE / ) {
      next if defined($h->{$_});
      my $error_msg = "No \"$_\" key! : " .
		join(', ',
			map { "$_=$h->{$_} " } sort keys %{$h}
		    );
      push @{$result->{ERROR}}, $error_msg;
    }
    return $result if $result->{ERROR};
    $result->{FILES}{$h->{DESTINATION}} = $h;
  }

  my $time = time;
  foreach ( keys %{$result->{FILES}} )
  {
    $result->{FILE_STATES}{ $result->{FILES}{$_}{STATE} }++;
    $result->{FILES}{$_}{TIMESTAMP} = $time;
  }

  $result->{ETC} = 0;
  print 'Transfer::Backend::Interface::FTS3CLIAsync::ParseListJob ', Dumper($result) if $self->{DEBUG} >=2;
  return $result;
}

=head2 StatePriority

Takes a job state-name as argument, and returns an integer from a lookup
hash. Another half-formed idea, the intention was to use this in the
calculation of monitoring priorities too. This is not needed at the moment,
but this routine is still needed in the passive polling mode, which is used
in the standalone transfer prototype. Essentially the C<< %states >> hash
should have all known states listed in it as keys, with zero for the states
that correspond to job-exit, and any non-zero value for states which are not
terminal.

=cut

sub StatePriority
{
  my ($self,$state) = @_;
  return $states{$state} if defined($states{$state});
  return $states{Default} if !$self->{DEBUGGING};
  die "Unknown state \"$state\" encountered in ",__PACKAGE__,"\n";
}

1;

