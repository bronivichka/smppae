package Log;

use strict;
#use warnings;
use POSIX qw(strftime);
use Data::Dumper;

sub new {
    my ($proto, $conf) = @_;

    my $class = ref($proto) || $proto;
    my $self = $conf;
    
    $self->{log_prefix} ||= (caller)[0];

    bless ($self, $class);

    $self->open_log() or return;

    return $self;
} # new


sub log {
    my $self = shift;
    my $level = shift;
    return if $level eq 'DEBUG' and !$self->{debug};

    my $channel = $level eq 'STDOUT' ? \*STDOUT : \*STDERR;

    print $channel join(' ', strftime('%Y.%m.%d %H:%M:%S', localtime), $level, $self->{log_prefix}, @_), "\n";

} # log

sub rotate_log {
    my ($self) = @_;

    $self->close_log();
    $self->open_log();

} # rotate_log

sub open_log {
    my ($self) = @_;

    return 1 if $self->{nologfile};

    $self->{log} or do {
        $self->log('ERROR', "No log configuration parameter set");
        return;
    };

    unless ($self->{nofilestdout}) {
        open (STDOUT, '>>', $self->{log}) or do {
            $self->log('ERROR', "Unable to open $self->{log}: $!");
            return;
        };
    }
    unless ($self->{nofilestderr}) {
        open (STDERR, '>>', $self->{log}) or do {
            $self->log('ERROR', "Unable to open $self->{log}: $!");
            return;
        };
    }

    select STDERR;
    $| = 1;
    select STDOUT;

    $self->log('NOTICE', "Opened $self->{log}");

    return 1;
} # open_log

sub close_log {
    my ($self) = @_;

    close STDERR;
    close STDOUT;

} # close_log

1;
