#!/usr/bin/perl

use strict;
use warnings;
use Data::Dumper;

use FindBin;
use lib "$ARGV[0]/mod", "$FindBin::Bin/../mod";
use Conf;
use SMPP;

usage() unless @ARGV >= 2;
shift(@ARGV);

#use utf8;
#use open qw(:std :utf8);
#use open ':encoding(utf8)';

my $id = $ARGV[0];
my $conf = $Conf::Conf{SMPP};
$conf->{db} = $Conf::Conf{DB};
$conf->{rabbitmq_exchange} = $Conf::Conf{RABBITMQ}->{default_exchange};

$0 = "$Conf::Conf{NAME_PREFIX}_smpp_$id";

my $smpp = SMPP->new($id, $conf) or exit;

$smpp->start_node();
$smpp->process_node();

sub usage {
    print STDERR "\nUsage: $0 </path/to/project> <smpp_config_id>\n\n";
    exit 1;
} # usage

