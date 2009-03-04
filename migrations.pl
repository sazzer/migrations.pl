#!/usr/bin/perl -w

use strict;
use DBI;
use Getopt::Long;
use XML::Simple;

sub main
{
    intro();
    my %options = options();
    my %conf = parseConfig($options{config});

    my $dburl = $options{dburl} || $conf{dburl};
    my $dbuser = $options{dbuser} || $conf{dbuser};
    my $dbpass = $options{dbpass} || $conf{dbpass};

    my $db = Database->new($dburl, $dbuser, $dbpass);
    print $db->details() . "\n";
}

sub parseConfig
{
    my $config = shift;
    my $xs = XML::Simple->new(ForceArray => 1, ForceContent => 1);
    my $ref = $xs->XMLin($config);
    
    my %config = ();
    $config{dburl} = $ref->{database}->[0]->{url}->[0]->{content};
    $config{dbuser} = $ref->{database}->[0]->{username}->[0]->{content};
    $config{dbpass} = $ref->{database}->[0]->{password}->[0]->{content};
    return %config;
}
sub options
{
    my %h = ();
    $h{config} = "migrations.xml";
    $h{dburl} = "";
    $h{dbuser} = "";
    $h{dbpass} = "";
    Getopt::Long::Configure ("bundling");
    GetOptions(\%h, 'help','dburl=s','dbuser=s','dbpass=s','config=s');
    return %h;
}

sub intro
{
    print "Database Migrations v0.1\n";
}

sub usage
{
}

main();

package Database;
sub new 
{
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my ($url, $user, $pass) = @_;

    unless (defined $url)
    {
        die "The Database URL must be defined!\n";
    }
    my $self = {};
    $self->{URL} = $url;
    $self->{USER} = $user || "";
    $self->{PASS} = $pass || "";

    $self->{DBH} = DBI->connect($self->{URL}, $self->{USER}, $self->{PASS})
        or die "Couldn't connect to database: " . DBI->errstr;

    if (scalar($self->{DBH}->tables('%', '%', 'MigrationsApplied', 'TABLE')) == 0)
    {
        my $sth = $self->{DBH}->prepare('CREATE TABLE MigrationsApplied (Migration VARCHAR(255), Applied TIMESTAMP)');
        $sth->execute();
        $sth->finish;
    }
    bless($self, $class);
    return $self;
}

sub details
{
    my $self = shift;
    my $url = $self->{URL};
    my $user = $self->{USER};
    my $pass = $self->{PASS};
    
    return "$url ($user/$pass)";
}

