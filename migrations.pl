#!/usr/bin/perl -w

use strict;
use DBI;
use Getopt::Long;
use XML::Simple;
use Data::Dumper;

sub main
{
    intro();
    my %options = options();
    my %conf = parseConfig($options{config});
    
    my $dburl = $options{dburl} || $conf{dburl};
    my $dbuser = $options{dbuser} || $conf{dbuser};
    my $dbpass = $options{dbpass} || $conf{dbpass};

    my $db = Database->new($dburl, $dbuser, $dbpass);
    if ($options{action} eq "apply")
    {
        apply($db, $conf{migrations});
    }
    elsif ($options{action} eq "list")
    {
        list($db, $conf{migrations});
    }
    elsif ($options{action} eq "revert")
    {
    }
    else
    {
    }
}

sub list
{
    my $db = shift;
    my $migrations = shift;
    foreach my $mig(@{ $migrations })
    {
        my $migName = $mig->{name};
        my $migSource = $mig->{source};
        my $applied = $db->isMigrationApplied($migName);
        if ($applied)
        {
            print "Migration $migName applied at $applied\n";
        }
        else
        {
            print "Migration $migName is not applied\n";
        }
    }
}

sub apply
{
    my $db = shift;
    my $migrations = shift;
    foreach my $mig(@{ $migrations })
    {
        my $migName = $mig->{name};
        my $migSource = $mig->{source};
        my $applied = $db->isMigrationApplied($migName);
        if ($applied)
        {
            print "Migration $migName applied at $applied\n";
        }
        else
        {
            applyMigration($db, $migSource, "up");
            print "Applying Migration $migName\n";
            $db->markApplied($migName);
        }
    }
}

sub applyMigration
{
    my $db = shift;
    my $source = shift;
    my $dir = shift;

    my $xs = XML::Simple->new(ForceArray => 1, ForceContent => 1);
    my $ref = $xs->XMLin($source);
    my $code = $ref->{$dir}->[0]->{content};
    $db->execute($code);
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
    $config{migrations} = [];
    foreach my $mig(keys(%{ $ref->{list}->[0]->{migration} }))
    {
        my $migration = ();
        $migration->{name} = $mig;
        $migration->{source} = $ref->{list}->[0]->{migration}->{$mig}->{content};
        push(@{ $config{migrations} }, $migration);
    }
    return %config;
}
sub options
{
    my %h = ();
    $h{config} = "migrations.xml";
    $h{dburl} = "";
    $h{dbuser} = "";
    $h{dbpass} = "";
    $h{action} = "apply";
    Getopt::Long::Configure ("bundling");
    GetOptions(\%h, 'help','dburl=s','dbuser=s','dbpass=s','config=s','action=s');
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

    $self->{ISAPPLIED} = $self->{DBH}->prepare_cached('SELECT Applied FROM MigrationsApplied WHERE Migration = ?');
    $self->{APPLY} = $self->{DBH}->prepare_cached('INSERT INTO MigrationsApplied(Migration, Applied) VALUES(?, CURRENT_TIMESTAMP)');
    bless($self, $class);
    return $self;
}

sub isMigrationApplied
{
    my $self = shift;
    my $name = shift;
    $self->{ISAPPLIED}->execute($name);
    my $resultset = $self->{ISAPPLIED}->fetchrow_arrayref;
    my $applied = undef;
    if ($resultset)
    {
        $applied = $resultset->[0];
    }
    $self->{ISAPPLIED}->finish;
    return $applied;
}

sub markApplied
{
    my $self = shift;
    my $name = shift;
    unless ($self->isMigrationApplied($name))
    {
        $self->{APPLY}->execute($name);
    }
}

sub execute
{
    my $self = shift;
    my $code = shift;
    my $sth = $self->{DBH}->prepare($code);
    $sth->execute;
    $sth->finish;
}


sub details
{
    my $self = shift;
    my $url = $self->{URL};
    my $user = $self->{USER};
    my $pass = $self->{PASS};
    
    return "$url ($user/$pass)";
}

