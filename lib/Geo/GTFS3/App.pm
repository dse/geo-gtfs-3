package Geo::GTFS3::App;
use warnings;
use strict;

use Geo::GTFS3;
use Geo::GTFS3::Realtime;

sub new {
    my ($class) = @_;
    my $self = bless({}, $class);
    $self->init() if $self->can("init");
    return $self;
}

sub options {
    my ($self) = @_;
    return (
	"verbose|v" => sub {
	    $self->gtfs3->{verbose} += 1;
	    $self->realtime->{verbose} += 1;
	}
       );
}

COMMAND("help", sub { print(<<"END"); });
commands:
  gtfs3 help
  gtfs3 list-agencies
  gtfs3 load FEEDURL
  gtfs3 list-routes AGENCYNAME [DATE]
  gtfs3 list-instances
  gtfs3 delete-instance INSTANCEID [...]
  gtfs3 realtime-json URL
END

COMMAND("load", sub {
	    my ($self, $url) = @_;
	    $self->gtfs3->load_from_url($url);
	});

COMMAND("reload", sub {
	    my ($self, $url) = @_;
	    $self->gtfs3->reload_from_url($url);
	});

sub cmd__list_agencies {
    my ($self) = @_;
    $self->gtfs3->output_list_of_agencies();
}

sub cmd__list_routes {
    my ($self, $agency_name, $date) = @_;
    die("Agency name must be specified.\n") unless defined $agency_name;
    $self->gtfs3->output_list_of_routes($agency_name, $date);
}

sub cmd__list_trips {
    my ($self, $agency_name, $date) = @_;
    die("Agency name must be specified.\n") unless defined $agency_name;
    $self->gtfs3->output_list_of_trips($agency_name, $date);
}

sub cmd__list_instances {
    my ($self) = @_;
    $self->gtfs3->output_list_of_instances();
}

sub cmd__delete_instance {
    goto &cmd__delete_instances;
}
sub cmd__delete_instances {
    my ($self, @instance_id) = @_;
    foreach my $instance_id (@instance_id) {
	$self->gtfs3->delete_instance($instance_id);
    }
}

sub cmd__realtime_json {
    my ($self, $url) = @_;
    $self->realtime->load_from_url($url);
    print($self->realtime->{data_json});
}

sub cmd__list_trip_stops {
    my ($self, $agency_name, $trip_id, $time_t) = @_;
    $self->gtfs3->output_list_of_trip_stops($agency_name, $trip_id, $time_t);
}

sub cmd__check_realtime {
    my ($self, $url, $agency_name) = @_;
    $self->realtime->load_from_url($url);
    $self->realtime->check_trip_updates_against($self->gtfs3, $agency_name);
}

#------------------------------------------------------------------------------

sub gtfs3 {
    my ($self) = @_;
    return $self->{gtfs3} if $self->{gtfs3};
    $self->{gtfs3} = Geo::GTFS3->new();
    return $self->{gtfs3};
}

sub realtime {
    my ($self) = @_;
    return $self->{realtime} if $self->{realtime};
    $self->{realtime} = Geo::GTFS3::Realtime->new();
    return $self->{realtime};
}

#------------------------------------------------------------------------------

use Getopt::Long;

sub run {
    my ($self, @args) = @_;

    {
	local *ARGV = \@args;
	my $p = Getopt::Long::Parser->new();
	$p->configure("bundling", "gnu_compat");
	$p->getoptions($self->options);
    }

    my ($command, @arguments) = @args;
    if (!defined $command) {
	die("gtfs3: No command specified.\n");
    }
    my $method = $self->METHOD($command);
    if (!$method) {
	die("gtfs3: $command: command not found.\n");
    }
    $self->$method(@arguments);
}

sub COMMAND {
    my ($command, $sub) = @_;
    my $name = __PACKAGE__->METHOD_NAME($command);
    no strict "refs";
    *$name = $sub;
}

sub METHOD {
    my ($self, $command) = @_;
    my $name = __PACKAGE__->METHOD_NAME($command);
    return $self->can($name);
}

sub METHOD_NAME {
    my ($self, $command) = @_;
    my $name = $command;
    $name =~ s{-}{_}g;
    $name = "cmd__" . $name;
    return $name;
}

1;

