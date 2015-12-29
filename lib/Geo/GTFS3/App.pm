package Geo::GTFS3::App;
use warnings;
use strict;

use Geo::GTFS3;

sub new {
    my ($class) = @_;
    my $self = bless({}, $class);
    $self->init() if $self->can("init");
    return $self;
}

sub cmd__help { print(<<"END"); }
commands:
  gtfs3 help
  gtfs3 list-agencies
  gtfs3 load FEEDURL
  gtfs3 list-routes AGENCYNAME [DATE]
END

sub cmd__load {
    my ($self, $url) = @_;
    $self->gtfs3->load_from_url($url);
}

sub cmd__list_agencies {
    my ($self) = @_;
    $self->gtfs3->output_list_of_agencies();
}

sub cmd__list_routes {
    my ($self, $agency_name, $date) = @_;
    $self->gtfs3->output_list_of_routes($agency_name, $date);
}

sub cmd__list_trips {
    my ($self, $agency_name, $date) = @_;
    $self->gtfs3->output_list_of_trips($agency_name, $date);
}

sub gtfs3 {
    my ($self) = @_;
    return $self->{gtfs3} if $self->{gtfs3};
    $self->{gtfs3} = Geo::GTFS3->new();
    return $self->{gtfs3};
}

sub run {
    my ($self, $command, @arguments) = @_;
    if (!defined $command) {
	die("gtfs3: No command specified.\n");
    }
    my $method = $self->method($command);
    if (!$method) {
	die("gtfs3: $command: command not found.\n");
    }
    $self->$method(@arguments);
}

sub method {
    my ($self, $command) = @_;
    my $name = $command;
    $name =~ s{-}{_}g;
    $name = "cmd__" . $name;
    return $self->can($name);
}

1;

