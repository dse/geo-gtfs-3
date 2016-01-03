package Geo::GTFS3::Realtime;
use warnings;
use strict;

use HTTP::Cache::Transparent;
use LWP::UserAgent;
use Google::ProtocolBuffers;
use JSON qw(-convert_blessed_universally);
use POSIX qw(strftime);
use File::Basename qw(dirname);
use File::Path qw(make_path);
use Data::Dumper;
use YAML;
use Text::Table;

sub new {
    my ($class) = @_;
    my $self = bless({}, $class);
    $self->init();
    return $self;
}

sub init {
    my ($self) = @_;
    my @pwent = getpwuid($>);
    my $username = $pwent[0];
    my $home = $ENV{HOME} // $pwent[7];
    my $dir = $self->{dir} = "$home/.geo-gtfs3";
    $self->{http_cache_dir} = "$dir/http-cache";
    $self->{verbose} = 0;
    $self->{proto_url} = "https://developers.google.com/transit/gtfs-realtime/gtfs-realtime.proto";
}

sub load_protocol {
    my ($self) = @_;
    return if $self->{proto_loaded};
    HTTP::Cache::Transparent::init({ BasePath => $self->{http_cache_dir},
				     Verbose => $self->{verbose},
				     NoUpdate => 86400,
				     UseCacheOnTimeout => 1,
				     NoUpdateImpatient => 0 });
    warn("Getting protocol from $self->{proto_url} ...\n");
    my $request = HTTP::Request->new("GET", $self->{proto_url});
    my $response = $self->ua->request($request);
    if (!$response->is_success()) {
	warn("Failed to pull protocol:\n");
	warn("  ", $self->{proto_url}, "\n");
	warn("  => ", $response->base, "\n") if $self->{proto_url} ne $response->base;
	warn("  => ", $response->status_line, "\n");
	exit(1);
    }
    if ($self->{verbose}) {
	warn(sprintf("  => %s\n", $response->base)) if $self->{proto_url} ne $response->base;
	warn(sprintf("  => %s\n", $response->status_line));
    }
    my $proto = $response->content();
    if (!defined $proto) {
	die("Failed to pull protocol: undefined content\n");
    }
    if (!$proto) {
	die("Failed to pull protocol: no content\n");
    }
    warn("Parsing protocol ...\n");
    Google::ProtocolBuffers->parse($proto);
    warn("Done.\n");
    $self->{proto_loaded} = 1;
}

sub ua {
    my ($self) = @_;
    return $self->{ua} if $self->{ua};
    $self->{ua} = LWP::UserAgent->new();
    return $self->{ua};
}

sub load_from_url {
    my ($self, $url) = @_;
    $self->load_protocol();
    HTTP::Cache::Transparent::init({ BasePath => $self->{http_cache_dir},
				     Verbose => $self->{verbose},
				     NoUpdate => 30,
				     NoUpdateImpatient => 1 });
    my $request = HTTP::Request->new("GET", $url);
    my $response = $self->ua->request($request);
    $self->{response} = $response;
    if (!$response->is_success) {
	warn(sprintf("%s =>\n", $url)) if $url ne $response->base;
	warn(sprintf("%s => %s\n", $response->base, $response->status_line));
	return;
    }
    if ($self->{verbose}) {
	warn("GET $url\n");
	warn(sprintf("  %s\n", $response->base)) if $response->base ne $url;
	warn(sprintf("  %s\n", $response->status_line));
    }
    return $self->load_from_http_response($response);
}

sub save_pb {
    my ($self, $response, $o) = @_;
    $response //= $self->{response};
    $o //= $self->{data};

    my $time = $o->{header}->{timestamp} // $response->last_modified // time();
    my $base_filename = strftime("%Y/%m/%d/%H%M%SZ", gmtime($time));
    my $pb_filename = sprintf("%s/realtime-data/%s/%s/%s.pb", $self->{dir}, $self->{agency_name}, $self->{feed_type_name}, $base_filename);
    make_path(dirname($pb_filename));
    my $fh;
    if (!open($fh, ">", $pb_filename)) {
	die("Cannot write $pb_filename: $!\n");
    }
    binmode($fh);
    print {$fh} $response->content;
}

sub save_json {
    my ($self, $response, $o) = @_;
    $response //= $self->{response};
    $o //= $self->{data};

    my $time = $o->{header}->{timestamp} // $response->last_modified // time();
    my $base_filename = strftime("%Y/%m/%d/%H%M%SZ", gmtime($time));
    my $json_filename = sprintf("%s/realtime-data/%s/%s/%s.json", $self->{dir}, $self->{agency_name}, $self->{feed_type_name}, $base_filename);
    make_path(dirname($json_filename));
    my $fh;
    if (!open($fh, ">", $json_filename)) {
	die("Cannot write $json_filename: $!\n");
    }
    binmode($fh);
    print {$fh} $self->json->encode($o);
}

sub save_dumper {
    my ($self, $response, $o) = @_;
    my $time = $o->{header}->{timestamp} // $response->last_modified // time();
    my $base_filename = strftime("%Y/%m/%d/%H%M%SZ", gmtime($time));
    my $dumper_filename = sprintf("%s/realtime-data/%s/%s/%s.dumper", $self->{dir}, $self->{agency_name}, $self->{feed_type_name}, $base_filename);
    make_path(dirname($dumper_filename));
    my $fh;
    if (!open($fh, ">", $dumper_filename)) {
	die("Cannot write $dumper_filename: $!\n");
    }
    binmode($fh);
    print {$fh} Dumper($o);
}

sub save_yaml {
    my ($self, $response, $o) = @_;
    my $time = $o->{header}->{timestamp} // $response->last_modified // time();
    my $base_filename = strftime("%Y/%m/%d/%H%M%SZ", gmtime($time));
    my $yaml_filename = sprintf("%s/realtime-data/%s/%s/%s.yaml", $self->{dir}, $self->{agency_name}, $self->{feed_type_name}, $base_filename);
    make_path(dirname($yaml_filename));
    my $fh;
    if (!open($fh, ">", $yaml_filename)) {
	die("Cannot write $yaml_filename: $!\n");
    }
    binmode($fh);
    print {$fh} Dump($o);
}

sub load_from_http_response {
    my ($self, $response) = @_;
    $response //= $self->{response};

    my $url = $response->base;
    my $cref = $response->content_ref;
    my $o = TransitRealtime::FeedMessage->decode($$cref);

    $self->{data_pb}     = $$cref;
    $self->{data_object} = $o;
    $self->{data_json}   = $self->json->encode($o);

    if ($self->{save_pb}) {
	$self->save_pb($response, $o);
    }
    if ($self->{save_json}) {
	$self->save_json($response, $o);
    }
    if ($self->{save_dumper}) {
	$self->save_dumper($response, $o);
    }
    if ($self->{save_yaml}) {
	$self->save_yaml($response, $o);
    }

    return ($o, $response) if wantarray;
    return $o;
}

=head2 check_trip_updates_against

    $rt->check_trip_updates_against($gtfs, $agency_name);

where:

    $gtfs         is a Geo::GTFS3 object
    $agency_name  is an agency name listed in the Geo::GTFS3 database

Checks the most recently retrieved GTFS-realtime feed against the schedule.

=cut

sub check_trip_updates_against {
    my ($self, $gtfs, $agency_name) = @_;

    # The most recently retrieved GTFS-realtime feed.
    my $o = $self->{data_object};

    # The timestamp on the most recently retrieved GTFS-realtime feed.
    my $time_t = $o->{header}->{timestamp};

    # Get the timestamp in "XX:XX:XX" format.
    my @localtime = localtime($time_t);
    my ($hh, $mm, $ss) = @localtime[2, 1, 0];
    my $hhmmss    = sprintf("%02d:%02d:%02d", $hh, $mm, $ss);
    my $hhmmss_xm = sprintf("%02d:%02d:%02d", $hh + 24, $mm, $ss); # in cases like "00:xx:xx" we also want "24:xx:xx".

    # Trips that are scheduled to be running at that time.
    my @scheduled_trips = $gtfs->get_list_of_current_trips($agency_name, $time_t);

    # An index to scheduled trips by trip id.
    my %scheduled_trips = map { ( $_->{trip_id} => $_ ) } @scheduled_trips;

    # Trip Update records are the ones we want from the GTFS-realtime
    # feed.
    my @trip_update = map { $_->{trip_update} } grep { $_->{trip_update} } @{$o->{entity}};

    # An index to trip update records by trip id.
    my %trip_update_trips = map { ( $_->{trip}->{trip_id} => $_) } grep { defined eval { $_->{trip}->{trip_id} } } @trip_update;

    # Vehicle Position records contain timestamps.
    my @vehicle_position = map { $_->{vehicle} } grep { $_->{vehicle} } @{$o->{entity}};

    # An index to vehicle position records by fleet number (the
    # vehicle's "label").
    my %vehicle_position = map { ( $_->{vehicle}->{label} => $_ ) } grep { defined eval { $_->{vehicle}->{label} } } @vehicle_position;

    # A list of trip ids that point to a trip update OR a scheduled
    # trip OR both.
    my %trip_ids = map { ( $_ => 1 ) } (keys(%trip_update_trips), keys(%scheduled_trips));
    my @trip_ids = sort keys %trip_ids;

    my $tb = Text::Table->new(
	"\nTrip ID",
	"\nBlock",
	"\nRoute",
	"\nTrip Headsign",
	"Sched.\nDepart",
	"Sched.\nArrive",
	"\nVehicle",
	"\nRoute",
	"\nDelay",
	"\nFlags"
       );

    # A list of trip IDs for scheduled trips that are accounted for by
    # a trip update record.
    my @accounted_for_trip_ids = grep { $scheduled_trips{$_} and $trip_update_trips{$_} } @trip_ids;

    # A list of trip IDs for scheduled trips that are NOT accounted
    # for by a trip update record.
    my @unaccounted_for_trip_ids = grep { $scheduled_trips{$_} and !$trip_update_trips{$_} } @trip_ids;

    # If there's no trip update record to account for a scheduled
    # trip, we check to see if there's a trip update record for a
    # previous trip on that scheduled trip's block number.  We assume
    # this indicates a vehicle is running late and still operating
    # that previous trip.  Another possibility is that a vehicle is
    # taking recovery time before its next trip.
    my %look_for_block_id;
    foreach my $trip_id (@unaccounted_for_trip_ids) {
	my $trip = $scheduled_trips{$trip_id};
	my $block_id = defined $trip ? $trip->{block_id} : undef;
	$look_for_block_id{$block_id} = 1 if defined $block_id;
    }

    my %found_block_id;

    # A list of trip IDs for trip update records that are NOT on a
    # scheduled trip.
    my @leftover_trip_update_trip_ids = grep { !$scheduled_trips{$_} and $trip_update_trips{$_} } @trip_ids;

    # We'll divide those trip ID's into three lists.  See below.
    my @leftover_trip_ids_A;
    my @leftover_trip_ids_B;
    my @leftover_trip_ids_C;

    # Pull the GTFS trip records for those trip ID's.  We extract the
    # block numbers from them to find vehicles late operating trips
    # previous to the ones they're scheduled to operate.
    my %leftover_trips;
    foreach my $trip_id (@leftover_trip_update_trip_ids) {
	my $trip = $gtfs->get_trip_by_trip_id($trip_id, $agency_name, $time_t);
	$leftover_trips{$trip_id} = $trip;
	my $block_id = $trip ? $trip->{block_id} : undef;
	my $trip_update = $trip_update_trips{$trip_id};
	if (defined $block_id && $look_for_block_id{$block_id}) {
	    # We found a vehicle running late on a trip previous to a
	    # currently scheduled one.  Or a vehicle done on its
	    # currently scheduled trip (perhaps a little early) and
	    # taking recovery time for its next trip.
	    $found_block_id{$block_id} = 1;
	    push(@leftover_trip_ids_A, $trip_id);
	} elsif ($trip_update && $trip_update->{stop_time_update}) {
	    # Probably a bus taking recovery time.
	    push(@leftover_trip_ids_B, $trip_id);
	} else {
	    # Probably a bus that's completed its last trip.
	    push(@leftover_trip_ids_C, $trip_id);
	}
    }

    #------------------------------------------------------------------------------

    my $add_row = sub {
	my ($trip, $trip_update) = @_;

	my $trip_update_label;
	my $trip_update_route_id;
	my $trip_update_delay;

	my @flags;

	if ($trip_update) {
	    $trip_update_label    = eval { $trip_update->{vehicle}->{label} } // "-";
	    $trip_update_route_id = eval { $trip_update->{trip}->{route_id} } // "-";
	    if ($trip_update->{stop_time_update} &&
		  $trip_update->{stop_time_update}->[0]) {
		$trip_update_delay =
		  eval { $trip_update->{stop_time_update}->[0]->{departure}->{delay} } // 
		  eval { $trip_update->{stop_time_update}->[0]->{arrival}->{delay} } //
		  0;
		if ($trip_update_delay < 0) {
		    push(@flags, int(-$trip_update_delay/60 + 0.5) . "m EARLY");
		} elsif ($trip_update_delay >= 300) {
		    push(@flags, int($trip_update_delay/60 + 0.5) . "m LATE");
		}
	    } else {
		$trip_update_delay = "-";
	    }
	} else {
	    push(@flags, "NO TRIP UPDATE");
	    $trip_update_label    = "-";
	    $trip_update_route_id = "-";
	    $trip_update_delay    = "-";
	}

	my $vehicle_position = $vehicle_position{$trip_update_label};
	my $vehicle_position_timestamp = $vehicle_position && $vehicle_position->{timestamp};
	my $vehicle_position_age = defined $vehicle_position_timestamp && $time_t - $vehicle_position_timestamp;

	if ($vehicle_position_age && $vehicle_position_age > 300) {
	    push(@flags, "INVALID");
	}

	$tb->add(
	    @{$trip}{qw(trip_id block_id route_short_name trip_headsign trip_departure_time trip_arrival_time)},
	    $trip_update_label,
	    $trip_update_route_id,
	    $trip_update_delay,
	    join(", ", @flags)
	   );
    };

    foreach my $trip_id (@accounted_for_trip_ids) {
	my $trip = $scheduled_trips{$trip_id};
	my $trip_update = $trip_update_trips{$trip_id};
	$add_row->($trip, $trip_update);
    }

    if (@unaccounted_for_trip_ids) {
	$tb->add("?");
	foreach my $trip_id (@unaccounted_for_trip_ids) {
	    my $trip = $scheduled_trips{$trip_id};
	    $add_row->($trip, undef);
	}
    }

    if (@leftover_trip_ids_A) {
	$tb->add("A");
	foreach my $trip_id (@leftover_trip_ids_A) {
	    my $trip_update = $trip_update_trips{$trip_id};
	    my $trip = $leftover_trips{$trip_id};
	    $add_row->($trip, $trip_update);
	}
    }

    if (@leftover_trip_ids_B) {
	$tb->add("B");
	foreach my $trip_id (@leftover_trip_ids_B) {
	    my $trip_update = $trip_update_trips{$trip_id};
	    my $trip = $leftover_trips{$trip_id};
	    $add_row->($trip, $trip_update);
	}
    }

    if (@leftover_trip_ids_C) {
	$tb->add("C");
	foreach my $trip_id (@leftover_trip_ids_C) {
	    my $trip_update = $trip_update_trips{$trip_id};
	    my $trip = $leftover_trips{$trip_id};
	    $add_row->($trip, $trip_update);
	}
    }

    print($tb->title);
    print($tb->rule("-"));
    print($tb->body);
}

sub json {
    my ($self) = @_;
    return $self->{json} if $self->{json};
    $self->{json} = JSON->new()->pretty()->allow_blessed()->convert_blessed();
    return $self->{json};
}

1;

