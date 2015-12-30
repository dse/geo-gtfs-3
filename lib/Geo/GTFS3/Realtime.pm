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

    $self->{pb} = $$ccref;
    $self->{data} = $o;
    $self->{data_json} = $self->json->encode($o);

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

sub json {
    my ($self) = @_;
    return $self->{json} if $self->{json};
    $self->{json} = JSON->new()->pretty()->allow_blessed()->convert_blessed();
    return $self->{json};
}

1;

