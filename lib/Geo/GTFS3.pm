package Geo::GTFS3;
use warnings;
use strict;

use Geo::GTFS3::DBI;

use HTTP::Cache::Transparent;
use LWP::UserAgent;
use Archive::Zip qw( :ERROR_CODES :CONSTANTS );
use Archive::Zip::MemberRead;
use Digest::MD5 qw/md5_hex/;
use File::Path qw(make_path);
use Text::CSV;
use File::Basename qw(dirname basename);

our @TABLES;
our %TABLES;

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
    $self->{dbfile} = "$dir/gtfs.sqlite";
    $self->{verbose} = 0;
}

sub dbh {
    my ($self) = @_;
    return $self->{dbh} if $self->{dbh};
    my $dbfile = $self->{dbfile};
    $self->{dbh} = Geo::GTFS3::DBI->connect("dbi:SQLite:$dbfile", "", "",
					    { RaiseError => 1, AutoCommit => 0 });
    $self->create_tables();
    return $self->{dbh};
}

sub load_from_url {
    my ($self, $url, $inactive) = @_;
    HTTP::Cache::Transparent::init({ BasePath => $self->{http_cache_dir} });
    my $request = HTTP::Request->new("GET", $url);
    my $response = $self->ua->request($request);
    if (!$response->is_success) {
	warn(sprintf("%s => %s\n", $response->base, $response->status_line));
	return;
    }
    if ($self->{verbose}) {
	warn("GET $url\n");
	warn(sprintf("  %s\n", $response->base)) if $response->base ne $url;
	warn(sprintf("  %s\n", $response->status_line));
    }
    $self->load_from_http_response($response, $inactive);
}

sub load_from_http_response {
    my ($self, $response, $inactive) = @_;

    my $url           = $response->base;
    my $last_modified = $response->last_modified;
    my $length        = $response->content_length;
    my $etag          = $response->header("Etag") // "";
    my $md5           = md5_hex(join($;, $url, $last_modified, $length, $etag));

    my $filename = sprintf("%s/data-cache/%s.zip", $self->{dir}, $md5);
    if (!-e $filename) {
	my $cref = $response->content_ref;
	make_path(dirname($filename));
	open(my $fh, ">", $filename) or die("Cannot write $filename: $!\n");
	binmode($fh);
	print {$fh} $$cref;
	close($fh);
	warn("Saved feed as $filename\n");
    }

    my $instance = $self->get_instance($url, $last_modified, $length, $etag);
    if ($instance) {
	my $instance_id = $instance->{instance_id};
	warn("Already in database as instance_id $instance_id\n");
    } else {
	my $retrieved = $response->date;
	my $instance_id = $self->create_instance($url, $last_modified, $length, $etag,
						 $retrieved, $filename);
	warn("Created new instance id: $instance_id\n");
	$self->load_from_zip($instance_id, $filename, $inactive);
    }
}

use IO::Handle;			# for STDERR->autoflush() call

sub load_from_zip {
    my ($self, $instance_id, $filename, $inactive) = @_;
    my $zip = Archive::Zip->new();
    unless ($zip->read($filename) == AZ_OK) {
	die("zip read error $filename\n");
    }

    foreach my $table (@TABLES) {
	my $sql = "
            delete from $table where instance_id = ?
        ";
	my $sth = $self->dbh->prepare($sql);
	$sth->execute();
    }

    my @members = $zip->members;
    foreach my $member (@members) {
	my $filename = $member->fileName();
	my $basename = basename($filename, ".txt");
	my $table_name = "$basename";
	my $fh = Archive::Zip::MemberRead->new($zip, $filename);
	my $csv = Text::CSV->new ({ binary => 1 });
	my $fields = $csv->getline($fh);
	die("no fields in member $filename of $filename\n")
	  unless $fields or scalar(@$fields);
	my $sql = sprintf("insert into $table_name(instance_id, %s) values(?, %s);",
			  join(", ", @$fields),
			  join(", ", ("?") x scalar(@$fields)));
	my $sth = $self->dbh->prepare($sql);
	warn("Populating $table_name ...\n");

	STDERR->autoflush(1);
	
	my $rows = 0;
	while (defined(my $data = $csv->getline($fh))) {
	    $sth->execute($instance_id, @$data);
	    $rows += 1;
	    if ($rows % 100 == 0) {
		print STDERR ("  $rows rows\r");
	    }
	}
	$self->dbh->commit();
	print STDERR ("  loaded $rows rows.\n");
    }
}

sub get_instance {
    my ($self, $url, $last_modified, $length, $etag) = @_;
    $etag //= "";
    my $sql = "
	select *
	from   instances
	where  url = ?
	       and modified = ?
	       and length = ?
	       and etag = ?;
    ";
    my $sth = $self->dbh->prepare($sql);
    $sth->execute($url, $last_modified, $length, $etag);
    return $sth->fetchrow_hashref();
}

sub create_instance {
    my ($self, $url, $last_modified, $length, $etag, $retrieved, $filename) = @_;
    my $sql = "
        insert into instances(
            url, modified, length, etag, retrieved, filename
        )
        values(?, ?, ?, ?, ?, ?);
    ";
    my $sth = $self->dbh->prepare($sql);
    $sth->execute($url, $last_modified, $length, $etag, $retrieved, $filename);
    my $id = $self->dbh->last_insert_id("", "", "", "");
    $self->dbh->commit();
    return $id;
}

# table column names in the 
our @WDAY_COLUMN_NAMES;
BEGIN {
    @WDAY_COLUMN_NAMES = qw(sunday monday tuesday wednesday thursday
			    friday saturday);
}

use POSIX qw(strftime);

sub get_instance_id_service_id_from_agency_name {
    my ($self, $agency_name, $date) = @_;
    $date //= time();
    my @localtime = localtime($date);
    my $wday = $localtime[6];
    my $column_name = $WDAY_COLUMN_NAMES[$wday];
    my $yyyymmdd = strftime("%Y%m%d", @localtime);
    {
	my $sql = "
            select a.instance_id as instance_id, cd.service_id as service_id
            from agency a
                   join calendar_dates cd on a.instance_id = cd.instance_id
                   join instances i on a.instance_id = i.instance_id
            where a.agency_name = ? and cd.date = ? and cd.exception_type = 2
            order by i.modified desc, i.retrieved desc
        ";
	my $sth = $self->dbh->prepare($sql);
	$sth->execute($agency_name, $yyyymmdd);
	my $row = $sth->fetchrow_hashref();
	if ($row) {
	    return ($row->{instance_id}, $row->{service_id}, 1);
	}
    }
    {
	my $sql = "
            select a.instance_id as instance_id, c.service_id as service_id
            from agency a
                   join calendar c on a.instance_id = c.instance_id
                   join instances i on a.instance_id = i.instance_id
            where $column_name and a.agency_name = ? and ? between c.start_date and c.end_date
            order by i.modified desc, i.retrieved desc
        ";
	my $sth = $self->dbh->prepare($sql);
	$sth->execute($agency_name, $yyyymmdd);
	my $row = $sth->fetchrow_hashref();
	if ($row) {
	    return ($row->{instance_id}, $row->{service_id});
	}
    }
}

sub ua {
    my ($self) = @_;
    return $self->{ua} if $self->{ua};
    $self->{ua} = LWP::UserAgent->new();
    return $self->{ua};
}

BEGIN {
    @TABLES = qw(instances
		 agency
		 stops
		 routes
		 trips
		 stop_times
		 calendar
		 calendar_dates
		 fare_attributes
		 fare_rules
		 shapes
		 frequencies
		 transfers
		 feed_info);
    $TABLES{instances} = "
        create table if not exists instances (
            instance_id           integer             primary key autoincrement,
            url                   text       not null,
            modified              datetime       null,
            length                integer    not null,
            etag                  text           null,
            retrieved             datetime   not null,
            filename              text       not null
        );
    ";
    $TABLES{agency} = "
        create table if not exists agency (
            instance_id           integer    not null references instances(instance_id),
	    agency_id             text           null,
	    agency_name           text       not null,
	    agency_url            text       not null,
	    agency_timezone       text       not null,
	    agency_lang           varchar(2)     null,
	    agency_phone          text           null,
	    agency_fare_url       text           null
        );
        create index if not exists agency__instance_id on agency(instance_id);
        create index if not exists agency__agency_id   on agency(agency_id);
    ";
    $TABLES{stops} = "
        create table if not exists stops (
            instance_id           integer    not null references instances(instance_id),
            stop_id               text       not null,
            stop_code             text           null,
            stop_name             text       not null,
            stop_desc             text           null,
            stop_lat              real       not null,
            stop_lon              real       not null,
            zone_id               text           null,
            stop_url              text           null,
            location_type         integer        null,
            parent_station        text           null references stops(stop_id),
            stop_timezone         text           null,
            wheelchair_boarding   integer        null
        );
        create index if not exists stops__instance_id on stops(instance_id);
        create index if not exists stops__stop_id     on stops(stop_id);
    ";
    $TABLES{routes} = "
        create table if not exists routes (
            instance_id           integer    not null references instances(instance_id),
            route_id              text       not null,
            agency_id             text           null references agency(agency_id),
            route_short_name      text       not null,
            route_long_name       text       not null,
            route_desc            text           null,
            route_type            integer    not null,
            route_url             text           null,
            route_color           varchar(6)     null,
            route_text_color      varchar(6)     null
        );
        create index if not exists routes__instance_id on routes(instance_id);
        create index if not exists routes__route_id    on routes(route_id);
        create index if not exists routes__agency_id   on routes(agency_id);
        create index if not exists routes__short_name  on routes(short_name);
    ";
    $TABLES{trips} = "
        create table if not exists trips (
            instance_id           integer    not null references instances(instance_id),
            route_id              text       not null references routes(route_id),
            service_id            text       not null,
            trip_id               text       not null,
            trip_headsign         text           null,
            trip_short_name       text           null,
            direction_id          integer        null,
            block_id              text           null,
            shape_id              text           null,
            wheelchair_accessible integer        null,
            bikes_allowed         integer        null
        );
        create index if not exists trips__instance_id on trips(instance_id);
        create index if not exists trips__route_id    on trips(route_id);
        create index if not exists trips__service_id  on trips(service_id);
        create index if not exists trips__trip_id     on trips(trip_id);
        create index if not exists trips__block_id    on trips(block_id);
    ";
    $TABLES{stop_times} = "
        create table if not exists stop_times (
            instance_id           integer    not null references instances(instance_id),
            trip_id               text       not null references trips(trip_id),
            arrival_time          varchar(8) not null,
            departure_time        varchar(8) not null,
            stop_id               text       not null references stops(stop_id),
            stop_sequence         integer    not null,
            stop_headsign         text           null,
            pickup_type           integer        null,
            drop_off_type         integer        null,
            shape_dist_traveled   real           null,
            timepoint             integer        null
        );
        create index if not exists stop_times__instance_id   on stop_times(instance_id);
        create index if not exists stop_times__trip_id       on stop_times(trip_id);
        create index if not exists stop_times__stop_id       on stop_times(stop_id);
        --create index if not exists stop_times__stop_sequence on stop_times(stop_sequence);
        drop index if exists stop_times__stop_sequence;
    ";
    $TABLES{calendar} = "
        create table if not exists calendar (
            instance_id           integer    not null references instances(instance_id),
            service_id            text       not null,
            monday                integer    not null,
            tuesday               integer    not null,
            wednesday             integer    not null,
            thursday              integer    not null,
            friday                integer    not null,
            saturday              integer    not null,
            sunday                integer    not null,
            start_date            varchar(8) not null,
            end_date              varchar(8) not null
        );
        create index if not exists calendar__instance_id on calendar(instance_id);
        create index if not exists calendar__monday      on calendar(monday);
        create index if not exists calendar__tuesday     on calendar(tuesday);
        create index if not exists calendar__wednesday   on calendar(wednesday);
        create index if not exists calendar__thursday    on calendar(thursday);
        create index if not exists calendar__friday      on calendar(friday);
        create index if not exists calendar__saturday    on calendar(saturday);
        create index if not exists calendar__sunday      on calendar(sunday);
    ";
    $TABLES{calendar_dates} = "
        create table if not exists calendar_dates (
            instance_id           integer    not null references instances(instance_id),
            service_id            text       not null,
            date                  varchar(8) not null,
            exception_type        integer    not null
        );
        create index if not exists calendar_dates__instance_id on calendar_dates(instance_id);
    ";
    $TABLES{fare_attributes} = "
        create table if not exists fare_attributes (
            instance_id           integer    not null references instances(instance_id),
            fare_id               text       not null,
            price                 real       not null,
            currency_type         varchar(3) not null,
            payment_method        integer    not null,
            transfers             integer    not null,
            transfer_duration     integer        null
        );
        create index if not exists fare_attributes__instance_id on fare_attributes(instance_id);
    ";
    $TABLES{fare_rules} = "
        create table if not exists fare_rules (
            instance_id           integer    not null references instances(instance_id),
            fare_id               text           null,
            route_id              text           null references routes(route_id),
            origin_id             text           null,
            destination_id        text           null,
            contains_id           text           null
        );
        create index if not exists fare_rules__instance_id on fare_rules(instance_id);
    ";
    $TABLES{shapes} = "
        create table if not exists shapes (
            instance_id           integer    not null references instances(instance_id),
            shape_id              text       not null,
            shape_pt_lat          real       not null,
            shape_pt_lon          real       not null,
            shape_pt_sequence     integer    not null,
            shape_dist_traveled   real           null
        );
        create index if not exists shapes__instance_id       on shapes(instance_id);
        create index if not exists shapes__shape_id          on shapes(shape_id);
        --create index if not exists shapes__shape_pt_sequence on shapes(shape_pt_sequence);
        drop index if exists shapes__shape_pt_sequence;
    ";
    $TABLES{frequencies} = "
        create table if not exists frequencies (
            instance_id           integer    not null references instances(instance_id),
            trip_id               text       not null references trips(trip_id),
            start_time            varchar(8) not null,
            end_time              varchar(8) not null,
            headway_secs          integer    not null,
            exact_times           integer        null
        );
        create index if not exists frequencies__instance_id on frequencies(instance_id);
    ";
    $TABLES{transfers} = "
        create table if not exists transfers (
            instance_id           integer    not null references instances(instance_id),
            from_stop_id          text       not null references stops(stop_id),
            to_stop_id            text       not null references stops(stop_id),
            transfer_type         integer    not null,
            min_transfer_time     integer        null
        );
        create index if not exists transfers__instance_id on transfers(instance_id);
    ";
    $TABLES{feed_info} = "
        create table if not exists feed_info (
            instance_id           integer    not null references instances(instance_id),
            feed_publisher_name   text       not null,
            feed_publisher_url    text       not null,
            feed_lang             varchar(2) not null,
            feed_start_date       varchar(8)     null,
            feed_end_date         varchar(8)     null,
            feed_version          text           null
        );
        create index if not exists feed_info__instance_id on feed_info(instance_id);
    ";
}

sub create_tables {
    my ($self) = @_;
    foreach my $table (@TABLES) {
	$self->dbh->do($TABLES{$table});
    }
}

sub DESTROY {
    my ($self) = @_;
    $self->dbh->rollback() if $self->dbh;
}

1;

