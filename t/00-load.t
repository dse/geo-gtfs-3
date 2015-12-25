#!perl -T

use Test::More tests => 1;

BEGIN {
    use_ok( 'Geo::GTFS3' ) || print "Bail out!\n";
}

diag( "Testing Geo::GTFS3 $Geo::GTFS3::VERSION, Perl $], $^X" );
