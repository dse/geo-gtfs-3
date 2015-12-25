use 5.006;
use strict;
use warnings;
use ExtUtils::MakeMaker;

WriteMakefile(
    NAME                => 'Geo::GTFS3',
    AUTHOR              => q{Darren Embry <dse@webonastick.com>},
    VERSION_FROM        => 'lib/Geo/GTFS3.pm',
    ABSTRACT_FROM       => 'lib/Geo/GTFS3.pm',
    ($ExtUtils::MakeMaker::VERSION >= 6.3002
      ? ('LICENSE'=> 'perl')
      : ()),
    PL_FILES            => {},
    PREREQ_PM => {
        'Test::More' => 0,
    },
    dist                => { COMPRESS => 'gzip -9f', SUFFIX => 'gz', },
    clean               => { FILES => 'Geo-GTFS3-*' },
);
