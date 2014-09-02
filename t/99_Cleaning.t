#!/usr/bin/perl

use Test::More skip_all;
use FindBin qw/$Bin/;
use lib "$Bin/../lib";
use Demo::MasterData;

my $dsn= "dbi:mysql:information_schema";
ok(my $data= Demo::MasterData->new("/data/tmp/tweets.csv", $dsn), "connect to MySQL");
ok($data->drop_tweet_master()  , "DROP DATABASE");
ok($data->destroy);

done_testing();

exit 0;
