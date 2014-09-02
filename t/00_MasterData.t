#!/usr/bin/perl

use Test::More;
use FindBin qw/$Bin/;
use lib "$Bin/../lib";
use Demo::MasterData;

my $dsn= "dbi:mysql:information_schema";
ok(my $data= Demo::MasterData->new("/data/tmp/tweets.csv", $dsn), "connect to MySQL");
ok($data->create_tweet_master(), "CREATE DATABASE and TABLE");
ok($data->fill_tweet_master()  , "LOAD DATA INFILE");
ok($data->create_traffic_table, "CREATE TABLE traffic");
ok($data->fill_traffic_table  , "INSERT INTO .. SELECT");
ok($data->destroy);

done_testing();

exit 0;
