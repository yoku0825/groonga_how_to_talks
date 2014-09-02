#!/usr/bin/perl

use Test::More;
use FindBin qw/$Bin/;
use lib "$Bin/../lib";
use Demo::Traffic;

my $dsn= "dbi:mysql:tweets";
ok(my $data= Demo::Traffic->new($dsn), "connect to MySQL");
ok($data->exec_insert_traffic(), "INSERT traffic");
ok($data->exec_select_traffic(), "SELECT traffic");
ok($data->destroy, "connection close");

done_testing();

exit 0;
