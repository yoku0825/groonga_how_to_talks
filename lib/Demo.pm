package Demo;

use Demo::MasterData;
use Demo::Traffic;
use Data::Dumper;

my $csv= "/data/traffic/data/tweets.csv";
my $dsn= "dbi:mysql:information_schema";

sub init
{
  my $demo= Demo::MasterData->new($csv, $dsn);
}


sub insert
{
  my ($count)= @_;
  my $demo= Demo::Traffic->new($dsn);

  $count= 65535 unless defined($count);
  for (my $n= 1; $n <= $count; $n++)
    {$demo->exec_insert_traffic();}
}


sub select
{
  my ($count)= @_;
  my $demo= Demo::Traffic->new($dsn);

  $count= 65535 unless defined($count);
  for (my $n= 1; $n <= $count; $n++)
    {$demo->exec_select_traffic();}
}


return 1;

