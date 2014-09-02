package Demo::Traffic;

use Time::HiRes qw/usleep/;
use DBI;
use Data::Dumper;

sub new
{
  my ($class, $dsn)= @_;
  my $self= {conn   => DBI->connect($dsn, "root", "", {PrintError => 1}),
             tweets => []};
  bless $self => $class;

  $self->pick_insert_traffic();

  return $self;
}



sub pick_insert_traffic
{
  my ($self)= @_;
  my $conn= $self->{conn};

  my $sql= << "DML";
SELECT *
FROM   tweets.tweets
WHERE  MATCH(text) AGAINST('清楚かわいい' IN BOOLEAN MODE)
  AND  text NOT LIKE '%RT%'
DML

  $self->{tweets}= $conn->selectall_arrayref($sql, {Slice => {}});
}


sub exec_insert_traffic
{
  my ($self)= @_;
  my $conn= $self->{conn};
  my $tweets= $self->{tweets};

  my $insert_dml= << 'DML';
INSERT INTO tweets.traffic (text) VALUES (?)
DML

  foreach my $row (@$tweets)
  {
    $conn->do($insert_dml, undef, $row->{text});
    usleep(rand(500000));
  }
  return 1;
}


sub exec_select_traffic
{
  my ($self)= @_;
  my $conn= $self->{conn};

  my $select_dml= << 'DML';
SELECT *
FROM tweets.traffic
WHERE MATCH(text) AGAINST ('清楚かわいい' IN BOOLEAN MODE)
DML

  $conn->do($select_dml);
}


sub destroy
{
  my ($self)= @_;
  $self->{conn}->disconnect;
}


return 1;
