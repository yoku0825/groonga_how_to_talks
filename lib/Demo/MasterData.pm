package Demo::MasterData;

use DBI;

sub new
{
  my ($class, $csv_file, $dsn)= @_;
  $self= {csv_file => $csv_file,
          conn     => DBI->connect($dsn, "root", "", {PrintError => 1})};

  bless $self => $class;

  $self->{conn}->do("SET SESSION sql_mode= ''");
  $self->create_tweet_master;
  $self->fill_tweet_master;
  $self->create_traffic_table;
  $self->fill_traffic_table;
  return $self;
}


sub create_tweet_master
{
  my ($self)= @_;
  my $conn= $self->{conn};

  my $schema_ddl= << 'DDL';
CREATE DATABASE IF NOT EXISTS tweets
DDL
  my $table_ddl= << 'DDL';
CREATE TABLE IF NOT EXISTS tweets.tweets (
  tweet_id bigint unsigned primary key,
  timestamp timestamp NOT NULL,
  text text NOT NULL,
  FULLTEXT KEY(text) COMMENT
    'parser "TokenBigram",
     normalizer "NormalizerMySQLUnicodeCIExceptKanaCIKanaWithVoicedSoundMark"')
Engine= mroonga
DDL

  $conn->do($schema_ddl) or die "$!";
  $conn->do($table_ddl)  or die "$!";
} 


sub fill_tweet_master
{
  my ($self)= @_;
  my $conn= $self->{conn};

  my $truncate_ddl= << 'DDL';
TRUNCATE tweets.tweets
DDL
  my $load_data_dml= << 'DML';
LOAD DATA INFILE ? INTO TABLE tweets.tweets 
  FIELDS TERMINATED BY ',' ENCLOSED BY '"' 
  IGNORE 1 ROWS
  (tweet_id, @dummy, @dummy, @timestamp, @dummy, text, @dummy)
  SET timestamp= DATE_ADD(@timestamp, INTERVAL 9 HOUR)
DML

  $conn->do($truncate_ddl)                            or die "$!";
  $conn->do($load_data_dml, undef, $self->{csv_file}) or die "$!";
}


sub drop_tweet_master
{
  my ($self)= @_;
  my $conn= $self->{conn};
  my $drop_ddl= << 'DDL';
DROP DATABASE IF EXISTS tweets
DDL
 
  $conn->do($drop_ddl) or die "$!";
}


sub create_traffic_table
{
  my ($self)= @_;
  my $conn= $self->{conn};

  my $table_ddl= << 'DDL';
CREATE TABLE IF NOT EXISTS tweets.traffic (
  tweet_id bigint unsigned auto_increment primary key,
  timestamp datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  text text NOT NULL,
  FULLTEXT KEY(text) COMMENT
    'parser "TokenBigram",
     normalizer "NormalizerMySQLUnicodeCIExceptKanaCIKanaWithVoicedSoundMark"')
Engine= mroonga
DDL
  my $truncate_ddl= << 'DDL';
TRUNCATE tweets.traffic
DDL

  $conn->do($table_ddl)    or die "$!";
  $conn->do($truncate_ddl) or die "$!";
}


sub fill_traffic_table
{
  my ($self)= @_;
  $conn= $self->{conn};

  my $insert_dml= << 'DDL';
INSERT INTO tweets.traffic (text)
SELECT text FROM tweets.tweets
DDL

  for (my $n= 0; $n <= 500; $n++)
    {$conn->do($insert_dml);}

  return 1;
}


sub destroy
{
  my ($self)= @_;

  $self->{conn}->disconnect;
}


return 1;

