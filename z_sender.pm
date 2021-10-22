package z_sender;

use utf8;
use strict;
use File::Temp qw(tempfile);

sub zabbix_sender {
my (%work) = @_;

my $sender_spool = "/tmp/zabbix_sender_spool";
my $sender_bin   = "/usr/bin/zabbix_sender";
my $proxyadres   = "127.0.0.1";

# Сохраняем данные во временный(е) файлы
if ( defined $work{sender} ) {
  my $f_handle;
  my $f_name;
  $work{sender_file_str}=0;
  foreach my $host ( keys $work{sender} ) {
    if ( ! defined $work{sender}{$host}{env} ) { next; }
    foreach my $env ( keys $work{sender}{$host}{env} ) {
      if ( ! defined $work{sender}{$host}{env}{$env}->{value} ) { next;}
      my $value    = $work{sender}{$host}{env}{$env}->{value}; my $unixtime = time();
      if ( defined $work{sender}{$host}{env}{$env}->{unixtime} ) {
        $unixtime = $work{sender}{$host}{env}{$env}->{unixtime};
        }
      if ( ! defined $f_handle ) {
        ($f_handle, $f_name ) = tempfile('zabbix-sender-XXXX', SUFFIX => '.txt', DIR => $sender_spool, UNLINK => 0 );
        $f_handle->autoflush(1);
        push @{$work{sender_file}}, $f_name;
        }
      if ( defined $f_handle and defined $host and defined $env and defined $unixtime and defined $value ) {
        print $f_handle '"'.$host.'"  "'.$env.'"  "'.$unixtime. '"  "'.$value.'"'."\n";
        $work{sender_file_str}++;
        }
      if ( defined $f_handle and $work{sender_file_str} > 245 ) {
        $work{sender_file_str} = 0;
        $f_handle->close;
        undef $f_handle;
        undef $f_name;
        }
      }
    }
  }
# Вызываем sender напровляем данные
if ( ! -e $sender_bin or ! -x $sender_bin ) {
  print "error no file sender_server [$sender_bin]\n";
  exit;
  }
if ( defined $work{sender_file} and ref($work{sender_file}) eq 'ARRAY' ) {
  foreach my $file ( @{$work{sender_file}} ) {
    system( $sender_bin." -z ". $proxyadres." -i ".$file." -T" );
    unlink ( $file );
    }
  }
}
#-------------------------------------------------------------------------------------------

1;