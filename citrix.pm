package citrix;

use utf8;
use strict;
use LWP::UserAgent;
use LWP;
use Encode;
use LWP::UserAgent;
use HTTP::Request::Common;
use JSON::XS;
use JSON;
use DateTime;

sub new {
my $instance = shift;
my $class = ref($instance) || $instance;
my (%args) = @_;
my $self = {
    'user'     => $args{user} || 'citrix-user',
    'password' => $args{password} || 'citrix-password',
    'host'     => $args{host} || 'ctxdc-host',
    'protocol' => $args{protocol} || 'http',
    'web_pach' => $args{web_pach} || 'Citrix/Monitor/OData',
    'port'     => undef,
    'ua'       => undef,
    'headers'  => undef,
    'cache' => { 'users' => undef, 
                 'machines' => undef,
                 'app' => undef,
                 'dg' => undef,
                 'enum' => undef,
                },

    'cache_files' => '/......./cache.json',
    };
bless($self, $class);
return $self;
}
#-------------------------------------------------------------------------------------------

sub init {
my $self = shift;

if ( $self->{protocol} eq 'http' ) {
  $self->{port} = 80;
  }
elsif ( $self->{protocol} eq 'https' ) {
  $self->{port} = 443;
  }
else {
  $self->{protocol} = 'http';
  $self->{port} = 80;
  }

my $credentials_host = "$self->{host}:$self->{port}";
$self->{ua} = new LWP::UserAgent( keep_alive => 1 );
$self->{ua}->credentials( $credentials_host, '', $self->{user}, $self->{password} );
$self->{headers} = HTTP::Headers->new( 'accept'=>'application/json' );


my $url = "$self->{protocol}://$self->{host}/$self->{web_pach}/v1/Data";
eval {
  my $rq = HTTP::Request->new('GET',$url,$self->{headers} );
  my $resp = $self->{ua}->request( $rq );
  my $content = undef;
  if ( defined $resp and $resp->{_previous}->{_content} ) {
    $content = $resp->{_previous}->{_content};
    }

  if ( defined $content and $content =~ /$self->{host}/ ) {
    return $self;
    }
  else {
    return undef;
    }
  };
}
#-------------------------------------------------------------------------------------------

sub do {
my $self = shift;
my (%args) = @_;

my $sets   = $args{sets} || undef;
my $top    = $args{top} || undef;
my $select = $args{select} || undef;
my $filter = $args{filter} || undef;
my $skip   = $args{skip} || undef;
my $type   = $args{type} || 'v1';     # type   => v2 / v3 / v4
my $folder = $args{folder} || 'Data'; # folder => Data / Methods

my $url;
if ( defined $sets ) {
  $url = "$self->{protocol}://$self->{host}/$self->{web_pach}/$type/$folder/$sets";
  }
else {
  return undef;
  }
if ( defined $top or defined $select or defined $filter or defined $skip ) {
  $url .= "()?";
  }
if ( defined $top )    { $url .= '$top='.$top.'&';        }
if ( defined $select ) { $url .= '$select='.$select.'&';  }
if ( defined $filter ) { $url .= '$filter='.$filter.'&';  }
if ( defined $skip )   { $url .= '$skip='.$skip.'&';      }

my $hash = undef;
eval {
  my $rq = HTTP::Request->new( 'GET', $url, $self->{headers} );
  my $resp = $self->{ua}->request( $rq );
  if ( defined $resp and defined $resp->is_success) {
    #  print $resp->content;
    $hash = decode_json( $resp->content );
    if ( defined $hash->{value} ) {
      $hash = $hash->{value};
      }
    }
  else {
    print "Error { $url }: " . $resp->status_line . "\n" . $resp->as_string;
    return undef;
    }
  };
if ($@) {
  return undef;
  }
return $hash;
}
#-------------------------------------------------------------------------------------------


sub Get {
my $self = shift;
my (%args) = @_;

my $sets       = $args{sets} || undef;
my $select     = $args{select} || undef;
my $arg_filter = $args{filter} || undef;
my $top        = $args{top} || undef;
my $type       = $args{type} || 'v3';
my $time_zone  = $args{time_zone} || 'Europe/Moscow';    # временная зона по умолчанию
                                                         # time_zone предписывает конвертацию входящих данных и найденных и возвращаемых

my $filter = undef;
# Операторы равенства:
#  eq: является ли поле равным постоянному значению
#  ne: является ли поле не равным постоянному значению

# Операторы диапазона:
#  gt: больше ли поле постоянного значения
#  lt: меньше ли поле постоянного значения
#  ge: больше или равно поле постоянному значению
#  le: меньше или равно поле постоянному значению

if ( defined $arg_filter and ref($arg_filter) eq 'HASH' ) {
  my %hash = %{$arg_filter};
  foreach my $arg ( keys %hash ) {
    my $value = $hash{$arg};
    my $text = "";

    $arg = lc($arg);

    if ( $arg eq 'id' ) {
      if ( $value =~/^[a-z\d]{1,}\-[a-z\d]{1,}\-[a-z\d]{1,}\-[a-z\d]{1,}\-[a-z\d]{1,}$/ ) {
        $text = "Id eq guid\'$value\'";
        }
      if ( $value =~/^\d{1,}$/ ) {
        $text = "Id eq $value";
        }
      }

    if ( defined $time_zone and $arg =~/^(created|modified|summary|failure|start|end|faultreported)date($|_ge|_le)/ ) {
      # Меняем вренмя на UTC по createddate_ge / createddate_le / createddate
      $value = $self->Time( time=>$value, zone=>$time_zone, type=>'OData', out_zone =>'UTC' );
      }

    if ( $arg eq 'createddate_last') {
      $value = $self->TimeLast( $value )->{last};
      $text = "CreatedDate ge DATETIME\'$value\'";
      }

    if ( $arg eq 'modifieddate_last') {
      $value = $self->TimeLast( $value )->{last};
      $text = "ModifiedDate ge DATETIME\'$value\'";
      }

    if ( $arg eq 'summarydate_last') {
      $value = $self->TimeLast( $value )->{last};
      $text = "SummaryDate ge DATETIME\'$value\'";
      }

    if ( $arg eq 'failuredate_last') {
      $value = $self->TimeLast( $value )->{last};
      $text = "FailureDate ge DATETIME\'$value\'";
      }

    if ( $arg eq 'startdate_last') {
      $value = $self->TimeLast( $value )->{last};
      $text = "StartDate ge DATETIME\'$value\'";
      }

    if ( $arg eq 'failuredate_null') {
      $text = "FailureDate eq null";
      }

    if ( $arg eq 'disconnectdate_null') {
      $text = "DisconnectDate eq null";
      }

    if ( $arg eq 'enddate_null') {
      $text = "EndDate eq null";
      }

    if ( $arg eq 'currentconnectionid' )  { $text = "CurrentConnectionId eq $value"; }

    if ( $arg eq 'hostedmachinename' )    { $text = "HostedMachineName eq \'$value\'"; }

    if ( $arg eq 'createddate_ge' )       { $text = "CreatedDate ge DATETIME\'$value\'"; }
    if ( $arg eq 'createddate_le' )       { $text = "CreatedDate le DATETIME\'$value\'"; }

    if ( $arg eq 'modifieddate_ge' )      { $text = "ModifiedDate ge DATETIME\'$value\'"; }
    if ( $arg eq 'modifieddate_le' )      { $text = "ModifiedDate le DATETIME\'$value\'"; }

    if ( $arg eq 'summarydate_ge' )       { $text = "SummaryDate ge DATETIME\'$value\'"; }
    if ( $arg eq 'summarydate_le' )       { $text = "SummaryDate le DATETIME\'$value\'"; }

    if ( $arg eq 'failuredate_ge' )       { $text = "FailureDate ge DATETIME\'$value\'"; }
    if ( $arg eq 'failuredate_le' )       { $text = "FailureDate le DATETIME\'$value\'"; }

    if ( $arg eq 'failurestartdate_ge' )  { $text = "FailureStartDate ge DATETIME\'$value\'"; }
    if ( $arg eq 'failureenddate_le' )    { $text = "FailureEndDate le DATETIME\'$value\'"; }

    if ( $arg eq 'startdate_ge' )         { $text = "StartDate ge DATETIME\'$value\'"; }
    if ( $arg eq 'startdate_le' )         { $text = "StartDate le DATETIME\'$value\'"; }

    if ( $arg eq 'enddate_ge' )           { $text = "EndDate ge DATETIME\'$value\'"; }
    if ( $arg eq 'enddate_le' )           { $text = "EndDate le DATETIME\'$value\'"; }

    if ( $arg eq 'faultreporteddate_ge' ) { $text = "FaultReportedDate ge DATETIME\'$value\'"; }
    if ( $arg eq 'faultreporteddate_le' ) { $text = "FaultReportedDate le DATETIME\'$value\'"; }

    if ( $arg eq 'failurecode' )          { $text = "FailureCode eq $value"; }

    if ( $arg eq 'sessionkey' )           { $text = "SessionKey eq guid\'$value\'"; }
    if ( $arg eq 'sessionid' )            { $text = "SessionId eq guid\'$value\'"; }
    if ( $arg eq 'machineid' )            { $text = "MachineId eq guid\'$value\'"; }
    if ( $arg eq 'applicationid' )        { $text = "ApplicationId eq guid\'$value\'"; }
    if ( $arg eq 'desktopgroupid' )       { $text = "DesktopGroupId eq guid\'$value\'"; }
    if ( $arg eq 'name' )                 { $text = "Name eq \'$value\'"; }
    if ( $arg eq 'publishedname' )        { $text = "PublishedName eq \'$value\'"; }

    if ( $arg eq 'userid' )               { $text = "UserId eq $value"; }
    if ( $arg eq 'username' )             { $text = "UserName eq \'$value\'"; }
    if ( $arg eq 'fullname' )             { $text = "FullName eq \'$value\'"; }
    if ( $arg eq 'sid' )                  { $text = "Sid eq \'$value\'"; }

    if ( defined $filter and length($text) > 2 ) {
      $filter .= " and $text";
      }
    elsif ( ! defined $filter and length($text) > 2 ) {
      $filter = "$text";
      }
    }
  }

my $get = $self->do( sets => $sets, type => $type, filter => $filter, select => $select, top => $top );

# Дополним вывод, метриками времени в формате UnixTime
my @array;
if ( defined $get and ref($get) eq 'ARRAY' ) {
  foreach my $item ( @$get ) {
    my %item1; # нижний регистр
    if ( defined $item and ref($item) eq 'HASH' ) {
      foreach my $Date_Name ( keys %{$item} ) {
        my $Date_Value = $item->{$Date_Name};
        if ( defined $Date_Name and defined $Date_Value and $Date_Name =~/^(Created|Modified|Failure|FailureStart|FailureEnd|FaultReported|Summary|VMStartStart|VMStartEnd|Brokering|LogOnStart|LogOnEnd|Start|End|ConnectionStateChange|Disconnect|Establishment|LogOnScriptsStart|LogOnScriptsEnd|GpoStart|GpoEnd|InteractiveStart|InteractiveEnd|ProfileLoadStart|ProfileLoadEnd|ClientSessionValidate|ServerSessionValidate|HdxStart|HdxEnd)Date$/ ) {
          $item->{"$Date_Name\_unixtime"} = $self->Time(time=>$item->{$Date_Name},zone=>'UTC',type=>'unixtime');
          $item->{$Date_Name}  = $self->Time(time=>$item->{$Date_Name},zone=>'UTC',type=>'OData',out_zone => $time_zone);
          }
        }
      push @array, $item;
      }
    }
  }
return @array;
}
#-------------------------------------------------------------------------------------------


sub Time {
my $self     = shift;
my (%args)   = @_;

my $time = $args{time} || undef;     # Входящее время в формате OData или UnixTime
my $zone = $args{zone} || undef;     # Входящий часовой пояс
my $type = $args{type} || undef;     # Требуемый тип времени OData или UnixTime
my $out_zone = $args{out_zone} || undef; # Требуемый часовой пояс

# Получаем, определяем полученный формат, переводим в UnixTime и возвращаем
my $dt = undef;

if( defined $time and defined $zone and $time =~ m{^(\d{4})\-(\d{2})\-(\d{2})\T(\d{2})\:(\d{2})\:(\d{2})}x ) {
  $dt = DateTime->new( year => $1, month => $2, day => $3, hour => $4, minute => $5, second => $6, time_zone => $zone );
  }
elsif( defined $time and defined $zone and $time =~ /^\d{1,}$}/ ) {
  $dt = DateTime->from_epoch( epoch => $time, time_zone => $zone );
  }

if ( defined $dt and defined $type and lc($type) eq 'unixtime' ) {
  return $dt->epoch;
  }

if ( defined $dt and defined $type and defined $out_zone and lc($type) eq 'odata' ) {
  $dt->set_time_zone($out_zone);
  return  $dt->strftime("%FT%Tz");
  }

return undef;
}
#-------------------------------------------------------------------------------------------

sub TimeLast {
my $self = shift;
my $last = shift;

my $dt_now  = DateTime->from_epoch( epoch => time() )->set_time_zone( 'UTC' );
my $dt_last = DateTime->from_epoch( epoch => ( time() - $last ) )->set_time_zone( 'UTC' );

my $form_now  = $dt_now->strftime("%FT%Tz");
my $form_last = $dt_last->strftime("%FT%Tz");

return { now  => $form_now,
         last => $form_last,
       };
}
#-------------------------------------------------------------------------------------------

sub LoadCache {
my $self = shift;
# Загрузить кеш из файла

print "LoadCache load files ...";

my $text = undef;
my %cache = ();

if ( open ( FILE , '<' , $self->{cache_files} ) ) {
  my $pos=0;
  while ( my $row = <FILE> ) {
    chomp $row;
    $text .= $row;
    }
  close FILE;
  }

# из текста в JSON, после в хеш массив
if ( defined $text and length($text) > 5 ) {
  eval {  my $json = decode_json( $text );
          %cache = %$json;
          }
  }
if ( ref($cache{app}) eq 'HASH' ) {
  $self->{cache}{app} = $cache{app};
  }
if ( ref($cache{machines}) eq 'HASH' ) {
  $self->{cache}{machines} = $cache{machines};
  }
if ( ref($cache{users}) eq 'HASH' ) {
  $self->{cache}{users} = $cache{users};
  }
if ( ref($cache{dg}) eq 'HASH' ) {
  $self->{cache}{dg} = $cache{dg};
  }

if ( ref($cache{enum}) eq 'HASH' ) {
  $self->{cache}{enum} = $cache{enum};
  }
print "[ok]\n";
return $self;
}
#-------------------------------------------------------------------------------------------


sub SaveCache {
my $self = shift;
# Сохранить кеш в файл
print "SaveCache saves files ...";
my $json = to_json( \%{$self->{cache}} , { 'pretty' => 1, 'utf8' => 1 } );
if ( open ( FILE , '>' , $self->{cache_files} ) ) {
  print FILE $json;
  close FILE;
  }
print "[ok]\n";
return $self;
}
#-------------------------------------------------------------------------------------------


sub MakeCache {
my $self = shift;
# Создаем кеш

my @Users = $self->Get ( sets => "Users", select => "Id,Domain,UserName,Sid,FullName" );
if ( scalar ($#Users) > 0 ) {
  delete $self->{cache}->{users};
  print "CheckCache Loading Users ...";
  foreach my $User ( @Users ) {
    my $Id = $User->{Id};
    $self->{cache}->{users}{$Id}->{Domain}   = $User->{Domain};
    $self->{cache}->{users}{$Id}->{UserName} = $User->{UserName};
    $self->{cache}->{users}{$Id}->{Sid}      = $User->{Sid};
    $self->{cache}->{users}{$Id}->{FullName} = $User->{FullName};
    }
  print "[ok]\n";
  }

my @Machines = $self->Get ( sets => "Machines", select => "Id,OSType,Sid,IPAddress,HostedMachineName,DesktopGroupId,CatalogId,HypervisorId,HostingServerName,FunctionalLevel,AgentVersion,AssociatedUserNames,AssociatedUserFullNames" );
if ( scalar ($#Machines) > 0 ) {
  delete $self->{cache}->{machines};
  print "CheckCache Loading Machines ...";
  foreach my $Machine ( @Machines ) {
    my $Id = $Machine->{Id};
    $self->{cache}->{machines}{$Id}->{HostedMachineName}       = $Machine->{HostedMachineName};
    $self->{cache}->{machines}{$Id}->{OSType}                  = $Machine->{OSType};
    $self->{cache}->{machines}{$Id}->{IPAddress}               = $Machine->{IPAddress};
    $self->{cache}->{machines}{$Id}->{Sid}                     = $Machine->{Sid};
    $self->{cache}->{machines}{$Id}->{DesktopGroupId}          = $Machine->{DesktopGroupId};
    $self->{cache}->{machines}{$Id}->{CatalogId}               = $Machine->{CatalogId};
    $self->{cache}->{machines}{$Id}->{HypervisorId}            = $Machine->{HypervisorId};
    $self->{cache}->{machines}{$Id}->{HostingServerName}       = $Machine->{HostingServerName};
    $self->{cache}->{machines}{$Id}->{FunctionalLevel}         = $Machine->{FunctionalLevel};
    $self->{cache}->{machines}{$Id}->{AgentVersion}            = $Machine->{AgentVersion};
    $self->{cache}->{machines}{$Id}->{AssociatedUserNames}     = $Machine->{AssociatedUserNames};
    $self->{cache}->{machines}{$Id}->{AssociatedUserFullNames} = $Machine->{AssociatedUserFullNames};
    }
  print "[ok]\n";
  }

my @Applications = $self->Get ( sets => "Applications", select => "Id,Name,PublishedName,BrowserName,Path,AdminFolder" );
if ( scalar ($#Applications) > 0 ) {
  delete $self->{cache}->{applications};
  print "CheckCache Loading Applications ...";
  foreach my $Application ( @Applications ) {
    my $Id = $Application->{Id};
    $self->{cache}->{app}{$Id}->{Name}          = $Application->{Name};
    $self->{cache}->{app}{$Id}->{PublishedName} = $Application->{PublishedName};
    $self->{cache}->{app}{$Id}->{BrowserName}   = $Application->{BrowserName};
    $self->{cache}->{app}{$Id}->{Path}          = $Application->{Path};
    $self->{cache}->{app}{$Id}->{AdminFolder}   = $Application->{AdminFolder};
    }
  print "[ok]\n";
  }

my @DesktopGroups = $self->Get ( sets => "DesktopGroups", select => "Id,DeliveryType,SessionSupport,CreatedDate,ModifiedDate,DesktopKind,Name,LifecycleState" );
if ( scalar ($#DesktopGroups) > 0 ) {
  delete $self->{cache}->{dg};
  print "CheckCache Loading DesktopGroup (DeliveryGroup) ...";
  foreach my $DesktopGroup ( @DesktopGroups ) {
    my $Id = $DesktopGroup->{Id};
    $self->{cache}->{dg}{$Id}->{Name}           = $DesktopGroup->{Name};
    $self->{cache}->{dg}{$Id}->{DeliveryType}   = $DesktopGroup->{DeliveryType};
    $self->{cache}->{dg}{$Id}->{SessionSupport} = $DesktopGroup->{SessionSupport};
    $self->{cache}->{dg}{$Id}->{CreatedDate}    = $DesktopGroup->{CreatedDate};
    $self->{cache}->{dg}{$Id}->{ModifiedDate}   = $DesktopGroup->{ModifiedDate};
    $self->{cache}->{dg}{$Id}->{DesktopKind}    = $DesktopGroup->{DesktopKind};
    $self->{cache}->{dg}{$Id}->{LifecycleState} = $DesktopGroup->{LifecycleState};
    }
  print "[ok]\n";
  }


my $enum = $self->do( folder => "Methods", sets => "GetAllMonitoringEnums" );
if ( defined $enum and ref($enum) eq 'ARRAY' ) {
  print "CheckCache Loading MonitoringEnums ...";
  delete $self->{cache}->{enum};
  foreach my $item ( @$enum ) {
    my $TypeName = $item->{TypeName};
    if ( defined $TypeName ) {
      my $Values = $self->do( folder => "Methods", sets => "GetAllMonitoringEnums(\'$TypeName\')/Values" );
      if ( defined $Values and ref($Values) eq 'ARRAY' ) {
        foreach my $element ( @$Values ) {
          my $Name  = $element->{Name};
          my $Value = $element->{Value};
          if ( defined $Name and defined $Value ) {
            $self->{cache}->{enum}{$TypeName}{$Value}=$Name;
            }
          }
        }
      }
    }
  print "[ok]\n";
  }

return $self;
}
#-------------------------------------------------------------------------------------------


sub ShowCache {
my $self = shift;

print "Users         " . scalar ( keys ( $self->{cache}->{users} ))."\n";
print "Machines      " . scalar ( keys ( $self->{cache}->{machines} ))."\n";
print "Applications  " . scalar ( keys ( $self->{cache}->{app} ))."\n";
print "DeliveryGroup " . scalar ( keys ( $self->{cache}->{dg} ))."\n";
print "Enums         " . scalar ( keys ( $self->{cache}->{enum} ))."\n";

return $self;
}
#-------------------------------------------------------------------------------------------


1;
