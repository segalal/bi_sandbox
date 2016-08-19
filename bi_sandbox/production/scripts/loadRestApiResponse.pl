#!/usr/bin/perl
#########################################################
# MODULE      : load_src_restapi.pl
# DESCRIPTION : Calls Rest API and loads into the DI_HISTORY
# Created     : 11/4/2015
# By          : pnair@rei.com
########################################################

########################################################
#
# MODIFICATION HISTORY:
#  Date       Name              Mods Description
#  ---------- ----------------- ---- -------------------------------------
#  02/17/2015 Christeena        1)   Added parameters user, pwd for username/password 
#                                    authentication
#                                    
#
#
########################################################

use strict;
use LWP::UserAgent;
use POSIX;
use REI::BI::DB;
use DBI;
use Digest::MD5  qw(md5 md5_hex md5_base64);
use Getopt::Long qw(GetOptions);


my $RUN_JOB_DB = 'ELT_HIST';
my $splitlength = 30000;
my $url = "";
my $srctable = "";
my $jobrunid = 0;
my $urlquery = "";
my $response = "";
my $request  = "";
my @reqhdr;
my @reqhdrval;
my $chksumflg = 0;
my $effdate   = "";
my $prevjobrunid = "";
my $srcdatakey = "";
my $response_out = "";
my $self="";
my $username = "";
my $password = "";

Getopt::Long::Configure("no_ignore_case", "prefix_pattern=(--|-|\/)");
my $result = GetOptions(
     "job=i" => \$jobrunid,
     "url=s" => \$url,
     "qry=s" => \$urlquery,
     "tbl=s" => \$srctable, 
     "rhdr=s{,}"=> \@reqhdr, 
     "rval=s{,}"=> \@reqhdrval,
     "day=s"=> \$effdate,
     "chk=s"  => \$chksumflg, 
     "pjob=s"=> \$prevjobrunid,
     "sdk=s" => \$srcdatakey,
     "user=s" => \$username, 
     "pwd=s" => \$password,  
 );

my $insert_di_hist_query = "INSERT INTO $srctable (		RAW_COL1, 
                                                                LINE_NUMBER, 
                                                                TRANSMIT_DATETIME, 
                                                                SOURCE_DATA_KEY,
                                                                JOB_RUN_ID) 
                                                         VALUES(?,
                                                                ?,
                                                                ?,
                                                               ?,
                                                               ?)";

my $select_prevday_message = "SELECT RAW_COL1 FROM $srctable WHERE JOB_RUN_ID = ? order by line_number";

if (!$result) {
    print STDERR "Invalid option\n";
    exit 1;
}

if ($jobrunid eq "" or $srctable eq "" or $url eq "") {
    print STDERR "Usage = load_src_api_response.pl -srctable=tablename -jobid=jobnum -extractprocess=sourcedataprocessname -url=url\n";
    exit 1;
}

my $ua = LWP::UserAgent -> new;
my $server_endpoint = $url.$urlquery;
print "\nserver endpoint: $server_endpoint\n";
$request = HTTP::Request -> new(GET => $server_endpoint);

if ($#reqhdr!= $#reqhdrval) {
    print STDERR "Request Header Parameter Array and Value Array Mismatch\n";
    exit 1;
}

## IF - ELSE block for two types of authentication
if ($#reqhdr > 0) {
for (0..$#reqhdr) {
    my $header_name = $reqhdr[$_];
    my $header_value = $reqhdrval[$_];
    $request -> header($header_name  => $header_value);
}
} else {
	$request -> authorization_basic($username, $password);
}

#set custom HTTP request header fields
$response = $ua -> request($request);
if ($response -> is_success) {
    $response_out = $response -> decoded_content;
} else {
    print "HTTP GET error code: ", $response -> code, "\n";
    print "HTTP GET error message: ", $response -> message, "\n";
    return 1;
}

if ($chksumflg eq 0) {
my ($rc) = &DiHistLoad();
if (!$result) {
   print STDERR "DI_HISTORY Load Failed\n";
    exit 1;
}
}
else {
if (!ref($self)) {
    $self = REI::BI::DB -> new($RUN_JOB_DB, REI::BI::DB::READWRITE);
}

my($rc,@rawcol) = $self -> PrepareAndExecute($select_prevday_message, $prevjobrunid);
my $scalar = scalar @rawcol;
print "num array = $scalar\n";
my @message=();

foreach my $rawcol (@rawcol) {
  my $prevmessage=$$rawcol{'RAW_COL1'};
  push(@message,$prevmessage);
}
     my $curr_digest = md5_hex($response_out);
     my $prev_digest = md5_hex(@message);
     if ($curr_digest ne $prev_digest) {
     my ($rc) = &DiHistLoad();
     if (!$rc) {
               print STDERR "DI_HISTORY Load Failed\n";
               exit 1;
     }
    }

}

exit 0;

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++
# SUB     : DiHistLoad 
# PURPOSE : Substrings the response at the value specified
#           in $splitlength
# RETURNS : Returns 1 if successful.
# USAGE   : 
#+++++++++++++++++++++++++++++++++++++++++++++++++++++++

sub DiHistLoad {
if (!ref($self)) {
    $self = REI::BI::DB -> new($RUN_JOB_DB, REI::BI::DB::READWRITE);
}
my $line_number = 1;
print $insert_di_hist_query;
my @message_parts = ($response_out =~m/(.{1,$splitlength})/gs);
for my $i (0..$#message_parts) {
    my($rc) = $self -> PrepareAndExecute($insert_di_hist_query, $message_parts[$i],$line_number,$effdate,$srcdatakey,$jobrunid);

    if (!$result) {
              print STDERR "Invalid option\n";
               return 0;
             }
   $line_number++;
    }
return 1;
}

