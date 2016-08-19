#!/usr/bin/perl

#########################################################
# MODULE      : load_src_web_service.pl
# DESCRIPTION : Loads web service via stdout to SRC table
# Created     : 9/19/2012
# By          : nparks@rei.com
# Modification: asegal 9/23/2013 standardization, comments
########################################################

use strict;
use Getopt::Long qw();
use REI::BI::HIERARCHY::SnpNode;
use REI::BI::DB;
use REI::BI::DB::Role;
use POSIX;
$|=1;

########################################################

my $debug = 0;
my $xmlfile = "reisnpinput";
my $parserModule = "REI::BI::HIERARCHY::ReiSnpInputParser";
my $srcjobid = 0;
my $etljobid = 0;
my $delim = "\t";
my $suffix = "dat";
my $srctable = "SRC_CEM_HIERARCHY";
my $CEM_HIST = "CEM_HIST";
my $CEM_STG = "CEM_STG";

my $processing_datetime = localtime;

my $rc = Getopt::Long::GetOptions(
  "-debug"         => \$debug,
  "-xmlfile=s"     => \$xmlfile,
  "-parser=s"      => \$parserModule,
  "-delim=s"       => \$delim,
  "-srcjobid=s"       => \$srcjobid,
  "-etljobid=s"       => \$etljobid,
  "-suffix=s"      => \$suffix,
  "-srctable=s"    => \$srctable
);

my $InsertDateTime = POSIX::strftime("%Y-%m-%d %H:%M:%S", localtime());
my $file_date = POSIX::strftime("%Y%m%d", localtime());

my $logfilename = "/workspace/dstage/data/3CPDATA/ongoing/scripts/log/build_snp_mapping." . $file_date . ".log";
open logfile, ">>" . $logfilename;

print logfile "======================================================================================================\n";

print logfile "0001 Starting build_snp_mapping.pl\n";

print logfile "0003 The processing and insert datetime is " . $InsertDateTime . "\n\n";

if (!$rc) {
  print logfile "0006 Invalid option\n";
  exit 1;
} elsif (scalar @ARGV > 0) {
  print logfile "0007 Too many arguments: @ARGV\n";
  exit 1;
}

if ($srcjobid == 0) {
  print logfile "0010 ERROR:  Job ID required\n ";
  exit 1;
}

# Create the parser
my $parser = undef;
eval "use $parserModule; \$parser = new $parserModule();";

print logfile "0011 Parser created for source job_run_id=$srcjobid and file $xmlfile\n";

########################################################
# read the SNP data from SRC_CEM_HIERARCHY

my $role = $CEM_HIST;
my $db = REI::BI::DB->new($role);
my $buf = "";

my $getTbl = "SELECT h.RAW_COL1 FROM  SRC_CEM_HIERARCHY H JOIN METAMART..SOURCE_DATA SD ON SD.SOURCE_DATA_KEY = H.SOURCE_DATA_KEY ";
$getTbl = $getTbl . "WHERE SD.SOURCE_NAME = '" . uc($xmlfile) . ".xml'";
$getTbl = $getTbl . "AND JOB_RUN_ID = " . $srcjobid . " ORDER BY h.LINE_NUMBER";

my $sth = $db->Prepare($getTbl);

my($rc, @rows) = $db->Execute($sth);
if ($rc) {
      # $buf = join("\n", map($_{'RAW_COL1'}, @rows));
    foreach my $line (@rows) {
      $buf = $buf . $line->{RAW_COL1}
    }
}
else {
  print logfile "0020 ERROR:  reading SnP data for job_run_id=$srcjobid\n";
}


########################################################
# Parse the input file into an array

print logfile "0021 Parse the input file into an array for job_run_id=$srcjobid and xml file $xmlfile\n";

my ($status, @nodes) = $parser->convertXmlToTree($buf);

if ($status == 1) {
   print logfile "0023 ERROR: Parsing into an array failed for the input file for job_run_id=$srcjobid file $xmlfile\n";
   exit 1;
}

########################################################
# Output the nested set to STG tables for use in KSH ETL to populate STYLE_HIERARCHY

print logfile "0024 Output the nested set to STG tables for use in KSH ETL to populate STYLE_HIERARCHY for job_run_id=$srcjobid\n";

# use role attributes to set NZ auth info 
  $role = $CEM_STG;
  my $dbrole = REI::BI::DB::Role->new($role);
  $ENV{'NZ_USER'} = $dbrole->Username();
  $ENV{'NZ_PASSWORD'} = $dbrole->Password();
  $ENV{'NZ_DATABASE'} = $dbrole->DBName();

print logfile "0030 nzload CEM_SNP_TAXONOMY for job_run_id=$srcjobid\n"; 
my $nzload = q#| nzload -db '# . $dbrole->DBName() . q#' -t 'CEM_SNP_TAXONOMY' -crInString -ctrlChars -delim '\t'  -fillRecord -maxErrors 30 -lf 'log/NZLOAD_CEM_SNP_TAXONOMY.log' -bf 'log/NZLOAD_CEM_SNP_TAXONOMY.bad'#;
print $nzload;
open(NZLOAD, $nzload) || die;

foreach my $node (@nodes) {
#	print NZLOAD join($delim, $node->ID(), $node->Taxonomy(), $node->Brand(), $srcjobid, $etljobid, $InsertDateTime, $InsertDateTime) . "\n";
	print NZLOAD join($delim, $node->ID(), $node->Taxonomy(), $node->Brand(), $srcjobid, $InsertDateTime, $InsertDateTime) . "\n";
}
close(NZLOAD);

print logfile "0035 nzload CEM_SNP_TAXONOMY completed for job_run_id=$srcjobid\n\n";


print logfile "0040 nzload CEM_SNP_CATEGORY for job_run_id=$srcjobid\n";
my $nzload = q#| nzload -db '# . $dbrole->DBName() . q#' -t 'CEM_SNP_CATEGORY' -crInString -ctrlChars -delim '\t'  -fillRecord -maxErrors 30 -lf 'log/NZLOAD_CEM_SNP_CATEGORY.log' -bf 'log/NZLOAD_CEM_SNP_CATEGORY.bad'#;
open(NZLOAD, $nzload) || die;

foreach my $node (@nodes) {
    foreach my $cat ($node->Categories()) {
       #print NZLOAD join($delim, $node->ID(), $cat, $srcjobid, $etljobid, $InsertDateTime, $InsertDateTime ) . "\n";
       print NZLOAD join($delim, $node->ID(), $cat, $srcjobid, $InsertDateTime, $InsertDateTime ) . "\n";
    }
}
close(NZLOAD);

print logfile "0045 nzload CEM_SNP_CATEGORY completed for job_run_id=$srcjobid\n\n";

print logfile "9999 Processing completed successfully for build_snp_mapping.pl\n";

print logfile "======================================================================================================\n";

exit 0;

########################################################
########################################################
1;
