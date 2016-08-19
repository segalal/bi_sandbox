#!/usr/bin/perl
######################################################################
# Script :      build_nested_set.ksh
#
# Modifications :
# 1.0  9/21/2012 nparks: Created
#      9/24/2013 asegal: standardization, error messaging, etc.
#
#
#######################################################################


use strict;
use Getopt::Long qw();
use REI::BI::HIERARCHY::NestedSetNode;
use REI::BI::DB;
use POSIX;

$|=1;

########################################################

my $debug = 0;
my $xmlfile = "taxcat";
my $parserModule = "REI::BI::HIERARCHY::TaxCatXmlParser";
my $delim = "\t";
my $srcjobid = 0;
my $etljobid = 0;
my $sourceDataKey = 0;
### DB ROLE GLOBALS
my $CEM_RDW = "CEM_RDW";
my $CEM_HIST = "CEM_HIST";
my $ETL_ADM = "ETL_ADM";

my $rc = Getopt::Long::GetOptions(
  "-debug"         => \$debug,
  "-xmlfile=s"     => \$xmlfile,
  "-parser=s"      => \$parserModule,
  "-delim=s"       => \$delim,
  "-srcjobid=s"    => \$srcjobid,
  "-etljobid=s"    => \$etljobid,
)
;

my $InsertDateTime = POSIX::strftime("%Y-%m-%d %H:%M:%S", localtime());
my $file_date = POSIX::strftime("%Y%m%d", localtime());

my $logfilename = "/workspace/dstage/data/3CPDATA/ongoing/scripts/log/build_nested_set." . $file_date . ".log";
open logfile, ">>" . $logfilename;

print logfile "======================================================================================================\n";

print logfile "0001 Starting build_nested_set.pl for job_run_id=$srcjobid\n";


if (!$rc) {
  print STDERR "0005 Invalid option\n";
  exit 1;
} elsif (scalar @ARGV > 0) {
  print STDERR "0007 Too many arguments: @ARGV\n";
  exit 1;
}

if ($srcjobid == 0) {
  print STDERR "0009 ERROR: SRC Job ID required\n ";
  exit 1;
}
if ($etljobid == 0) {
  print STDERR "0011 ERROR: ETL Job ID required\n ";
  exit 1;
}


my $db = REI::BI::DB->new($ETL_ADM);
my $query = "SELECT SOURCE_DATA_KEY FROM SOURCE_DATA WHERE SOURCE_NAME = '" . uc($xmlfile) . ".xml'";
my $sth = $db->Prepare($query);
my($rc, @results) = $db->Execute($sth);
if ($rc) {
	$sourceDataKey = @results[0]->{SOURCE_DATA_KEY};
}
else {
  print logfile "0015 ERROR:  getting max SOURCE_DATA_KEY\n";
}


#######################################################
# get the EFFECTIVE START DATETIME - THE TIME THE SRC FILE WAS LOADED in the JOB_RUN_EFFECTIVITY table

print logfile "0020 Get the EFFECTIVE START DATETIME - THE TIME THE SRC FILE WAS LOADED in the JOB_RUN_EFFECTIVITY table for job_run_id=$srcjobid\n"; 
my $sql = "SELECT EFFECTIVE_DATETIME FROM METAMART..JOB_RUN_EFFECTIVITY WHERE JOB_RUN_ID = " . $srcjobid . " AND SOURCE_DATA_KEY = ".$sourceDataKey.";";
my $sth = $db->Prepare($sql);
my($rc, @results) = $db->Execute($sth);
my $effstartdatetime = 0;
if ($rc) {
   if ($#results != -1) {
      $effstartdatetime = @results[0]->{EFFECTIVE_DATETIME};
   }
}
else {
  print logfile "0022 Error retrieving Effective Start Datetime from JOB table for ".$xmlfile . " for job_run_id=$srcjobid\n";
}

print logfile "0030 Retrieve historical data for job_run_id=$srcjobid\n";

my $role = $CEM_HIST;
my $db = REI::BI::DB->new($role);
my $buf = "";

my $getTbl = "SELECT h.RAW_COL1 FROM  SRC_CEM_HIERARCHY H WHERE H.SOURCE_DATA_KEY='".$sourceDataKey."' ";
$getTbl = $getTbl . "AND JOB_RUN_ID = " . $srcjobid . "  ORDER BY h.LINE_NUMBER";


#my $sth = $db->Prepare($SourceString);
my $sth = $db->Prepare($getTbl);

my($rc, @rows) = $db->Execute($sth);
if ($rc) {
    foreach my $line (@rows) {
      $buf .= $line->{RAW_COL1} . "\n";
    }
    print logfile "0040 Loaded " . (scalar @rows) . " rows from SRC_CEM_HIERARCHY for job_run_id=$srcjobid\n";
}
else {
  print logfile "0043 Error: reading " . $xmlfile . " from database for job_run_id=$srcjobid\n";
  exit 1;
}

# Glue split data back together
$buf =~ s/\\\n//g;

########################################################
# Create the parser

print logfile "0050 Create the parser\n";

my $parser = undef;
eval "use $parserModule; \$parser = new $parserModule();";

########################################################
# Parse the input file into a tree
my ($status,$root) = $parser->convertXmlToTree($buf);
if ($status == 1) {
   print logfile "0053 Error parsing the ". $xmlfile . " xml ... exiting during job_run_id=$srcjobid\n";
   exit 1;
}

########################################################
# Convert the tree into a nested set

print logfile "0055 Convert the tree into a nested set for job_run_id=$srcjobid\n";

my %reiHierarchySet = convertTreeToNestedSet($root);

########################################################
# Get the max HIERARCHY_DESCIPTION_KEY
print logfile "0060 Get Max Description Key for job_run_id=$srcjobid.  Connect using ".$CEM_RDW . "\n";
my $role = $CEM_RDW;
my $db = REI::BI::DB->new($role);

my $lastDescriptionKey = 0;
my $sth = $db->Prepare("select ISNULL(MAX(HIERARCHY_DESCRIPTION_KEY),0) MAX_DESCRIPTION_KEY FROM REI_HIERARCHY_DESCRIPTIONS;");
my($rc, @results) = $db->Execute($sth);
if ($rc) {
	$lastDescriptionKey = @results[0]->{MAX_DESCRIPTION_KEY}+1;
}
else {
  print logfile "0063 Error: Getting max Hierarchy Description key for job_run_id=$srcjobid\n"; 
}
########################################################
# Get the max HIERARCHY_KEY  from REI_HIERARCHY

print logfile "0067 Get Max Hierarchy Key for job_run_id=$srcjobid\n";

my $nextReiHierarchyKey = 0;
$sth = $db->Prepare("select ISNULL(MAX(REI_HIERARCHY_KEY),0) MAX_HIERARCHY_KEY FROM REI_HIERARCHY;");
($rc, @results) = $db->Execute($sth);
if ($rc) {
	$nextReiHierarchyKey = @results[0]->{MAX_HIERARCHY_KEY}+1;
}
else {
  print logfile "0070 Error:  getting max Hierarchy Description key for job_run_id=$srcjobid\n"; 
}


#######################################################
# populate reiHierarchyDescriptions from the database
print logfile "0072 Populate reiHierarchyDescriptions from the database for job_run_id=$srcjobid\n";
 
my %reiHierarchyDescriptions = ();
my $sth = $db->Prepare("select HIERARCHY_DESCRIPTION_KEY,HIERARCHY_DESCRIPTION,HIERARCHY_ALTERNATE_DESCRIPTION from REI_HIERARCHY_DESCRIPTIONS");
my($rc, @results) = $db->Execute($sth);
if (!$rc) {
  print logfile "0075 Report error for job_run_id=$srcjobid";
}
else {
        foreach my $descr (@results)  {
                $reiHierarchyDescriptions{$descr->{HIERARCHY_DESCRIPTION}}{key} = $descr->{HIERARCHY_DESCRIPTION_KEY};
                $reiHierarchyDescriptions{$descr->{HIERARCHY_DESCRIPTION}}{short} = $descr->{HIERARCHY_DESCRIPTION};
        }
}

#######################################################
# update the TreeNodes description key from the existing description table .
print logfile "0078 Update the TreeNodes description key from the existing description table for job_run_id=$srcjobid\n";

my $nextDescriptionKey = $lastDescriptionKey;
foreach my $key (sort keys %reiHierarchySet) {
	my $node = $reiHierarchySet{$key};
	$node->SetAttribute('REI_HIERARCHY_KEY', $nextReiHierarchyKey++);
	my $description = $node->ID();
	if (!exists $reiHierarchyDescriptions{$description}) {
		$reiHierarchyDescriptions{$description}{key} = $nextDescriptionKey++;
                $reiHierarchyDescriptions{$description}{alternate} = $node->Title();

	}
	$node->SetAttribute('REI_HIERARCHY_DESCRIPTION_KEY', $reiHierarchyDescriptions{$description}{key});
}


########################################################
# Update REI_HIERARCHY_DESCRIPTIONS
print logfile "0081 Update REI_HIERARCHY_DESCRIPTIONS for job_run_id=$srcjobid\n";

# use role attributes to set NZ auth info 
  $ENV{'NZ_USER'} = $db->Username();
  $ENV{'NZ_PASSWORD'} = $db->Password();
  $ENV{'NZ_DATABASE'} = $db->DBName();

my $InsertDateTime = POSIX::strftime("%Y-%m-%d %H:%M:%S", localtime());
# Create the information needed for nzload
my $nzload = q#| nzload -db '#.$db->DBName.q#' -t 'REI_HIERARCHY_DESCRIPTIONS' -crInString -ctrlChars -delim '\t' -fillRecord -maxErrors 30 -lf 'log/NZLOAD_REI_HIERARCHY_DESCRIPTIONS.log' -bf 'log/NZLOAD_REI_HIERARCHY_DESCRIPTIONS.bad'#;
open(NZLOAD, $nzload) || die;

# insert new descriptions
foreach my $description (sort keys %reiHierarchyDescriptions) {
	my $descriptionKey = $reiHierarchyDescriptions{$description}{key};
	my $alternateDescription = $reiHierarchyDescriptions{$description}{alternate};
        if ($descriptionKey >= $lastDescriptionKey) {
           print NZLOAD join($delim, $descriptionKey, $description,$alternateDescription, $effstartdatetime, $etljobid, $InsertDateTime,  $InsertDateTime) . "\n";
        }
}
close(NZLOAD);


# get the max keys (for the table and the specific hierarchy

print logfile "0083 Get the max keys (for the table and the specific hierarchy for job_run_id=$srcjobid\n";

my $sql = "SELECT (SELECT MAX(HIERARCHY_VERSION_KEY)  FROM REI_HIERARCHY_VERSIONS) NEXTVERSIONRECORD, ";
   $sql .= "( select count(*)+1 FROM REI_HIERARCHY_VERSIONS WHERE HIERARCHY_SOURCE = '" . uc($xmlfile) . ".xml')  THISHIERVERSIONNUM";
my $sth = $db->Prepare($sql);
my($rc, @results) = $db->Execute($sth);
my $nextversionkey = 0;
my $thishierarchyversionkey = 0; 
if ($rc) {
   if ($#results != -1) {
      $nextversionkey = @results[0]->{NEXTVERSIONRECORD};
      $thishierarchyversionkey = @results[0]->{THISHIERVERSIONNUM};
   }
}
else {
  print logfile "0085 Error retrieving the max version key for  ".$xmlfile . " for job_run_id=$srcjobid\n";
}

# insert a new version
$sql = q|INSERT INTO REI_HIERARCHY_VERSIONS (HIERARCHY_VERSION_KEY 
       , HIERARCHY_VERSION_DESCRIPTION
       , HIERARCHY_VERSION_NUMBER
       , HIERARCHY_SOURCE
       , EFF_START_DATETIME
       , JOB_RUN_ID
) SELECT |;
$sql .=  $nextversionkey+1;
$sql .=  q|,'| . uc($xmlfile) . ".xml'";
$sql .=  q|,(select count(*)+1 FROM REI_HIERARCHY_VERSIONS WHERE HIERARCHY_SOURCE = '| . uc($xmlfile) . q|.xml')|;
$sql .=  q|,'| .uc($xmlfile) . q|.xml','|. $effstartdatetime . "'," . $etljobid;

print logfile "0089 File $xmlfile and source job_run_id=$srcjobid and current job_run_id=$etljobid is currently being processed\n";

my $sth = $db->Prepare($sql);
my($rc, @results) = $db->Execute($sth);

if ($rc) {
  print logfile "0087 MAX Version Key inserted for  " . $xmlfile . " = " . $nextversionkey+1 . "\n";
} else { 
  print logfile "0088 Error inserting the max version key for  ".$xmlfile . " for job_run_id=$srcjobid\n";
}


print logfile "0090 Insert the REI_HIERARCHY Records" . " for job_run_id=$srcjobid\n";

$nzload = q#| nzload -db '#.$db->DBName.q#' -t 'REI_HIERARCHY' -crInString -ctrlChars -delim '\t'  -fillRecord -maxErrors 30 -lf 'log/NZLOAD_REI_HIERARCHY.log' -bf 'log/NZLOAD_REI_HIERARCHY.bad'#;
open(NZLOAD, $nzload) || die;
foreach my $key (sort keys %reiHierarchySet) {
	my $node = $reiHierarchySet{$key};
        if ($node->Attribute('REI_HIERARCHY_KEY') > 0){
	    print NZLOAD join($delim
                  , $node->Attribute('REI_HIERARCHY_KEY')
                  , $nextversionkey+1
                  , $node->Attribute('REI_HIERARCHY_DESCRIPTION_KEY')
                  , $node->NodeID(),
                  , $node->Parent(),
                  , $node->Left()
                  , $node->Right()
                  , $effstartdatetime
                  , $etljobid
                  , $node->Level()
                  , $InsertDateTime
                  , $InsertDateTime) . "\n";
        }
}
close(NZLOAD);

print logfile "9999 Processing completed successfully for build_snp_mapping.pl for job_run_id=$srcjobid\n";

print logfile "======================================================================================================\n";


exit 0;

########################################################
########################################################
sub convertTreeToNestedSet {
	my($root) = @_;
  my %set = ();
  my $level = 0;
  my $node_id = 0;
  processTreeNode($root, 0, 0, \%set, "",$level, \$node_id, -2);

  return %set;
}

sub processTreeNode {
	my($node, $indent, $index, $set, $prefix, $level, $node_id, $parentNode) = @_;
	$index++;
  $level++; 
  my $indentStr = " " x $indent;
  my $name = $node->ID();
  $$node_id++;
  my $setNode = new REI::BI::HIERARCHY::NestedSetNode($name);

  $setNode->SetTitle($node->Title());
  $setNode->SetLeft($index);
  $setNode->SetLevel($level);
  $setNode->SetNodeID($$node_id);
  $setNode->SetParent($parentNode);
  print $indentStr . $setNode->ID() . " [" . $setNode->Title() . "] left=" . $setNode->Left() . "\n" if ($debug);

  my $mykey = "$prefix/$name";
  my @children = $node->Children();
    $parentNode = $$node_id;
  foreach my $child (@children) {
    $index = processTreeNode($child, $indent+2, $index, $set, $mykey, $level, $node_id, $parentNode);
  }

  $index++;
  $setNode->SetRight($index);
  print $indentStr . "===> " . $setNode->ID() . " [" . $setNode->Title() . "] right=" . $setNode->Right() . "\n" if ($debug);
  $set->{$mykey} = $setNode;
  return $index;
}


1;
