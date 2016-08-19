#!/usr/bin/perl -w

########################################################
#
# Recreational Equipment Incorporated
#
# COPYRIGHT (c) 2012 by Recreational Equipment Incorporated
#
# This software is furnished under a  license and may be used and copied  only
# in accordance with the terms of  such license and with the inclusion of  the
# above copyright notice.  This  software or any other copies thereof  may not
# be provided or  otherwise made available to  any other person.   No title to
# and ownership of the software is hereby transferred.
#
########################################################
#
# MODULE      : gc_aim_parse.pl
# DESCRIPTION : Code to parse the json AIM files
#
# ABSTRACT:
#
# NOTES:
#
# SYNOPSIS:
#
########################################################
#
# MODIFICATION HISTORY:
#  Date       Name              Mods Description
#  ---------- ----------------- ---- -------------------------------------
#  09/01/2012 Devi Kamjula           Original Draft
#  10/04/2012 Mike Green       	A    Initial code completion
#  10/13/2012 Anna Segal        B/F  Removed references to MG in log names and standardized the nzload log names  
#                                    Altered column name STATE to STATE_CODE  
#
# MODIFICATION LEGEND:
#   B = Bugfixes
#   A = Architectural change
#   F = Feature addition
#   R = Code re-write
#   C = Comment update
#
########################################################

use strict;
use REI::BI::DB;
use Data::Dumper;
use JSON ();
use POSIX 'strftime';

my $json = JSON->new->allow_nonref;

# Debug constants
my $debug = 0;
my $runNz = 1;

# Database constants
my $SCRIPTS_DIR = "/workspace/dstage/data/3CPDATA/ongoing/scripts";
my $HISTDB = $ENV{'NZ_HIST_DB'};
my $STGDB  = $ENV{'NZ_STGRDW_DB'};
my $METADB = $ENV{'NZ_METAMART_DB'};
my $SOURCENAME = "REIGCREC";
my $PROCESSING_DATETIME =  strftime( "%Y-%m-%d %H:%M:%S",localtime);

# Input parameter
my $curjobrunid = shift;
print "curjobrunid=$curjobrunid" if ($debug);

######################################################
# Get new data from SRC table                        #
######################################################

my $SOURCING_QUERY = 
"
SELECT		SRC.RAW_COL1
FROM		$HISTDB..SRC_GC_ORDER_RECON SRC
JOIN
(

		SELECT	SOURCE_DATA_KEY
		FROM	$METADB..SOURCE_DATA
		WHERE	SOURCE_NAME = '$SOURCENAME'

) 			SOURCEDATA ON SRC.SOURCE_DATA_KEY = SOURCEDATA.SOURCE_DATA_KEY
WHERE		JOB_RUN_ID = $curjobrunid
ORDER BY	LINE_NUMBER
";

my $jsonString = `
. $SCRIPTS_DIR/load_library.ksh
run_query -d $HISTDB -q "$SOURCING_QUERY" -m "Sourcing query failed"
`;

# Glues split data back together
$jsonString =~ s/\\\n//g; 

######################################################
# Creates PARSED tables                              #
######################################################

my $WRK_ORDER_TABLE_NAME = 'WRK_AIM_ORD_PARSED';
my $WRK_CARD_TABLE_NAME = 'WRK_AIM_CARD_PARSED';
my $WRK_CLSP_TABLE_NAME = 'WRK_AIM_CLSP_PARSED';


my $WRK_ORD_TABLE_CREATION = "
CREATE TABLE $WRK_ORDER_TABLE_NAME
(
	CLIENT_ORDER_ID CHARACTER VARYING(20),
	VENDOR_ORDER_ID CHARACTER VARYING(20),
	VENDOR_CLIENT_ID CHARACTER VARYING(20),
	ORDER_DATE CHARACTER VARYING(60),
	SHIP_DATE CHARACTER VARYING(60),
	SHIP_TYPE CHARACTER VARYING(20),
	ORDER_TYPE CHARACTER VARYING(20),
	ORDER_AMOUNT CHARACTER VARYING(20),
	DISCOUNT CHARACTER VARYING(20),
	TRACKING_NUMBER CHARACTER VARYING(50),
	SHIPPING_AMOUNT CHARACTER VARYING(20),
	HANDLING_FEE CHARACTER VARYING(20),
	PERSONALIZATION_FEE CHARACTER VARYING(20),
	BILLTO_NAMEPREFIX CHARACTER VARYING(20),
	BILLTO_FIRSTNAME CHARACTER VARYING(362),
	BILLTO_LASTNAME CHARACTER VARYING(362),
	BILLTO_ADDRESS1 CHARACTER VARYING(362),
	BILLTO_ADDRESS2 CHARACTER VARYING(362),
	BILLTO_CITY CHARACTER VARYING(362),
	BILLTO_STATE_CODE CHARACTER VARYING(4),
	BILLTO_COUNTRY CHARACTER VARYING(50),
	BILLTO_ZIP CHARACTER VARYING(10),
	BILLTO_PHONENUMBER CHARACTER VARYING(362),
	BILLTO_PHONEEXT CHARACTER VARYING(10),
	BILLTO_EMAIL CHARACTER VARYING(256),
	BILLTO_COMPANYNAME CHARACTER VARYING(55),
	SHIPTO_NAMEPREFIX CHARACTER VARYING(20),
	SHIPTO_FIRSTNAME CHARACTER VARYING(362),
	SHIPTO_LASTNAME CHARACTER VARYING(362),
	SHIPTO_ADDRESS1 CHARACTER VARYING(362),
	SHIPTO_ADDRESS2 CHARACTER VARYING(362),
	SHIPTO_CITY CHARACTER VARYING(362),
	SHIPTO_STATE_CODE CHARACTER VARYING(4),
	SHIPTO_COUNTRY CHARACTER VARYING(50),
	SHIPTO_ZIP CHARACTER VARYING(10),
	SHIPTO_PHONENUMBER CHARACTER VARYING(362),
	SHIPTO_PHONEEXT CHARACTER VARYING(10),
	SHIPTO_EMAIL CHARACTER VARYING(256),
	SHIPTO_COMPANYNAME CHARACTER VARYING(55),
	BILLTO_EMAIL_KEY INTEGER,
	SHIPTO_EMAIL_KEY INTEGER,
	BILLTO_PHONE_KEY INTEGER,
	SHIPTO_PHONE_KEY INTEGER,
	BILLTO_PERSON_KEY INTEGER,
	SHIPTO_PERSON_KEY INTEGER,
	BILLTO_ADDRESS_KEY INTEGER,
	SHIPTO_ADDRESS_KEY INTEGER,
	BILLTO_CARD_PERSON_KEY INTEGER,
	SHIPTO_CARD_PERSON_KEY INTEGER,
	M_INSERT_DATETIME TIMESTAMP
)
DISTRIBUTE ON (CLIENT_ORDER_ID)
";

my $WRK_CARD_TABLE_CREATION = "
CREATE TABLE $WRK_CARD_TABLE_NAME
(
	CLIENT_ORDER_ID CHARACTER VARYING(20),
	VENDOR_ORDER_ID CHARACTER VARYING(20),
	VENDOR_CLIENT_ID CHARACTER VARYING(20),	
	CLIENT_CARD_ID CHARACTER VARYING(100),
	VENDOR_CARD_ID CHARACTER VARYING(50),
	AMOUNT CHARACTER VARYING(50),
	CARD_SKU CHARACTER VARYING(40),
	CARD_NUMBER CHARACTER VARYING(500),
	ACTIVATION_DATE CHARACTER VARYING(50),
	DIGITAL_DELIVERY_EMAIL CHARACTER VARYING(500),
	M_INSERT_DATETIME TIMESTAMP
)
DISTRIBUTE ON (CLIENT_ORDER_ID)
";

my $WRK_CLSP_TABLE_CREATION = "
CREATE TABLE $WRK_CLSP_TABLE_NAME
(
	CLIENT_ORDER_ID CHARACTER VARYING(20),
	SPECIFICATION_KEY CHARACTER VARYING(10),
	SPECIFICATION_VALUE CHARACTER VARYING(20),
	M_INSERT_DATETIME TIMESTAMP
)
DISTRIBUTE ON (CLIENT_ORDER_ID)
";

my $WRK_CHECK_QUERY = "SELECT 1 FROM _V_TABLE WHERE TABLENAME = ";
my $WRK_DROP_TABLE = "DROP TABLE ";

#CHECK IF WRK TABLES EXIST
my $WRK_ORDER_CHECK = `
. $SCRIPTS_DIR/load_library.ksh 
run_query -d $STGDB -q "$WRK_CHECK_QUERY '$WRK_ORDER_TABLE_NAME'" -m "$WRK_ORDER_TABLE_NAME check failed"
`;
my $WRK_CARD_CHECK = `
. $SCRIPTS_DIR/load_library.ksh 
run_query -d $STGDB -q "$WRK_CHECK_QUERY '$WRK_CARD_TABLE_NAME'" -m "$WRK_CARD_TABLE_NAME check failed"
`;
my $WRK_CLSP_CHECK = `
. $SCRIPTS_DIR/load_library.ksh 
run_query -d $STGDB -q "$WRK_CHECK_QUERY '$WRK_CLSP_TABLE_NAME'" -m "$WRK_CLSP_TABLE_NAME check failed"
`;
print "WRK_ORDER_CHECK=$WRK_ORDER_CHECK\nWRK_CARD_CHECK=$WRK_CARD_CHECK\nWRK_CLSP_CHECK=$WRK_CLSP_CHECK\n" if ($debug);

# DROP EXISTING WRK TABLES
`. $SCRIPTS_DIR/load_library.ksh
run_query -d $STGDB -q "$WRK_DROP_TABLE $WRK_ORDER_TABLE_NAME" -m "ORD WRK table creation failed"
` if ($WRK_CLSP_CHECK);

`. $SCRIPTS_DIR/load_library.ksh
run_query -d $STGDB -q "$WRK_DROP_TABLE $WRK_CARD_TABLE_NAME" -m "CARD WRK table creation failed"
` if ($WRK_CARD_CHECK);

`. $SCRIPTS_DIR/load_library.ksh
run_query -d $STGDB -q "$WRK_DROP_TABLE $WRK_CLSP_TABLE_NAME" -m "CLSP WRK table creation failed"
` if ($WRK_CLSP_CHECK);

# CREATE WRK TABLES
`
. $SCRIPTS_DIR/load_library.ksh
run_query -d $STGDB -q "$WRK_ORD_TABLE_CREATION" -m "ORDER WRK table creation failed"
`;

`. $SCRIPTS_DIR/load_library.ksh
run_query -d $STGDB -q "$WRK_CARD_TABLE_CREATION" -m "CARD WRK table creation failed"
`;

`. $SCRIPTS_DIR/load_library.ksh
run_query -d $STGDB -q "$WRK_CLSP_TABLE_CREATION" -m "CLSP WRK table creation failed"
`;

######################################################
# Data parsing                                       #
######################################################


my @Orders;
my @Cards;
my @ClientDetails;


my $data = $json->decode( $jsonString );

foreach my $d (@$data)
{ 
  my %Order = %$d;
  if (defined $Order{'ShipTo'} ) {
    my %ShipToData = %{$Order{'ShipTo'}};
    @Order{keys %ShipToData} = values %ShipToData;
  }
  if (defined $Order{'BillTo'} ) {
    my %BillToData = %{$Order{'BillTo'}};
    @Order{keys %BillToData} = values %BillToData; 
  }
  push(@Orders , \%Order); 

  #Fetching the Card Array                                                
  my @CardData = @{$Order{'Cards'}};
  foreach my $cd (@CardData)
  {
    my %EachCard = %$cd;  #Pushing The Card Data into Cards Hash
    my %CardHash = ( 
                  'ClientOrderId' => $Order{'ClientOrderId'},
				  'AimOrderId' => $Order{'AimOrderId'},
				  'AimClientId' => $Order{'AimClientId'},
                  'CardNumber' => $EachCard{'CardNumber'},
                  'AimCardId' => $EachCard{'AimCardId'},
                  'ClientCardId' => $EachCard{'ClientCardId'},
                  'Amount' => $EachCard{'Amount'},
                  'CardSKU' => $EachCard{'CardSKU'},
                  'ActivationDate' => $EachCard{'ActivationDate'},
                  'DigitalDeliveryEmail' => $EachCard{'DigitalDeliveryEmail'},
                   );

    push(@Cards , \%CardHash); 
  }

  #Client Specific Details for each order
  if (defined $Order{'ClientSpecificData'}) {
    my @ClientSpecificData = @{$Order{'ClientSpecificData'}};
    foreach my $csp (@ClientSpecificData)
    {
      my %EachClient = %$csp;
      my %ClientHash = (
                    'OrderId' => $Order{'ClientOrderId'},
                    'Value' => $EachClient{'Value'},
                    'Key' => $EachClient{'Key'},
                     );
      push(@ClientDetails , \%ClientHash);
    }
  }
}

print " DUMPER:\n[". Dumper (@Orders) . "]\n" if ($debug);
print " DUMPER:\n[". Dumper (@Cards) . "]\n" if ($debug);
print " DUMPER:\n[". Dumper (@ClientDetails) . "]\n" if ($debug);

my $nzloadCmd;

#NZLOAD for order details
$nzloadCmd = q#| nzload -db # . $STGDB . q# -t # . $WRK_ORDER_TABLE_NAME . q# -crInString  -nullValue '' -ctrlChars -fillRecord -maxErrors 30 -lf 'log/gc_aim_parse_nzload.log' -bf 'log/gc_aim_parse_nzload.bad'#;
if ($runNz) {
   open(NZLOAD, $nzloadCmd) || die; 
}

foreach my $order (@Orders)
{
  my %Order = %$order;
  my $record = joinFields(
	$Order{'ClientOrderId'},
	$Order{'AimOrderId'},
	$Order{'AimClientId'},
	$Order{'OrderDate'},
	$Order{'ShipDate'},
	$Order{'ShipType'},
	$Order{'OrderType'},
	$Order{'Amount'},
	$Order{'Discount'},
	$Order{'TrackingNumber'},
	$Order{'ShippingAmount'},
	$Order{'HandlingFee'},
	$Order{'PersonalizationFee'},
	$Order{'BillTo'}{'namePrefix'},
	$Order{'BillTo'}{'firstName'},
	$Order{'BillTo'}{'lastName'},
	$Order{'BillTo'}{'address1'},
	$Order{'BillTo'}{'address2'},
	$Order{'BillTo'}{'city'},
	$Order{'BillTo'}{'state'},
	$Order{'BillTo'}{'country'},
	$Order{'BillTo'}{'zip'},
	$Order{'BillTo'}{'phoneNumber'},
	$Order{'BillTo'}{'phoneExt'},
	$Order{'BillTo'}{'Email'},
	$Order{'BillTo'}{'companyName'},
	$Order{'ShipTo'}{'namePrefix'},
	$Order{'ShipTo'}{'firstName'},
	$Order{'ShipTo'}{'lastName'},
	$Order{'ShipTo'}{'address1'},
	$Order{'ShipTo'}{'address2'},
	$Order{'ShipTo'}{'city'},
	$Order{'ShipTo'}{'state'},
	$Order{'ShipTo'}{'country'},
	$Order{'ShipTo'}{'zip'},
	$Order{'ShipTo'}{'phoneNumber'},
	$Order{'ShipTo'}{'phoneExt'},
	$Order{'ShipTo'}{'email'},
	$Order{'ShipTo'}{'companyName'},
	'-2',
	'-2',
	'-2',
	'-2',
	'-2',
	'-2',
	'-2',
	'-2',
	'-2',
	'-2',
    $PROCESSING_DATETIME,
	) . "\n";
  
   print NZLOAD $record if ($runNz);
   print $record if ($debug);
}
close(NZLOAD) if ($runNz);

$nzloadCmd = q#| nzload -db # . $STGDB . q# -t # . $WRK_CARD_TABLE_NAME . q# -crInString  -nullValue '' -ctrlChars -fillRecord -maxErrors 30 -lf 'log/gc_aim_parse_nzload.log' -bf 'log/gc_aim_parse_nzload.bad'#;

if ($runNz) {
   open(NZLOAD, $nzloadCmd) || die; 
}

foreach my $card (@Cards)
{ 
  my %card = %$card;
  my $record = joinFields(
	$card{'ClientOrderId'},
	$card{'AimOrderId'},
	$card{'AimClientId'},
	$card{'ClientCardId'},
	$card{'AimCardId'},
	$card{'Amount'},
	$card{'CardSKU'},
	$card{'CardNumber'},
	$card{'ActivationDate'},
	$card{'DigitalDeliveryEmail'},
	$PROCESSING_DATETIME,
  ) . "\n";

   print NZLOAD $record if ($runNz);
   print $record if ($debug);
}
close(NZLOAD) if ($runNz);

#NZLOAD for client specific data

$nzloadCmd = q#|nzload -db # . $STGDB . q# -t # . $WRK_CLSP_TABLE_NAME . q# -crInString  -nullValue '' -ctrlChars  -fillRecord -maxErrors 30 -lf 'log/gc_aim_parse_nzload.log' -bf 'log/gc_aim_parse_nzload.bad'#;

if ($runNz) {
   open(NZLOAD, $nzloadCmd) || die; 
}
  
foreach my $cd (@ClientDetails)
{
   my %EachClientDetail = %$cd;
   my $record = joinFields(
	$EachClientDetail{'OrderId'},
	$EachClientDetail{'Key'},
	$EachClientDetail{'Value'},
	$PROCESSING_DATETIME,
  ) . "\n";
  
   print NZLOAD $record if ($runNz);
   print $record if ($debug);

   }
close(NZLOAD) if ($runNz);

# my $UPDATE_KEY_SQL = "
# UPDATE $WRK_ORDER_TABLE_NAME 
# SET 	BILLTO_EMAIL_KEY    	= -2,
		# SHIPTO_EMAIL_KEY   		= -2,
		# BILLTO_PHONE_KEY    	= -2,
		# SHIPTO_PHONE_KEY   		= -2,
		# BILLTO_PERSON_KEY   	= -2,
		# SHIPTO_PERSON_KEY  		= -2,
		# BILLTO_ADDRESS_KEY  	= -2,
		# SHIPTO_ADDRESS_KEY 		= -2,
		# BILLTO_CARD_PERSON_KEY	= -2,
		# SHIPTO_CARD_PERSON_KEY	= -2
# ";

# `
# . $SCRIPTS_DIR/load_library.ksh
# run_query -d $STGDB -q "$UPDATE_KEY_SQL" -m "Updating KEYs to -2 failed"
# `;
# print "Update KEYs complete\n"
# ;

exit 0;

sub joinFields {
  my(@fields) = @_;
  my $record = join(chr(9), map(defined $_ ? $_ : "", @fields));
  return $record;
}


