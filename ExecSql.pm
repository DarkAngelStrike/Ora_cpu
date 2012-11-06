# ExecSql.pm: Execute SQL statements by sqlplus
#
# This module allows to execute ORACLE SQL commands
#
# ExecSql is similar in function to CPAN DBD/DBI::Oracle with one important difference:
#
# **  It does not require C/C++ compiler to be present **
#
# Getting C compiler installed on production non-Linux (i.e. AIX) systems might be
# a tough deal for various reasons: $$, security etc)
# To work around that, the only thing ExecSql needs is: sqlplus (and access to /tmp)
#
# ExecSql implements a small subset of DBI::Oracle functionality:
#   think - SELECTs mainly, returning relatively few rows (100s, not 100 million)
# 
# In other words,
#
# IF you need a full scale SQL functionality, including DML, stored procedures,
#   return lots of data or care about microsecond response times,
#   OR you already have Perl DBI::Oracle installed
#   THEN ExecSql is not for you and you should use DBI::Oracle instead
#
# HOWEVER, if the only thing you need to do is to run a bunch of small SQLs 
#   that return only a few rows AND you do not want the hassle of installing
#   a full scale DBI::Oracle, then this module is for you
#
# How to use:
#
# use ExecSql;
# 
# my $SQL = ExecSql->connect('connection string');
# my $szSql = 'select user from dual';
#
# my $raRows = $SQL->selectall_hashref($szSql);  [{"COL11"->col11, "COL12"->col12, ..., }, ...] 
# my $raRows = $SQL->selectall_arrayref($szSql); [[col11, col12, ..., ], [col21, col22, ...], ...]
# my $szRows = $SQL->selectall_str($szSql);      "col11 col12 ...", "col21 col22 ..."
#
# if(0 != $SQL->err) {
#   die $SQL->errstr;
# } else {
#   print $SQL->rows . " rows returned\n";
#   foreach $rhRow @$raRows { # Assumes: selectall_hashref
#     print "User: " . $rhRow->{USER} . "\n";
#   }
# }
#
# Known issues:
#   Column name is truncated to the length of column size,
#     i.e. substr(status, 1, 2) AS Status will be truncated to 'ST'
#
# Author: Maxym Kharchenko maxymkharchenko@yahoo.com
#
##########################################################################

package ExecSql;
use strict;

#-----------------------------------------------------------------------
# Constants
#-----------------------------------------------------------------------
my $gc_nSqlUndef = -2;  # Execution status: undefined number
my $gc_szSqlUndef = ""; # Execution status: undefined string
my $gc_nSqlOk = 0;      # Execution status: successful
my $gc_nSqlError = -1;  # Execution status: error
my $gc_nFile = 10;      # Parameter type: file name
my $gc_nStr = 11;       # Parameter type: string
my $gc_nPageSize = 9999;  # SQLplus page size
my $gc_nLineSize = 8192;  # SQLplus line size
my $gc_nStmtTypeSelect = 0;  # SQLplus select STMT type
my $gc_nStmtTypeOther = 1;   # SQLplus "other" STMT type

my @gc_DmlPatterns = (
"^.*row.*selected", 
"^.*row.*created", 
"^.*row.*updated",  
"^.*row.*deleted"  
);

#-----------------------------------------------------------------------
# Private members
#-----------------------------------------------------------------------
my $g_nHandles = 0;     # Number of db handles
my @g_aHandles = ();    # List of db handles
my $lasth = undef;      # Last db handle

END {
   map { $_->disconnect(); } @g_aHandles;
}

# ----------------------------------------------------------------------------
# SigHandler: Exit program cleanly
# ----------------------------------------------------------------------------
sub SigHandler
{
   map { $_->disconnect(); } @g_aHandles;
   die "You killed me while I was executing SQL ;-(";
}

# ----------------------------------------------------------------------------
# Convert string to number
# Parameter: 1 - "Number" string (mandatory)
#            2 - (0 - do not Exit if NOT number , 1 (or NOT specified) - Exit)
#                (optional)
# Returns:   Number if parsed or 0 if unparsed and "Die" flag is not set
# ----------------------------------------------------------------------------
sub StrToNumber
{
   my ($szStr, $szRet);
   my $bDie = 1;

   die("Wrong number of parameters in StrToNumber()")
      if ($#_ != 0) && ($#_ != 1);
   $szStr = $_[0];
   $bDie = $_[1] if 1 == $#_;

   die("Invalid value of bDie var: $bDie in StrToNumber()")
      if (1 != $bDie) && (0 != $bDie);

   if($szStr =~ /[0-9]+[0-9\.]*/) {
      $szRet = $szStr;
   } else {
      die("Error parsing number string: $szStr in StrToNumber()")
         if 1 == $bDie;
      $szRet = "0";
   }

   return $szRet;
}


# ----------------------------------------------------------------------------
# Remove blanks from both ends
# Parameter: 1 - string to be trimmed
# ----------------------------------------------------------------------------
sub trim
{
   my $szStr = undef;

   die("Wrong number of parameters in trim()")
      if($#_ != 0);
   $szStr = $_[0];

   $szStr =~ s/^\s+//; # leading
   $szStr =~ s/\s+$//; # trailing

   return $szStr;
}

# ------------------------------------------------------------------------
# Initializes "state" of the db connection object
# ------------------------------------------------------------------------
sub _CleanSqlVars
{
   my $self = shift;
   my $varbase = $self->{_permitted};

   $varbase->{err} = $gc_nSqlOk;
   $varbase->{errstr} = $gc_szSqlUndef;
   $varbase->{rows} = $gc_nSqlUndef;
   $varbase->{LAST_SQLOUTPUT} = [];
   $varbase->{LAST_SQLOUTPUT_ARR} = [];
   $varbase->{LAST_SQLOUTPUT_HASH} = [];
   $varbase->{LAST_SQLHEADERS_ARR} = [];
   $varbase->{LAST_SQLCOLSIZES_ARR} = [];
   $varbase->{LAST_SQLHEADERS_FORMAT} = "";
}

# ------------------------------------------------------------------------
# Removes connection information ("closes" db connection)
# ------------------------------------------------------------------------
sub disconnect
{
   my $self = shift;
   my $varbase = $self->{_permitted};

   unlink $varbase->{OUTFILE} if -f $varbase->{OUTFILE};
   unlink $varbase->{SQLFILE} if -f $varbase->{SQLFILE};
   $varbase->{ACTIVE_SQL}  = $gc_szSqlUndef;
   $varbase->{N_STMT_TYPE} = $gc_nSqlUndef;

   for(my $i=0; $i <= $#g_aHandles; $i++) {
      if($self == $g_aHandles[$i]) {
         splice(@g_aHandles, $i, 1);
         last;
      }
   }
   $g_nHandles--;
}

# ------------------------------------------------------------------------
# Parses TNS string into user, password and TNS name
# Parameters: 1 - TNS string
# Returns:    (TNS name, User, Password)
# ------------------------------------------------------------------------
sub ParseTns
{
   my $szConnection;
   my $szTns;

   die("Wrong number of parameters in ExecSql::ParseTns()\n")
      if 0 != $#_;
   $szConnection = $_[0];

   die("Empty TNS string in ExecSql::ParseTns()")
      if ! defined($szConnection) || (0 == length($szConnection));

   $szConnection =~ /(\S+)\/(\S+)\@(\S+)/;

   die("Invalid TNS string: $szConnection in ExecSql::ParseTns()")
      if ! defined($1) || ! defined($2) || ! defined($3);

   return ($1, $2, $3);
}

# ------------------------------------------------------------------------
# Constructor 
# Parameters: 1 - TNS connection string (mandatory)
#             2 - connection options (optional)
#                 currently supported: TMPDIR, LOGOBJ, LOGLEVEL, CONNECT_AS,
#                 DIE_IF_ERROR
# ------------------------------------------------------------------------
sub connect {
   my $proto = shift;
   my $class = ref($proto) || $proto;
   my $self = undef;
   my $Var;
   my ($szConn, $rhOpts);

   my %SqlVars= (
      DBCONN  => "",
      OUTFILE => "%TMPDIR%/execsql.$$.$g_nHandles.out",
      SQLFILE => "%TMPDIR%/execsql.$$.$g_nHandles.sql",
      ACTIVE_SQL        => $gc_szSqlUndef,
      N_STMT_TYPE       => $gc_nSqlUndef,
      err               => $gc_nSqlOk,
      errstr            => $gc_szSqlUndef,
      rows              => $gc_nSqlUndef,
      LAST_SQLOUTPUT       => [],
      LAST_SQLOUTPUT_ARR   => [],
      LAST_SQLOUTPUT_HASH  => [],
      LAST_SQLHEADERS_ARR  => [],
      LAST_SQLHEADERS_FORMAT => "",
      LAST_SQLCOLSIZES_ARR => [],
      LOG               => undef,
      LOGLEVEL          => 0,
      DIE_IF_ERROR      => "N"
   );

   die("Invalid number of parameters in ExecSql::connect()")
      if ($#_ != 0) && ($#_ != 1);
   $szConn = $_[0];
   $rhOpts = $_[1] if $#_ >= 1;

   $SqlVars{DBCONN} = $szConn;
   if($szConn =~ /^sys\//i) {
      $SqlVars{DBCONN} .= " as sysdba";
   } elsif(exists ($rhOpts->{CONNECT_AS}) && 
           defined ($rhOpts->{CONNECT_AS})) {
      $SqlVars{DBCONN} .= " as " . $rhOpts->{CONNECT_AS};
   }

   $SqlVars{TMPDIR} = "/tmp" if (-d "/tmp") && (-w "/tmp");
   $SqlVars{TMPDIR} = $ENV{TEMP} if exists $ENV{TEMP};
   $SqlVars{TMPDIR} = $rhOpts->{TMPDIR}
      if exists ($rhOpts->{TMPDIR}) && defined ($rhOpts->{TMPDIR});

   # Expand variables
   foreach $Var (keys %SqlVars) {
      next if $Var =~ /_ARR|_HASH|N_/;
      next if $Var eq "LOG";
      $SqlVars{$Var} =~ s:%([^\\/]+)%:$SqlVars{$1}:g;
   }

   $SqlVars{DIE_IF_ERROR} = "Y"
      if exists ($rhOpts->{DIE_IF_ERROR}) && 
         defined ($rhOpts->{DIE_IF_ERROR}) &&
         "Y" eq uc($rhOpts->{DIE_IF_ERROR});
   $SqlVars{LOG} = $rhOpts->{LOGOBJ}
      if exists ($rhOpts->{LOGOBJ}) && defined ($rhOpts->{LOGOBJ});
   $SqlVars{LOGLEVEL} = StrToNumber($rhOpts->{LOGLEVEL})
      if exists ($rhOpts->{LOGLEVEL}) && defined ($rhOpts->{LOGLEVEL});

   $self = {
      _permitted => \%SqlVars,
      %SqlVars
   };

   bless($self, $class);

   $g_nHandles++;
   push @g_aHandles, $self;

   return $self;
}

# ------------------------------------------------------------------------
# Process single SQL statement
# Parameters: 1 - SQL text (mandatory)
# ------------------------------------------------------------------------
sub _ProcessSqlStmt
{
   my $self = shift;
   my $varbase = $self->{_permitted};
   my $szSqlFile = $varbase->{SQLFILE};
   my $szSqlCmd;
   my $log = $varbase->{LOG};
   my $szDbCon;

   $szDbCon = $varbase->{DBCONN};
   $szDbCon =~ s/\/[^\@]+/\/***/;

   die("Wrong number of parameters in ExecSql::_ProcessSqlStmt()\n")
      if 0 != $#_;
   $szSqlCmd = trim($_[0]);

   open(SQLFILE, ">$szSqlFile") or die("Cannot open $szSqlFile: $!");
   print SQLFILE "spool " . $varbase->{OUTFILE} . "\n";
   print SQLFILE "connect " . $varbase->{DBCONN} . "\n";
   print SQLFILE "set pagesize " . $gc_nPageSize . "\n";
   print SQLFILE "set linesize " . $gc_nLineSize . "\n";
   print SQLFILE "clear columns\n";
   print SQLFILE "set heading on\n";
   print SQLFILE "set feedback on\n";
   print SQLFILE "set verif off\n";
   print SQLFILE "set newpage none\n";
   print SQLFILE "set trimspool on\n";

   $self->_FindStmtType($szSqlCmd);
   print SQLFILE "$szSqlCmd";
   print SQLFILE ";" if(substr($szSqlCmd, length($szSqlCmd)-1) ne ";");
   print SQLFILE "\n";
   print SQLFILE "exit\n";
   close(SQLFILE);

   $varbase->{ACTIVE_SQL} = $szSqlCmd;

   if((ref $log) && ($varbase->{LOGLEVEL} >= 1)) {
      $log->PrintTS("Connecting to: " . $szDbCon);
      $log->PrintTS("START: " . $varbase->{ACTIVE_SQL});
   }
}

# ------------------------------------------------------------------------
# Run statements from SQL File
# Parameters: 1 - SQL File Name
#             2 - SQL File Options (optional)
# ------------------------------------------------------------------------
sub _ProcessSqlFile
{
   my $self = shift;
   my $varbase = $self->{_permitted};
   my $szSqlCmd = "";
   my $szSqlFile;
   my $szSqlFileName;
   my @aLine;
   my $rhOpts;
   my $i;

   die("Wrong number of parameters in ExecSql::_ProcessSqlFile()\n")
      if (0 != $#_) && (1 != $#_);
   $szSqlFileName = trim($_[0]);
   $rhOpts = $_[1] if 1 == $#_;

   open(SOURCESQL, "<$szSqlFileName") or 
      die("Cannot open $szSqlFileName: $!");

   # Here we find all statements and run them individually
   while(<SOURCESQL>) {
      next if /^\s*exit\b/i;
      next if /^\s*spool\b/i;
      next if /^\s*set\b/i;
      next if /^\s*connect\b/i;
      chomp; 
      next if 0 == length($_);

      foreach $i (keys %{$rhOpts}) {
         $_ =~ s/&$i/$rhOpts->{$i}/g;
      }

      if(/[\/;]/) {
         @aLine = split(/[\/;]/, $_);
 
         if (-1 == $#aLine) { # Only delimiter in line
            $self->do($szSqlCmd) if 0 != length($szSqlCmd);
            $szSqlCmd = "";
         } elsif (0 == $#aLine) { # 1 command in line
            $szSqlCmd .= $aLine[0];
            if($_ ne $aLine[0]) {   # We have a delimiter in line
               $self->do($szSqlCmd) if 0 != length($szSqlCmd); 
               $szSqlCmd = "";
            }
         } else { # Many commands in line
            for($i = 0; $i <= $#aLine; $i++) {
               if(0 == $i) {
                  $szSqlCmd .= $aLine[$i];
               } else {
                  $szSqlCmd = $aLine[$i];
               }
               if($i != $#aLine) {
                  $self->do($szSqlCmd) if 0 != length($szSqlCmd); 
                  $szSqlCmd = "";
               }
            }
         }
      } else { $szSqlCmd .= $_; }
   }

   $self->do($szSqlCmd) if 0 != length($szSqlCmd);

   close(SOURCESQL);
}

# ------------------------------------------------------------------------
# Find SQL type (select or DML)
# Parameters: 1 - SQL to analyze
# ------------------------------------------------------------------------
sub _FindStmtType
{
   my $self = shift;
   my $varbase = $self->{_permitted};

   die("Wrong number of parameters in ExecSql::_FindStmtType()\n")
      if 0 != $#_;

   $varbase->{N_STMT_TYPE} = ($_[0] =~ /SELECT/i) ? 
      $gc_nStmtTypeSelect : $gc_nStmtTypeOther;
}

# ------------------------------------------------------------------------
# Find status and error message from the last executed SQL statement
# ------------------------------------------------------------------------
sub _FindSqlStatus
{
   my $self = shift;
   my $varbase = $self->{_permitted};
   my $szSqlOutFile = $varbase->{OUTFILE};
   my $log = $varbase->{LOG};
   my $bStoreOutput = 0;
   my $bError = 0;
   my $szHeaders = undef;
   my @aOutput = ();
   my $szStr = "";
   my $szLastGoodStr = "";
   my $szPattern = "";
   my @aTmp = ();
   my $nLength = 0;
   my $cCnt = 0;

   # Set everything initially as "undefined"
   $self->_CleanSqlVars();

   die("Wrong number of parameters in ExecSql::_FindSqlStatus()\n")
      if -1 != $#_;

   open(SQLOUT, "<$szSqlOutFile") 
      or die("Cannot open Sql out file: $szSqlOutFile: $!");

   while(<SQLOUT>) {
      chomp;

      if (/^ORA-/ || /^SP2-/) {
         $varbase->{err}=$gc_nSqlError;
         $varbase->{errstr} .= $_;
         $bError = 1;
      }

      next if 1 == $bError;

      if(/^.*Connected/) {
         $bStoreOutput = 1;
         next;
      }

      next if 0 == $bStoreOutput;

      $cCnt++; $cCnt = 1 if $cCnt > $gc_nPageSize;
      next if ! /\S+/; # Skip empty lines
      $szLastGoodStr = $_;

      if($gc_nStmtTypeSelect == $varbase->{N_STMT_TYPE}) {
         # Column headers
         if(1 == $cCnt) {
            if(not defined $szHeaders) {
               $szHeaders = $szLastGoodStr;
               next;
            }
         }

         # Here we should determine columns and their sizes
         # by analyzing ----- s
         if((2 == $cCnt) && ($szLastGoodStr =~ /^-+/)) {
            # This is very important line as we expand all 
            # strings to exactly the same length - which will be expected
            # by the unpack function later
            # Second line's length (with '------'s) is taken as an etalon
            $nLength = length($szLastGoodStr);
            $szHeaders = pack("A$nLength", $szHeaders);
            $szHeaders = sprintf("%-${nLength}s", $szHeaders);

            $szPattern = $szLastGoodStr;
            $szPattern =~ s/\-+/"A".length($&)/eg;
            $szPattern =~ s/\s/x/g;
            @aTmp = unpack($szPattern, $szHeaders);
            map { $_ = trim($_); }  @aTmp;
            push @{ $varbase->{LAST_SQLHEADERS_ARR} }, @aTmp;

            $varbase->{LAST_SQLHEADERS_FORMAT} = $szPattern;
            $_ = $szPattern;
            @aTmp = /(\d+)/g;
            map { $_ = trim($_); }  @aTmp;
            push @{ $varbase->{LAST_SQLCOLSIZES_ARR} }, @aTmp;

            next; 
         }
      } # END if($gc_nStmtTypeSelect == $varbase->{N_STMT_TYPE})

      push @aOutput, pack("A$nLength", $szLastGoodStr);
   } # End WHILE

   close(SQLOUT);

   if (0 == $bStoreOutput) {
      $varbase->{err} = $gc_nSqlError;
      $varbase->{errstr} .= "Cannot find 'Connect' in output file";
   }

   if ($gc_nSqlOk == $varbase->{err}) {
      # Get number of records in SQL output
      $szStr = pop @aOutput;
      push @aOutput, $szStr if ! _IsDmlStatus($szLastGoodStr);

      # Number of records - is valid only for DML statements
      if(_IsDmlStatus($szLastGoodStr)) {
         @aTmp = split(/\s/, $szLastGoodStr);
         $varbase->{rows} = ("no" eq $aTmp[0]) ? 0 : $aTmp[0];
      } else { $varbase->{rows} = $gc_nSqlUndef; }

      # Get SQL output
      @{ $varbase->{LAST_SQLOUTPUT} } = @aOutput;
      $lasth = $self;
   } # END IF ($gc_nSqlOk == $varbase->{err})

   if((ref $log) && ($varbase->{LOGLEVEL} >= 1)) {
      if($gc_nSqlOk == $varbase->{err}) {
         $log->PrintTS("OK: " . $varbase->{ACTIVE_SQL});
         if(_IsDmlStatus($szLastGoodStr) && (0 != $varbase->{rows}) && 
            ($varbase->{LOGLEVEL} >= 2)) {
            $log->Print("@{$varbase->{LAST_SQLHEADERS_ARR}}");
            map { $log->Print("@{$_}"); } @{$self->fetchall_arrayref()};
         }
      } else {
         $log->PrintTS("FAILED: " . $varbase->{ACTIVE_SQL});
         $log->PrintTS("ERROR: " . $varbase->{errstr});
      }
   } # END IF (ref $log ...)

   die($varbase->{errstr}) 
      if ("Y" eq $varbase->{DIE_IF_ERROR}) && (0 != $varbase->{err});

} # END SUB _FindSqlStatus()

# ------------------------------------------------------------------------
# Check if the line is describing DML execution status
# Parameters: 1 - SQL output to analyze
# Returns: 1 - if "status line", 0 - otherwise
# ------------------------------------------------------------------------
sub _IsDmlStatus
{
   my $Var;

   die("Wrong number of parameters in ExecSql::_IsDmlStatus()\n")
      if 0 != $#_;

   return 0 if ! defined ($_[0]);
   return 0 if (0 == length($_[0]));

   foreach $Var (@gc_DmlPatterns) {
      return 1 if $_[0] =~ /$Var/;
   }

   return 0;
}

# ------------------------------------------------------------------------
# Execute SQL
# Parameters: 1 - SQL text (mandatory)
#             2, 3, ... - parameters (will replace &n sqlplus variables)
# Return values: # of rows
# ------------------------------------------------------------------------
sub do
{
   my $self = shift;
   my $varbase = $self->{_permitted};
   my $szCmd = "sqlplus /nolog @" . $varbase->{SQLFILE};

   $self->_ProcessSqlStmt($_[0]);
   shift;
   $szCmd .= " @_";

   my $SaveInt = $SIG{INT}; $SIG{INT} = \&SigHandler;

   `$szCmd`; 
   $self->_FindSqlStatus();

   $SIG{INT} = $SaveInt;

   return $varbase->{rows};
}

# ------------------------------------------------------------------------
# Execute SQL from file
# Parameters: 1 - SQL file (mandatory)
#             2 - Substitute variables (optional)
# Return values: # of rows for the last command
# ------------------------------------------------------------------------
sub do_file
{
   my $self = shift;
   my $varbase = $self->{_permitted};

   die("Wrong number of parameters in ExecSql::do_file()\n")
      if (0 != $#_) && (1 != $#_) ;

   $self->_ProcessSqlFile(@_);

   return $varbase->{rows};
}

# ------------------------------------------------------------------------
# Returns status of last SQL command
# Return values: 0  - OK
#               -1 - Error
#               -2  - No SQL has been executed yet
# ------------------------------------------------------------------------
sub err
{
   my $self = shift;
   my $varbase = $self->{_permitted};

   return $varbase->{err};
}

# ------------------------------------------------------------------------
# Returns error message for last SQL command
# ------------------------------------------------------------------------
sub errstr
{
   my $self = shift;
   my $varbase = $self->{_permitted};

   return $varbase->{errstr};
}

# ------------------------------------------------------------------------
# Returns number of rows for last SQL command
# ------------------------------------------------------------------------
sub rows
{
   my $self = shift;
   my $varbase = $self->{_permitted};

   return $gc_nSqlUndef if $gc_nSqlOk != $varbase->{err};

   return $varbase->{rows};
}

# ------------------------------------------------------------------------
# Returns handle for last SQL command
# ------------------------------------------------------------------------
sub lasth
{
   my $self = shift;

   return $lasth;
}

# ------------------------------------------------------------------------
# Returns column headers for last SQL command
# ------------------------------------------------------------------------
sub sqlheaders_arrayref
{
   my $self = shift;
   my $varbase = $self->{_permitted};

   return [] if $gc_nSqlOk != $varbase->{err};
   return [] if -1 == $#{$varbase->{LAST_SQLHEADERS_ARR}};

   return $varbase->{LAST_SQLHEADERS_ARR};
}

# ------------------------------------------------------------------------
# Returns output from last SQL command as array of arrays
# Format: [[col11, col12, ..., ], [col21, col22, ...], ...]
# ------------------------------------------------------------------------
sub fetchall_arrayref
{
   my $self = shift;
   my $varbase = $self->{_permitted};
   my @aOutput;
   my @aRow;
   my $szRow;
   my $nLen;

   return [] if $gc_nSqlOk != $varbase->{err};
   return [] if $gc_nStmtTypeSelect != $varbase->{N_STMT_TYPE};
   return [] if -1 == $#{$varbase->{LAST_SQLOUTPUT}};
   return [] if 0 == length($varbase->{LAST_SQLHEADERS_FORMAT});

   return $varbase->{LAST_SQLOUTPUT_ARR} 
      if -1 != $#{$varbase->{LAST_SQLOUTPUT_ARR}};

   foreach $szRow (@{ $varbase->{LAST_SQLOUTPUT} }) {
      @aRow = unpack($varbase->{LAST_SQLHEADERS_FORMAT}, $szRow);
      map { $_ = trim($_); } @aRow;
      push @aOutput, [@aRow];
   }

   $varbase->{LAST_SQLOUTPUT_ARR} = [ @aOutput ];

   return $varbase->{LAST_SQLOUTPUT_ARR};
}

# ------------------------------------------------------------------------
# Returns output from last SQL command as array of strings
# Format: "col11 col12 ...", "col21 col22 ..."
# ------------------------------------------------------------------------
sub fetchall_str
{
   my $self = shift;
   my $varbase = $self->{_permitted};
   my @aTmp;
   my $chOldSep = $";
   my $szStr = undef;

   return $gc_szSqlUndef if $gc_nSqlOk != $varbase->{err};
   return [] if $gc_nStmtTypeSelect != $varbase->{N_STMT_TYPE};

   @aTmp = @{ $varbase->{LAST_SQLOUTPUT} };
   map { $_ = trim($_); } @aTmp;

   $" = "\n";
   $szStr = "@aTmp";
   $" = $chOldSep;

   return $szStr;
}

# ------------------------------------------------------------------------
# Returns output from last SQL command as array of hashes
# Format: [{"COL11"->col11, "COL12"->col12, ..., }, ...]
# ------------------------------------------------------------------------
sub fetchall_hashref
{
   my $self = shift;
   my $varbase = $self->{_permitted};
   my @aOutput;
   my $raRow;
   my $raHeaders;
   my $rhRow;
   my $nPos = 0;
   my $i;

   return [] if $gc_nSqlOk != $varbase->{err};
   return [] if $gc_nStmtTypeSelect != $varbase->{N_STMT_TYPE};
   return [] if -1 == $#{$varbase->{LAST_SQLOUTPUT}};

   return $varbase->{LAST_SQLOUTPUT_HASH}
      if -1 != $#{$varbase->{LAST_SQLOUTPUT_HASH}};

   $self->fetchall_arrayref();
   return [] if -1 == $#{$varbase->{LAST_SQLOUTPUT_ARR}};

   $raHeaders = $varbase->{LAST_SQLHEADERS_ARR};

   # Create hash
   foreach $raRow (@{ $varbase->{LAST_SQLOUTPUT_ARR} }) {
      $rhRow = {};
      for($i=0; $i <= $#{$raRow}; $i++) {
         $rhRow->{$raHeaders->[$i]} = $raRow->[$i];
      }
      push @aOutput, $rhRow;
   }

   $varbase->{LAST_SQLOUTPUT_HASH} = [ @aOutput ];
    
   return $varbase->{LAST_SQLOUTPUT_HASH};
}

# ------------------------------------------------------------------------
# Returns output from last SQL command as array of hashes
# Parameters: 1 - SQL command to execute (mandatory)
# Format: [{"COL11"->col11, "COL12"->col12, ..., }, ...]
# ------------------------------------------------------------------------
sub selectall_hashref
{
   my $self = shift;
   my $varbase = $self->{_permitted};
   my $szSql;

   die("Invalid # of parameters in ExecSql::selectall_hashref()")
      if 0 != $#_;
   $szSql = $_[0];

   $self->do($szSql) if $szSql ne $varbase->{ACTIVE_SQL};

   return $self->fetchall_hashref();
}

# ------------------------------------------------------------------------
# Returns row output from last SQL command as hash
# Parameters: 1 - SQL command to execute (mandatory)
#             2 - Row # (mandatory)
# Format: {"COL11"->col11, "COL12"->col12, ..., }
# ------------------------------------------------------------------------
sub selectrow_hashref
{
   my $self = shift;
   my $varbase = $self->{_permitted};
   my ($szSql, $nRow);
   my $rhRes;

   die("Invalid # of parameters in ExecSql::selectrow_hashref()")
      if 1 != $#_;
   ($szSql, $nRow) = @_;

   $rhRes = $self->selectall_hashref($szSql);

   if(($nRow > $varbase->{rows}) or ($nRow <= 0)) {
      return undef;
   }
      
   return @{$rhRes}[$nRow-1];
}

# ------------------------------------------------------------------------
# Returns output from last SQL command as array of arrays
# Parameters: 1 - SQL command to execute (mandatory)
# Format: [[col11, col12, ..., ], [col21, col22, ...], ...]
# ------------------------------------------------------------------------
sub selectall_arrayref
{
   my $self = shift;
   my $varbase = $self->{_permitted};
   my $szSql;

   die("Invalid # of parameters in ExecSql::selectall_arrayref()")
      if 0 != $#_;
   $szSql = $_[0];

   $self->do($szSql) if $szSql ne $varbase->{ACTIVE_SQL};

   return $self->fetchall_arrayref();
}

# ------------------------------------------------------------------------
# Returns row output from last SQL command as array
# Parameters: 1 - SQL command to execute (mandatory)
#             2 - Row # (mandatory)
# Format: [col11, col12, ..., ]
# ------------------------------------------------------------------------
sub selectrow_arrayref
{
   my $self = shift;
   my $varbase = $self->{_permitted};
   my ($szSql, $nRow);
   my $rhRes;

   die("Invalid # of parameters in ExecSql::selectrow_arrayref()")
      if 1 != $#_;
   ($szSql, $nRow) = @_;

   $rhRes = $self->selectall_arrayref($szSql);

   if(($nRow > $varbase->{rows}) or ($nRow <= 0)) {
      return undef;
   }

   return @{$rhRes}[$nRow-1];
}

# ------------------------------------------------------------------------
# Returns output from last SQL command, formatted as strings
# Parameters: 1 - SQL command to execute (mandatory)
# Format: "col11 col12 ...", "col21 col22 ..."
# ------------------------------------------------------------------------
sub selectall_str
{
   my $self = shift;
   my $varbase = $self->{_permitted};
   my $szSql;

   die("Invalid # of parameters in ExecSql::selectall_arrayref()")
      if 0 != $#_;
   $szSql = $_[0];

   $self->do($szSql) if $szSql ne $varbase->{ACTIVE_SQL};

   return $self->fetchall_str();
}

# ------------------------------------------------------------------------
# Returns row output from last SQL command as string
# Parameters: 1 - SQL command to execute (mandatory)
#             2 - Row # (mandatory)
# Format: "col11 col12 ..."
# ------------------------------------------------------------------------
sub selectrow_str
{
   my $self = shift;
   my $varbase = $self->{_permitted};
   my ($szSql, $nRow);
   my @aRes;

   die("Invalid # of parameters in ExecSql::selectrow_str()")
      if 1 != $#_;
   ($szSql, $nRow) = @_;

   @aRes = split(/\n/, $self->selectall_str($szSql));

   if(($#aRes+1) != $varbase->{rows}) {
      return undef;
   }

   if(($nRow > $varbase->{rows}) or ($nRow <= 0)) {
      return undef;
   }

   return $aRes[$nRow-1];
}

1;
