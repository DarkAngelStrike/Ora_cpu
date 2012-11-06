#! /usr/bin/perl -w
#######################################################################
# Script displays CPU statistics for ORACLE processes running under AIX or Linux
# as well as associated session information
#
# Script is significantly more useful when used
# with ORACLE session information (-c parameter or $ORA_DB/$DORA_DB)
# and on a BIG screen (180 symbols minimum)
#
# Usage: Run ora_cpu.pl -h for usage info
#
# Prerequisites:
#   1) Database instance needs to run
#
# Maxym Kharchenko 2010-2011
#######################################################################

use strict;
use Getopt::Std qw(getopts);
use POSIX qw(ceil);
use FindBin qw($Bin);
use lib $Bin; 

# use Data::Dumper qw(Dumper);

# Trapping exit signals to exit (interactive mode) gracefully
use sigtrap qw(handler SigHandler normal-signals stack-trace error-signals);

BEGIN { $| = 1 } # Immediate flush in buffered output (printf etc)

#-----------------------------------------------------------------------
# Colored output setup - Term::ANSIColor should be standard in Perl 5.8+
#-----------------------------------------------------------------------
# Check whether Term::ANSIColor is installed and use it if it is
if ("require Term::ANSIColor") {
   use Term::ANSIColor qw(color);
   $Term::ANSIColor::AUTORESET = 1;
} else {
   eval "sub color { return ''; };"
}

# Terminal control code sequences
my $CURSOR_HIDE = "\e[?25l";   # Hide cursor
my $CURSOR_SHOW = "\e[?25h";   # Show cursor  

#-----------------------------------------------------------------------
# Script constants
#-----------------------------------------------------------------------
use constant PS_CPU_METRICS => qw(status total_time cpu cmd);
use constant ORACLE_CPU_METRICS => qw(sid ser s process client object logon_time event seconds_in_wait state sql_hash_value);
use constant ORACLE_BLOCKER_METRICS => qw(sid ser client lmode ty object event seconds_in_wait state sql_hash_value);
use constant ORACLE_ASH_METRICS => qw(event seconds_waited);
use constant ORACLE_LONGOPS_METRICS => qw(longops_info);

use constant DEFAULT_RUN_COUNT => 1000;

use constant STATUS_WAITING => 'Waiting ...';
use constant STATUS_WORKING => 'Collecting Data ...';

use constant ORACLE_IDLE_WAITS => (
  'dispatcher timer',
  'pipe get',
  'pmon timer',
  'PX Idle Wait',
  'PX Deq Credit: need buffer', 
  'rdbms ipc message',
  'shared server idle wait',
  'smon timer',
  'SQL\*Net message from client'
);

#-----------------------------------------------------------------------
# Script variables and default values
#-----------------------------------------------------------------------
my $g_rhScriptVars = {
  F_TOP_PROCESSES => -1,
  STRIP_STRINGS => '\(?TNS V1\-V3.*$|\(LOCAL\=.*$|\(DESCRIPTION\=.*$|\(ADDRESS\=.*$',
  ORACLE_SID    => $ENV{ORACLE_SID},
  ORACLE_CON    => defined($ENV{ORA_DB}) ? $ENV{ORA_DB} : $ENV{DORA_DB},
  SORT_BY       => 'cpu_util',
  PRINT_WAIT    => 'N',
  PRINT_SQL     => 'N',
  PRINT_OBJ     => 'N',
  PRINT_BLOCK   => 'N',
  PRINT_ASH     => 0,
  PRINT_LONGOPS => 'N',
  F_ACTIVE_ONLY => 'N',
  F_CLI_PATTERN => undef,
  F_SRV_PATTERN => undef,
  F_CLI_PID     => undef,
  F_SRV_PID     => undef,
  INTERACTIVE   => 'N',
  SCREEN_LINES   => 0,
  SCREEN_COLUMNS => 0,
  DISABLE_COLOR => 'N',
  WAIT_INTERVAL => -1,
  RUN_COUNT     => 1,
};

#-----------------------------------------------------------------------
# Global screen variables
#-----------------------------------------------------------------------
my $g_nCurrentLine = 0;

####################### SUBROUTINE SECTION #############################

# ----------------------------------------------------------------------------
# SigHandler: Exit program cleanly
# ----------------------------------------------------------------------------
sub SigHandler
{
  print color 'clear' if 'Y' ne $g_rhScriptVars->{DISABLE_COLOR};
  print $CURSOR_SHOW;
  die "Thank you for using ora_cpu.pl ;-)";
}

#-----------------------------------------------------------------------
# Replace undefined value
#-----------------------------------------------------------------------
sub nvl {
  my $i = shift;
  my $r = shift;

  $r = '' if ! defined $r;

  return (defined($i) ? $i : $r);
} 

#-----------------------------------------------------------------------
# Triple check: Hash key exists, value is defined and is not empty
#-----------------------------------------------------------------------
sub element_not_empty {
  my ($rhH, $szKey) = @_;

  die "Invalid HASH in element_not_empty()" if ! defined $rhH or 'HASH' ne ref $rhH;
  die "Invalid KEY in element_not_empty()" if ! defined $szKey or length($szKey) <= 0;

  return (exists($rhH->{$szKey}) and defined($rhH->{$szKey}) and length($rhH->{$szKey}) > 0) ? 1 : 0;
} 

#-----------------------------------------------------------------------
# Double check: Hash key exists and value is defined
#-----------------------------------------------------------------------
sub element_defined {
  my ($rhH, $szKey) = @_;

  die "Invalid HASH in element_not_empty()" if ! defined $rhH or 'HASH' ne ref $rhH;
  die "Invalid KEY in element_not_empty()" if ! defined $szKey or length($szKey) <= 0;

  return (exists($rhH->{$szKey}) and defined($rhH->{$szKey})) ? 1 : 0;
} 

#-----------------------------------------------------------------------
# Double check: value is defined and is not empty
#-----------------------------------------------------------------------
sub not_empty {   
  my $Val = shift;

  return (defined($Val) and length($Val) > 0) ? 1 : 0;
}

#-----------------------------------------------------------------------
# Print program header
#-----------------------------------------------------------------------
sub PrintHeader
{
  print <<EOM ;

Display UNIX and ORACLE Statistics for Active ORACLE Processes
Author: Maxym Kharchenko, 2010-2011

EOM
}

#-----------------------------------------------------------------------
# Print program usage info
#-----------------------------------------------------------------------
sub usage
{
  die <<EOM ;
usage: ora_cpu.pl <options | -h> [<wait seconds> [<count>] ]

Options: [-i oracle_sid] [-c oracle_connection | -C] [ -s cpu_util|cpu_pct|total_time ]

         [-q | -Q] [-w | -W] [-l | -L] [-o] [-b] [-H N] [-a]

         [-t N] [-A] [-z <client name pattern> ] [-Z <server name pattern>]
         [-p <client pid1,pid2 ...>] [-P <server pid1,pid2 ...>]

         [-g] [-E]

Basic Options:
  -i   Oracle Instance Name                         Default: \$ENV{ORACLE_SID}
  -c   Oracle Connection (SELECT ANY DICTIONARY)    Default: \$ENV{ORA_DB} or \$ENV{DORA_DB}
  -C   Do NOT connect to ORACLE
  -s   Sort processes by. Default: $g_rhScriptVars->{SORT_BY}

Additional Display:
  -q   Print curent SQL
  -Q   Print current or previous SQL
  -w   Print current wait info (skip idle waits)
  -W   Print current wait info (include all waits)
  -l   Print currently running long operations
  -L   Print ALL long operations (including completed)
  -o   Print current object info
  -H N Print top N active session history waits
  -b   Print blocking sessions
  -a   Equals: -Q -w -o -l

Filters:
  -t N Limit report to top N most active processes. Default: show all
  -A   Limit report to "active" sessions only (those that have active waits, objects etc)
  -z <pattern> Limit report to only specified <pattern> in client process names
  -Z <pattern> Limit report to only specified <pattern> in server process names
  -p <pid1,pid2 ...> Limit report to only specified client process pids
  -P <pid1,pid2 ...> Limit report to only specified server process pids

Miscellaneous:
  -E   Do NOT strip excessive information (default domain, TNS version etc)
  -g   Disable colored output

  -h   Print usage message

  Try these settings: -c db_user/db_password -a

EOM
}

#-----------------------------------------------------------------------
# Transform Pid list from comma separated to match pattern
#-----------------------------------------------------------------------
sub PidListToPattern {
  my $szPidList = shift;

  die "Undefined list supplied in PidListToPattern()" if ! defined $szPidList;

  $szPidList =~ s/,/|/g; $szPidList =~ s/\s+//g;

  return $szPidList;
}

#-----------------------------------------------------------------------
# Parse command line
#-----------------------------------------------------------------------
sub ParseCmdLine
{
  my %Options;
  my $Var;

  die("Flag is not supported")
    if ! Getopt::Std::getopts('i:c:Ct:H:s:z:Z:p:P:EqQwWlLobAgah', \%Options);
  usage() if exists $Options{h};

  $g_rhScriptVars->{ORACLE_SID} = lc($Options{i}) if exists $Options{i};
  die "ORACLE_SID is NOT defined" if ! defined $g_rhScriptVars->{ORACLE_SID};

  die "Options -c and -C are mutually exclusive"
    if exists $Options{c} and exists $Options{C};
  $g_rhScriptVars->{ORACLE_CON} = $Options{c} if exists $Options{c};  
  $g_rhScriptVars->{ORACLE_CON} = undef if exists $Options{C};  

  $g_rhScriptVars->{F_TOP_PROCESSES} = $Options{t} if exists $Options{t};
  die "-t parameter is supposed to be a positive number"
    if $g_rhScriptVars->{F_TOP_PROCESSES} !~ /^\d+$/ and -1 != $g_rhScriptVars->{F_TOP_PROCESSES};

  $g_rhScriptVars->{SORT_BY} = lc($Options{s}) if exists $Options{s};
  die "Invalid sort mode: $g_rhScriptVars->{SORT_BY}"
    if $g_rhScriptVars->{SORT_BY} !~ /^cpu_util$|^cpu_pct$|^total_time$/;  
  die "Linux version only supports cpu_util sort for now"
    if 'linux' eq $^O and $g_rhScriptVars->{SORT_BY} =~ /^cpu_pct$|^total_time$/;

  if(exists $Options{a}) {
    $g_rhScriptVars->{PRINT_SQL} = 'A';
    $g_rhScriptVars->{PRINT_WAIT} = 'Y';
    $g_rhScriptVars->{PRINT_OBJ} = 'Y';
    $g_rhScriptVars->{PRINT_LONGOPS} = 'Y';
  }

  $g_rhScriptVars->{PRINT_ASH} = $Options{H} if exists $Options{H};
  die "-H parameter is supposed to be a positive number"
    if $g_rhScriptVars->{PRINT_ASH} !~ /^\d+$/;

  die "Options -q and -Q are mutually exclusive"
    if exists $Options{q} and exists $Options{Q};

  $g_rhScriptVars->{PRINT_SQL} = 'Y' if exists($Options{q});
  $g_rhScriptVars->{PRINT_SQL} = 'A' if exists($Options{Q});

  die "Options -l and -L are mutually exclusive"
    if exists $Options{l} and exists $Options{L};

  $g_rhScriptVars->{PRINT_LONGOPS} = 'Y' if exists($Options{l});
  $g_rhScriptVars->{PRINT_LONGOPS} = 'A' if exists($Options{L});

  die "Options -w and -W are mutually exclusive"
    if exists $Options{w} and exists $Options{W};

  $g_rhScriptVars->{PRINT_WAIT} = 'Y' if exists($Options{w});
  $g_rhScriptVars->{PRINT_WAIT} = 'A' if exists($Options{W});

  $g_rhScriptVars->{PRINT_OBJ} = 'Y' if exists($Options{o});

  $g_rhScriptVars->{PRINT_BLOCK} = 'Y' if exists($Options{b});

  $g_rhScriptVars->{F_ACTIVE_ONLY} = 'Y' if exists($Options{A});
  $g_rhScriptVars->{F_CLI_PATTERN} = $Options{z} if exists($Options{z});
  $g_rhScriptVars->{F_SRV_PATTERN} = $Options{Z} if exists($Options{Z});
  $g_rhScriptVars->{F_CLI_PID} = PidListToPattern($Options{p}) if exists($Options{p});
  $g_rhScriptVars->{F_SRV_PID} = PidListToPattern($Options{P}) if exists($Options{P});

  $g_rhScriptVars->{STRIP_STRINGS} = undef if exists $Options{E};

  $g_rhScriptVars->{DISABLE_COLOR} = 'Y' if exists($Options{g});

  # $ARGV[0] = wait_interval, $ARGV[1] = runs
  if(defined($ARGV[0])) {
    $g_rhScriptVars->{INTERACTIVE} = 'Y';
    $g_rhScriptVars->{WAIT_INTERVAL} = $ARGV[0];
    die "Wait interval is supposed to be a positive number"
      if $g_rhScriptVars->{WAIT_INTERVAL} !~ /^\d+$/ and -1 != $g_rhScriptVars->{WAIT_INTERVAL};

    $g_rhScriptVars->{RUN_COUNT} = defined($ARGV[1]) ? $ARGV[1] : DEFAULT_RUN_COUNT;
    die "Run count is supposed to be a positive number"
      if $g_rhScriptVars->{RUN_COUNT} !~ /^\d+$/;
  }
}

#-----------------------------------------------------------------------
# Print in color
#-----------------------------------------------------------------------
sub printf_unconditional_c {
  my $szColor = shift;
  my $szFormat = shift;
  my @aVars = @_;
  my $bDisableColor = $g_rhScriptVars->{DISABLE_COLOR};

  print color $szColor unless 'Y' eq $bDisableColor;
  printf $szFormat, @aVars;
  print color 'reset' unless 'Y' eq $bDisableColor;
} 

#-----------------------------------------------------------------------
# Fit on the Screen and Print in color
#-----------------------------------------------------------------------
sub printf_c {
  my $szColor = shift;
  my $szFormat = shift;
  my @aVars = @_;
  my ($bInteractive, $nMaxScreenLines, $nMaxScreenCols) = @$g_rhScriptVars{qw(INTERACTIVE SCREEN_LINES SCREEN_COLUMNS)};

  return -1 if 'Y' eq $bInteractive and $g_nCurrentLine > $nMaxScreenLines;

  my $szLine = sprintf $szFormat, @aVars;
  $szLine =~ s/%/%%/g;

  if('Y' eq $bInteractive and -1 != $nMaxScreenLines and -1 != $nMaxScreenCols) {
    my $nNewLines = ceil(length($szLine)/$nMaxScreenCols);
    if($g_nCurrentLine+$nNewLines > $nMaxScreenLines) {
      $szLine = substr($szLine, 1, ($nMaxScreenLines-$g_nCurrentLine)*$nMaxScreenCols);
      $g_nCurrentLine = $nMaxScreenLines;
    } else {
      $g_nCurrentLine += $nNewLines;
    }
  }

  printf_unconditional_c $szColor, $szLine;

  if('Y' eq $bInteractive and -1 != $nMaxScreenLines and -1 != $nMaxScreenCols) {
    return ($g_nCurrentLine >= $nMaxScreenLines) ? -1 : $g_nCurrentLine;
  } else {
    return 0;
  }
} 

#-----------------------------------------------------------------------
# Get "Default" Domain Name(s)
#-----------------------------------------------------------------------
sub GetDefaultDomainNames {
  my $szCmd = "cat /etc/resolv.conf | egrep \"domain|search\" | awk '{print \$2}' | sort | uniq 2>/dev/null";
  my @aDomains = split /\n/, `$szCmd`;

  return (-1 == $#aDomains) ? undef : '|.' . join '|.', @aDomains;
}

#-----------------------------------------------------------------------
# Find screen (terminal) dimensions
#-----------------------------------------------------------------------
sub FindScreenSize {
  my $szCmd = 'stty size';
  my @aT = split(/ /, `$szCmd`);

  if(1 == $#aT) {
    $g_rhScriptVars->{SCREEN_LINES} = $aT[0]-7; # 5 lines - header, 1 line - status, 1 - (reserved) status line by terminal
    $g_rhScriptVars->{SCREEN_COLUMNS} = $aT[1];
  } else {
    $g_rhScriptVars->{SCREEN_LINES} = -1;
    $g_rhScriptVars->{SCREEN_COLUMNS} = -1;
    printf_c 'REVERSE', "Problem running stty: Screen size cannot be determined\n";
  }
}

#-----------------------------------------------------------------------
# Check Prerequisites
#-----------------------------------------------------------------------
sub CheckPrerequisites {
  my $szCmd = ('aix' eq $^O) ?
    'ps -ef | fgrep ora_smon_' . $g_rhScriptVars->{ORACLE_SID} . ' | grep -v fgrep | awk \'{print $2}\'' :
    'pgrep -fx ora_smon_' . $g_rhScriptVars->{ORACLE_SID}
  ;
  my $nSmonPid = `$szCmd`;
  die "Database instance $g_rhScriptVars->{ORACLE_SID} is NOT running"
    if $nSmonPid !~ /\d+/;

  # Set autotop option to screen size if top processes is NOT requested
  FindScreenSize();
  # SCREEN_LINES, SCREEN_COLUMNS == -1 IF stty did not return valid numbers

  # We do not need to check for ORACLE connection if it is NOT requested
  if(! defined $g_rhScriptVars->{ORACLE_CON}) {
    print "Database session information has NOT been requested\n";
    return;
  }

  # Check whether ExecSql module is available and only use it then
  if(eval "require ExecSql") {
    eval "use ExecSql";
  } else {
    # If ExecSql module is not available - do NOT try to connect to ORACLE
    $g_rhScriptVars->{ORACLE_CON} = undef;
    die "Unable to connect to ORACLE. Module: ExecSql.pm is not available. Locate the module or disable with -C";
  }

  print "Gathering session statistics for database instance: $g_rhScriptVars->{ORACLE_SID}\n";

  print "Checking database connection $g_rhScriptVars->{ORACLE_CON} ...";

  my $SQL = ExecSql->connect($g_rhScriptVars->{ORACLE_CON});
  my $szInstance = $SQL->selectall_str('select instance_name from v$instance');

  if(0 == $SQL->err) {
    print " SUCCESS\n\n";
    die "It appears that ORACLE connection is made to the wrong instance. Expected: $g_rhScriptVars->{ORACLE_SID} Got: $szInstance"
      if $g_rhScriptVars->{ORACLE_SID} ne $szInstance;
  } else {
    print " FAILED\n\n";
    die($SQL->errstr);
  }

  # Check whether active session history table is present
  if($g_rhScriptVars->{PRINT_ASH} > 0) {
    my $bAshPresent = $SQL->selectall_str('select count(1) from dictionary where table_name = \'V$ACTIVE_SESSION_HISTORY\'');  
    $bAshPresent =~ s/^\s+//; $bAshPresent =~ s/\s+$//;
    if(0 == $bAshPresent) {
      warn "ASH history is requested but is NOT present in this database\n";
      $g_rhScriptVars->{PRINT_ASH} = 0;
    }
  }
  
  # Get domain names
  $g_rhScriptVars->{STRIP_STRINGS} .= GetDefaultDomainNames() if defined $g_rhScriptVars->{STRIP_STRINGS};
}


#-----------------------------------------------------------------------
# Form PS command line for AIX
#-----------------------------------------------------------------------
sub FormPsCmdLineAIX {
  my ($szSid, $szSortBy) = @$g_rhScriptVars{qw(ORACLE_SID SORT_BY)};
  my ($nTop, $szSrvPids, $szSrvPattern) = @$g_rhScriptVars{qw(F_TOP_PROCESSES F_SRV_PID F_SRV_PATTERN)};

  my $szCmd = ($szSortBy eq "cpu_util") ? "ps lgw " : "ps vgw ";
  $szCmd .= "| egrep \" oracle$szSid | ora_.\*_$szSid \"";
  # Quick and dirty initial srv pid grep, will double check later
  $szCmd .= "| egrep \"$szSrvPids\""    if defined $szSrvPids;
  # Quick and dirty initial srv pattern grep, will double check later
  $szCmd .= "| egrep \"$szSrvPattern\"" if defined $szSrvPattern;
  $szCmd .= "| grep -v egrep ";

  if($szSortBy eq "cpu_util") {
    $szCmd .= "| sort -nr +5";
  } elsif($szSortBy eq "cpu_pct") {
    $szCmd .= "| sort -nr +10";
  } elsif($szSortBy eq "total_time") {
    $szCmd .= "| sort -nr +3";
  }
  
  $szCmd .= " | head -$nTop" if -1 != $nTop;

  return $szCmd;
}

#-----------------------------------------------------------------------
# Form PS command line for Linux
#-----------------------------------------------------------------------
sub FormPsCmdLineLinux {
  my ($szSid) = @$g_rhScriptVars{qw(ORACLE_SID)};
  my ($nTop, $szSrvPids, $szSrvPattern) = @$g_rhScriptVars{qw(F_TOP_PROCESSES F_SRV_PID F_SRV_PATTERN)};

  my $szCmd = 'ps h -p $(pgrep -d, -f ora_.*${ORACLE_SID}\|oracle${ORACLE_SID}) k -cp,-pcpu -o s,pid,cputime,cp,cmd';
  # Quick and dirty initial srv pid grep, will double check later
  $szCmd .= "| egrep \"$szSrvPids\""    if defined $szSrvPids;
  # Quick and dirty initial srv pattern grep, will double check later
  $szCmd .= "| egrep \"$szSrvPattern\"" if defined $szSrvPattern;
  $szCmd .= "| grep -v egrep ";

  $szCmd .= " | head -$nTop" if -1 != $nTop;

  return $szCmd;
}

#-----------------------------------------------------------------------
# Get process data for requested instance from PS command
#-----------------------------------------------------------------------
sub GetPsData {
  my ($szSid, $szSortBy, $szStrip) = @$g_rhScriptVars{qw(ORACLE_SID SORT_BY STRIP_STRINGS)};
  my ($nTop, $szSrvPids, $szSrvPattern) = @$g_rhScriptVars{qw(F_TOP_PROCESSES F_SRV_PID F_SRV_PATTERN)};

  my $szCmd = ('aix' eq $^O) ? FormPsCmdLineAIX() : FormPsCmdLineLinux();

  my @aProc = split /\n/, `$szCmd`;
  my $rhProc = {};

  foreach $_ (@aProc) {
    # Strip excessive command info
    s/$szStrip//g;
    s/\s+/ /g;

    my @aProcOpts = split /\s+/;
    my ($nPid, $chStatus, $szTime, $szCpu, $szCmd) =
      ('aix' eq $^O) ?
        ($szSortBy =~ /cpu_pct|total_time/) ? @aProcOpts[1,3,4,-3,-1] : @aProcOpts[3,1,-2,5,-1] :
        @aProcOpts[1,0,2,3,4]
    ;

    next if defined $szSrvPids and $nPid !~ m/$szSrvPids/;         # Double checking PIDs
    next if defined $szSrvPattern and $szCmd !~ m/$szSrvPattern/;  # Double checking pattern

    $rhProc->{$nPid}->{ps}->{cmd} = $szCmd;
    $rhProc->{$nPid}->{ps}->{status} = $chStatus;
    $rhProc->{$nPid}->{ps}->{total_time} = $szTime;
    $rhProc->{$nPid}->{ps}->{cpu} = $szCpu;
  }

  return $rhProc;
}

#-----------------------------------------------------------------------
# Run SQL (selectall_hashref) and return results
#-----------------------------------------------------------------------
sub RunSql {
  my $szSql = shift;
  my $SQL = ExecSql->connect($g_rhScriptVars->{ORACLE_CON});

  my $raSql = $SQL->selectall_hashref($szSql);
  if(0 != $SQL->err) {
    print "$szSql\n";
    die "SQL error: " . $SQL->errstr;
  }

  $SQL->disconnect();

  return $raSql;
}

#-----------------------------------------------------------------------
# Get ORACLE Session data
#-----------------------------------------------------------------------
sub SqlSessionData {
  my $szServerPids = shift;
  my $nTop = $g_rhScriptVars->{F_TOP_PROCESSES};

  return {} if -1 != $nTop and ! not_empty($szServerPids);

  my $szWherePids = (-1 == $nTop) ? "1 = 1" : "p.spid IN ($szServerPids)";

  my $szSql = <<EOM ;
SELECT p.spid, s.sid, s.serial\# AS ser, s.process, s.osuser||'\@'||s.machine||' ['||s.username||']: '||nvl(s.process, '?process')||'->'||nvl(s.program, '?client') AS client,
  to_char(s.logon_time, 'HH24:MI:SS MM/DD') AS logon_time, substr(s.status, 1, 1) AS s,
  s.row_wait_obj\# as object,
  w.event, w.seconds_in_wait, w.state, decode(s.sql_hash_value, 0, -1*s.prev_hash_value, s.sql_hash_value) AS sql_hash_value
FROM v\$session s, v\$session_wait w, v\$process p
WHERE s.sid = w.sid
  AND s.paddr = p.addr
  AND s.type='USER'
  AND $szWherePids
UNION ALL
SELECT p.spid, s.sid, s.serial\# AS ser, s.process, 'BACKGROUND: '||b.name||' '||b.description AS client,
  to_char(s.logon_time, 'HH24:MI:SS MM/DD') AS logon_time, substr(s.status, 1, 1) AS s,
  s.row_wait_obj\# as object,
  w.event, w.seconds_in_wait, w.state, decode(s.sql_hash_value, 0, -1*s.prev_hash_value, s.sql_hash_value) AS sql_hash_value
FROM v\$session s, v\$session_wait w, v\$bgprocess b, v\$process p
WHERE s.sid = w.sid
  AND s.paddr = b.paddr
  AND b.paddr = p.addr
  AND s.type='BACKGROUND'
  AND $szWherePids

EOM

  return RunSql($szSql);
}

#-----------------------------------------------------------------------
# Get ORACLE Object data
#-----------------------------------------------------------------------
sub SqlObjectData {
  my $szObjectIds = shift;

  return {} if ! not_empty($szObjectIds);

  my $szSql = <<EOM ;
SELECT object_id, decode(o.object_type, 'LOB', 
  (SELECT 'LOB:'||l.owner||'.'||l.table_name||'.'||l.column_name FROM dba_lobs l WHERE l.segment_name=o.object_name), 
  o.object_type||':'||o.owner||'.'||o.object_name) AS object
FROM dba_objects o
WHERE o.object_id IN ($szObjectIds)

EOM

  return RunSql($szSql);
}

#-----------------------------------------------------------------------
# Get Active SQL Statements
#-----------------------------------------------------------------------
sub SqlActiveSqlData {
  my $szSqlHashes = shift;

  return {} if ! not_empty($szSqlHashes);

  my $szSql = <<EOM ;
SELECT s.hash_value, s.sql_text
FROM v\$sql s
WHERE s.hash_value IN ($szSqlHashes)

EOM

  return RunSql($szSql);
}

#-----------------------------------------------------------------------
# Get ORACLE Blocking Session data
#-----------------------------------------------------------------------
sub SqlBlockingSessions {
  my $szSids = shift;
  my $nTop = $g_rhScriptVars->{F_TOP_PROCESSES};

  return {} if -1 != $nTop and ! not_empty($szSids);

  my $szWhereSids = (-1 == $nTop) ? "1 = 1" : "session_id IN ($szSids)";

  my $szSql = <<EOM ;
SELECT s.sid, s.serial\# AS ser, s.osuser||'\@'||s.machine||' ['||s.username||']: '||nvl(s.process, '?process')||'->'||nvl(s.program, '?client') AS client,
  decode(l.lmode, 0, 'None', 1, 'NULL', 2, 'row-S', 3, 'row-X', 4, 'S', 5, 'SSX', 6, 'X') as lmode,
  l.type, s.row_wait_obj\# as object,
  w.event, w.seconds_in_wait, w.state, s.sql_hash_value
FROM v\$lock l, v\$session s, v\$session_wait w
WHERE l.sid = s.sid
  AND s.sid = w.sid
  AND l.request = 0
  AND s.type = 'USER'
  AND (l.id1, l.id2, l.type) IN (SELECT id1, id2, type FROM v\$lock WHERE request > 0 AND $szWhereSids)
UNION ALL
SELECT s.sid, s.serial\# AS ser, 'BACKGROUND: '||b.name||' '||b.description AS client,
  decode(l.lmode, 0, 'None', 1, 'NULL', 2, 'row-S', 3, 'row-X', 4, 'S', 5, 'SSX', 6, 'X') as lmode,
  l.type,
  s.row_wait_obj\# as object,
  w.event, w.seconds_in_wait, w.state, s.sql_hash_value
FROM v\$lock l, v\$session s, v\$session_wait w, v\$bgprocess b
WHERE l.sid = s.sid
  AND s.sid = w.sid
  AND s.paddr = b.paddr
  AND l.request = 0
  AND s.type = 'BACKGROUND'
  AND (l.id1, l.id2, l.type) IN (SELECT id1, id2, type FROM v\$lock WHERE request > 0 AND $szWhereSids)

EOM

  return RunSql($szSql);
}

#-----------------------------------------------------------------------
# Get ORACLE ASH data
#-----------------------------------------------------------------------
sub SqlAshData {
  my $szSids = shift;
  my ($nTop, $nTopAsh) = @$g_rhScriptVars{qw(F_TOP_PROCESSES PRINT_ASH)};

  return {} if -1 != $nTop and ! not_empty($szSids);

  my $szWhereSids = "session_id IN ($szSids)";

  my $szSql  = "
WITH target_data AS (
  SELECT session_id AS sid, session_serial\# AS ser, event, sum(time_waited)/1000000 AS seconds_waited, row_number() OVER (PARTITION BY session_id ORDER BY sum(time_waited) DESC) AS rn
  FROM v\$active_session_history
  WHERE time_waited > 0
    AND $szWhereSids
  GROUP BY session_id, session_serial\#, event
  HAVING sum(time_waited) > 0
) SELECT sid, ser, event, lpad(to_char(seconds_waited), length('seconds_waited')) AS seconds_waited FROM target_data WHERE rn <= $nTopAsh
";

  return RunSql($szSql);
}

#-----------------------------------------------------------------------
# Get ORACLE longops data
#-----------------------------------------------------------------------
sub SqlLongopsData {
  my $szSids = shift;
  my ($nTop, $bPrintLongops) = @$g_rhScriptVars{qw(F_TOP_PROCESSES PRINT_LONGOPS)};

  return {} if -1 != $nTop and ! not_empty($szSids);

  my $szWhereSids = (-1 == $nTop) ? "1 = 1" : " sid IN ($szSids)";
  my $szWhereRunning = ('A' eq $bPrintLongops) ? '' : ' AND time_remaining > 0';

  my $szSql  = <<EOM ;
SELECT sid, serial\# AS ser,
  decode(totalwork-sofar, 0, 'COMPLETED: ', 'RUNNING: ')||opname||' '||nvl(target, '')||
    ' Elapsed: '||elapsed_seconds||' sec '||
    decode(totalwork-sofar, 
    0, 'Finished '||to_char(last_update_time, 'HH24:MI:SS MM/DD/YYYY'), 
    'Remaining: '||time_remaining||' sec Pct: '||to_char(round(sofar/totalwork*100, 2))) AS longops_info
FROM v\$session_longops
WHERE $szWhereSids $szWhereRunning

EOM

  return RunSql($szSql);
}

#-----------------------------------------------------------------------
# MAIN Procedure: Get session, SQL, wait etc data from ORACLE instance
#-----------------------------------------------------------------------
sub GetOracleInstanceData {
  my $rhProc = shift;
  my ($szOraCon, $nTop, $bPrintBlock, $bPrintSql, $bPrintAsh, $bPrintLongops) =
    @$g_rhScriptVars{qw(ORACLE_CON F_TOP_PROCESSES PRINT_BLOCK PRINT_SQL PRINT_ASH PRINT_LONGOPS)};

  return $rhProc if ! defined $szOraCon;

  my $szServerPids = (-1 != $nTop) ? join ",", (sort keys %$rhProc) : 0;

  my $raSql = SqlSessionData($szServerPids);

  foreach my $rhRow (@$raSql) {
    foreach my $szM (ORACLE_CPU_METRICS) {
      $rhProc->{$rhRow->{SPID}}->{oracle}->{$szM} = $rhRow->{uc($szM)};
    }
  }

  # Calculate SID list
  my @aSidList = (); my $rhSidMap = {}; my $szSidList = undef;
  map {
    push @aSidList, $rhProc->{$_}->{oracle}->{sid};
    $rhSidMap->{$rhProc->{$_}->{oracle}->{sid}} = $_;
  } grep {element_defined($rhProc->{$_}, 'oracle')} keys %$rhProc;
  $szSidList = join ",", (sort {$a <=> $b} @aSidList);

  # Query blocking sessions
  if('Y' eq $bPrintBlock) {
    $raSql = SqlBlockingSessions($szSidList);
    foreach my $rhRow (@$raSql) {
      foreach my $szM (ORACLE_BLOCKER_METRICS) {
        $rhProc->{$rhSidMap->{$rhRow->{SID}}}->{oracle}->{blocker}->{$szM} = $rhRow->{uc($szM)};
      }
    }
  }

  # Query object names (we could do it in the original SQL, but the performance kind of sucks ...)
  my $rhObjectIds = {};
  foreach my $nPid (keys %$rhProc) {
    my $nObj = $rhProc->{$nPid}->{oracle}->{object};
    if(element_not_empty($rhProc->{$nPid}->{oracle}, 'object')) {
      if((0 != $nObj) and (-1 != $nObj)) {
        $rhObjectIds->{$nObj} = [] if ! exists($rhObjectIds->{$nObj});
        push @{$rhObjectIds->{$nObj}}, $nPid;
      } else {
        # Replace 0 and -1 with empty strings as these are not real object names
        $rhProc->{$nPid}->{oracle}->{object} = "";
      }
    }
  }
  if(0 != scalar keys %$rhObjectIds) {
    my $szObjectIds = join ",", sort keys %$rhObjectIds;

    $raSql = SqlObjectData($szObjectIds);

    foreach my $rhRow (@$raSql) {
      foreach my $nPid (@{$rhObjectIds->{$rhRow->{OBJECT_ID}}}) {
        $rhProc->{$nPid}->{oracle}->{object} = $rhRow->{OBJECT};
      }
    }
  }

  # Query active SQLs (again, we do it separately to save performance)
  if('N' ne $bPrintSql) {
    my $rhSqlHashes = {};
    foreach my $nPid (grep {element_not_empty($rhProc->{$_}->{oracle}, 'sql_hash_value') and $rhProc->{$_}->{oracle}->{sql_hash_value} ne '0'} keys %$rhProc) {
      my $ora = $rhProc->{$nPid}->{oracle};

      if($ora->{sql_hash_value} > 0) {
        $rhSqlHashes->{$ora->{sql_hash_value}} = {pid => $nPid, type => 'current'};
      } elsif('A' eq $bPrintSql and $ora->{sql_hash_value} < 0) {
        $rhSqlHashes->{-1*$ora->{sql_hash_value}} = {pid => $nPid, type => 'previous'};
      }
    }

    if(0 != scalar keys %$rhSqlHashes) {
      my $szSqlHashes = join ",", sort keys %$rhSqlHashes;

      $raSql = SqlActiveSqlData($szSqlHashes);
      foreach my $rhRow (@$raSql) {
        my $s = $rhSqlHashes->{$rhRow->{HASH_VALUE}};
        $rhProc->{$s->{pid}}->{oracle}->{sql_text} = (('previous' eq $s->{type}) ? '[PREV] ' : '') . $rhRow->{SQL_TEXT};
      }
    }
  }

  # Query ASH data
  if($bPrintAsh > 0) {
    $raSql = SqlAshData($szSidList);

    foreach my $rhRow (grep {exists($rhSidMap->{$_->{SID}})} @$raSql) {
      my %hMRow = map {(lc($_), $rhRow->{$_})} keys %$rhRow;
      push @{$rhProc->{$rhSidMap->{$rhRow->{SID}}}->{oracle}->{ash}}, \%hMRow
        if $rhProc->{$rhSidMap->{$rhRow->{SID}}}->{oracle}->{ser} == $rhRow->{SER};
    }
  }

  # Query Longops data
  if('N' ne $bPrintLongops) {
    $raSql = SqlLongopsData($szSidList);

    foreach my $rhRow (grep {exists($rhSidMap->{$_->{SID}})} @$raSql) {
      foreach my $szM (ORACLE_LONGOPS_METRICS) {
        $rhProc->{$rhSidMap->{$rhRow->{SID}}}->{oracle}->{longops}->{$szM} = $rhRow->{uc($szM)}
          if $rhProc->{$rhSidMap->{$rhRow->{SID}}}->{oracle}->{ser} == $rhRow->{SER};
      }
    }
  }

  return $rhProc;
}

#-----------------------------------------------------------------------
# Preprocess Data: Strip excessive info from "client application" field,
#   "identify" idle shared servers and dispatchers ets
#-----------------------------------------------------------------------
sub PreprocessData {
  my $rhProc = shift;
  my ($szStrip, $szSid, $szSortBy) = @$g_rhScriptVars{qw(STRIP_STRINGS ORACLE_SID SORT_BY)};

  foreach my $nPid (keys %$rhProc) {
    my ($ora, $ps) = ($rhProc->{$nPid}->{oracle}, $rhProc->{$nPid}->{ps});

    # Strip excessive client info
    $ora->{client} =~ s/$szStrip//g if exists $ora->{client};

    # Transofrm time to minutes for total_time comparison if requested
    if('total_time' eq $szSortBy) {
      my ($nHours, $nMins) = split /:/, $ps->{total_time};
      $ps->{total_minutes} = defined($nMins) ? $nHours*60 + $nMins: 0;
    }

    $ora->{blocker}->{client} =~ s/$szStrip//g if exists $ora->{blocker};

    # "Identify" idle dispatchers and shared servers
    $ora->{client} = 'IDLE: Dispatcher #' . int($1)
      if ! exists($ora->{client}) and $ps->{cmd} =~ /ora_d(\d{3,3})_$szSid/;
    $ora->{client} = 'IDLE: Shared Server #' . int($1)
      if ! exists($ora->{client}) and $ps->{cmd} =~ /ora_s(\d{3,3})_$szSid/;
  }

  return $rhProc;
}

#-----------------------------------------------------------------------
# Print Report Header - ORACLE Info included
#-----------------------------------------------------------------------
sub PrintReportHeaderOracle {
  print <<EOM;
----------------------------------------------- --------------------------------------------------------------------------------------------------------
                  UNIX                                                                          ORACLE
-------- - ---- ---------- -------------------- ------ - -------------- --------------------------------------------------------------------------------
  PID    S  Cpu Total Time        Command        SID   S   Logon Time                                 Client Application                                      
-------- - ---- ---------- -------------------- ------ - -------------- --------------------------------------------------------------------------------
EOM
}

#-----------------------------------------------------------------------
# Print Report Header - No ORACLE Info
#-----------------------------------------------------------------------
sub PrintReportHeaderNoOracle {
  print <<EOM;
-----------------------------------------------
                  UNIX                   
-------- - ---- ---------- --------------------
  PID    S  Cpu Total Time       Command    
-------- - ---- ---------- --------------------
EOM
} 


#-----------------------------------------------------------------------
# Get a string of active filters
#-----------------------------------------------------------------------
sub GetActiveFilters {
  return ' N/A' if ! defined $g_rhScriptVars->{ORACLE_CON};

  my ($bActive, $nTop, $szSrvPat, $szSrvPids, $szCliPat, $szCliPids) = 
    @$g_rhScriptVars{qw(F_ACTIVE_ONLY F_TOP_PROCESSES F_SRV_PATTERN F_SRV_PID F_CLI_PATTERN F_CLI_PID)};

  my $szFilters = '';

  $szFilters .= "ACTIVE: Y" if 'Y' eq $bActive;
  $szFilters .= " TOP: $nTop" if -1 != $nTop;
  $szFilters .= " SERVER: $szSrvPat" if not_empty($szSrvPat);
  $szFilters .= " SERVER PIDS: " . ( (length($szSrvPids) > 10) ? substr($szSrvPids, 0, 10) . ".." : $szSrvPids )
    if not_empty($szSrvPids);
  $szFilters .= " CLIENT: $szCliPat" if not_empty($szCliPat);
  $szFilters .= " CLIENT PIDS: " . ( (length($szCliPids) > 10) ? substr($szCliPids, 0, 10) . ".." : $szCliPids )
    if not_empty($szCliPids);

  return nvl($szFilters, ' NONE');
}

#-----------------------------------------------------------------------
# Print Process/Session Report
#-----------------------------------------------------------------------
sub PrintReport {
  my $rhProc = shift;
  # Parameters
  my ($szOraCon, $nMaxScreenLines, $bInteractive, $szSortBy) = 
    @$g_rhScriptVars{qw(ORACLE_CON SCREEN_LINES INTERACTIVE SORT_BY)};
  # Additional display
  my ($bPrintSql, $bPrintWait, $bPrintObj, $bPrintBlock, $bPrintAsh, $bPrintLongops) = 
    @$g_rhScriptVars{qw(PRINT_SQL PRINT_WAIT PRINT_OBJ PRINT_BLOCK PRINT_ASH PRINT_LONGOPS)};
  # Filters
  my ($nTop, $bActiveOnly, $szCliPattern, $szCliPids) = 
    @$g_rhScriptVars{qw(F_TOP_PROCESSES F_ACTIVE_ONLY F_CLI_PATTERN F_CLI_PID)};

  my $szIdleWaits = join '|', (ORACLE_IDLE_WAITS);
  $szIdleWaits .= "|idle wait"; # Some wait event names are very straightforward ...

  # Sort blocks (different for time/cpu)
  my $rfSort = ('total_time' eq $szSortBy) ? 
    sub {$rhProc->{$b}->{ps}->{total_minutes} <=> $rhProc->{$a}->{ps}->{total_minutes}} :
    sub {$rhProc->{$b}->{ps}->{cpu} <=> $rhProc->{$a}->{ps}->{cpu}}
  ;

  my $cProc = 0; my $cMaxProc = 0; $g_nCurrentLine = 0;

  if(defined($szOraCon)) {
    # Get Relevant Processes - discard those that do NOT satisfy requested conditions
    my @aRelevantIds = 
      grep {
        my ($ps, $ora) = ($rhProc->{$_}->{ps}, $rhProc->{$_}->{oracle});

        # Do we have basic information
        defined($ps) and defined($ora)

        # Have we collected relevant information for report ?
        and defined(("total_time" eq $szSortBy) ? $ps->{total_time} : $ps->{cpu})
        
        # Restricting by client pattern, if requested
        and (! defined($szCliPattern) or (element_not_empty($ora, 'client') and $ora->{client} =~ /$szCliPattern/))

        # Restricting by client pids, if requested
        and (! defined($szCliPids) or (element_not_empty($ora, 'process') > 0 and $ora->{process} =~ /$szCliPids/))

        # Restricting by "active sessions" only if requested
        and ('N' eq $bActiveOnly or (
          ( ('N' ne $bPrintSql) and element_not_empty($ora, 'sql_text') )
          or (('N' ne $bPrintWait) and element_not_empty($ora, 'event') 
            and (('A' eq $bPrintWait) or ($ora->{event} !~ /$szIdleWaits/i) ) )
          or (('Y' eq $bPrintObj) and element_not_empty($ora, 'object') )
          or (('Y' eq $bPrintBlock) and element_defined($ora, 'blocker') )
          or (($bPrintAsh > 0) and element_defined($ora, 'ash') )
          )
        )
      } keys %$rhProc;

    PrintReportHeaderOracle();

    $cMaxProc = $#aRelevantIds+1;
    foreach my $nPid (sort $rfSort @aRelevantIds) {
      $cProc++;

      my ($ps, $ora) = ($rhProc->{$nPid}->{ps}, $rhProc->{$nPid}->{oracle});
      my $szFormat = ('cpu_util' eq $szSortBy) ?
        "%8d %1s %4d %10s %-20s %6s %1s %14s %-30s\n" :
        "%8d %1s %4.2f %10s %-20s %6s %1s %14s %-30s\n";
      last if -1 == printf_c 'RESET', $szFormat,
        $nPid,
        $ps->{status}, $ps->{cpu}, $ps->{total_time}, $ps->{cmd},
        nvl($ora->{sid}), nvl($ora->{s}), nvl($ora->{logon_time}), nvl($ora->{client})
      ;

      # Print current SQL
      if('N' ne $bPrintSql and element_not_empty($ora, 'sql_text')) {
        last if -1 == printf_c 'GREEN', "   ->SQL: %-60s\n", $ora->{sql_text};
      }

      # Print Current Wait
      if(element_not_empty($ora, 'event')) {
        if(('A' eq $bPrintWait) or (('Y' eq $bPrintWait) and ($ora->{event} !~ /$szIdleWaits/i))) {
          last if -1 == printf_c 'CYAN', "   ->WAIT: %-60s\n", $ora->{event} . ' [' . $ora->{state} . ']: ' . $ora->{seconds_in_wait};
        }
      }

      # Print ASH waits
      if ($bPrintAsh > 0 and element_defined($ora, 'ash')) {
        foreach my $w (sort {$b->{seconds_waited} <=> $a->{seconds_waited}} @{$ora->{ash}}) {
          last if -1 == printf_c 'MAGENTA', "     ->ASH WAIT: %-60s\n", "$w->{event}: $w->{seconds_waited}";
        }
      }

      # Print Current Object
      if('Y' eq $bPrintObj and element_not_empty($ora, 'object')) {
        last if -1 == printf_c 'YELLOW', "   ->OBJ: %-60s\n", $ora->{object};
      }

      # Print Blocking Sessions
      if ('Y' eq $bPrintBlock and element_defined($ora, 'blocker')) {
        my $b = $ora->{blocker};
        last if -1 == printf_c 'RED', "   ->BLOCKING SESSION: %-60s\n", "$b->{client} [$b->{ty}:$b->{lmode}]";
      }

      # Print longops 
      if ('N' ne $bPrintLongops and element_defined($ora, 'longops')) {
        my $l = $ora->{longops};
        last if -1 == printf_c 'WHITE', "   ->LONGOP: %-60s\n", "$l->{longops_info}";
      }
    }
  } else {
    $cMaxProc = scalar keys %$rhProc;

    PrintReportHeaderNoOracle();

    foreach my $nPid (sort $rfSort keys %$rhProc) {
      my $ps = $rhProc->{$nPid}->{ps};
      my $szFormat = ('cpu_util' eq $szSortBy) ?
        "%8d %1s %4d %10s %-20s\n" :
        "%8d %1s %4.2f %10s %-20s\n";

      $cProc++;
      last if -1 == printf_c 'RESET', $szFormat,
        $nPid,
        nvl($ps->{status}), nvl($ps->{cpu}), nvl($ps->{total_time}), nvl($ps->{cmd})
      ;
    }
  }

  # Print Status Line
  printf_unconditional_c 'REVERSE',
    "%-120s", " ... $cProc top processes are displayed ... Skipped: " . ($cMaxProc-$cProc) . " Filters:" . GetActiveFilters() . ' ';
  printf_unconditional_c 'RESET', " %-30s", STATUS_WAITING if 'Y' eq $bInteractive;
}

#################### MAIN PROGRAM BEGINS HERE #########################################################

PrintHeader();
die "Sorry, this tool only supports AIX and Linux" if $^O !~ /aix|linux/;
ParseCmdLine();
CheckPrerequisites();

print $CURSOR_HIDE if 'Y' eq $g_rhScriptVars->{INTERACTIVE};

for(my $nRun = 0; $nRun < $g_rhScriptVars->{RUN_COUNT}; $nRun++) {
  printf_unconditional_c 'RESET', "\b" x 31 . " %-30s", STATUS_WORKING
    if 'Y' eq $g_rhScriptVars->{INTERACTIVE} and $nRun > 0;

  my $rhProc = GetPsData();
  $rhProc = GetOracleInstanceData($rhProc) if defined $g_rhScriptVars->{ORACLE_CON};
  $rhProc = PreprocessData($rhProc);

#  print Dumper($rhProc) . "\n";

  system 'clear'
    if 'Y' eq $g_rhScriptVars->{INTERACTIVE} and $g_rhScriptVars->{RUN_COUNT} > 1;
  PrintReport($rhProc);

  sleep($g_rhScriptVars->{WAIT_INTERVAL})
    if 'Y' eq $g_rhScriptVars->{INTERACTIVE} and $g_rhScriptVars->{RUN_COUNT} > 1;
}

print $CURSOR_SHOW if 'Y' eq $g_rhScriptVars->{INTERACTIVE};
print "\n";
