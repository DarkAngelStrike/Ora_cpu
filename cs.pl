#! /usr/bin/perl -w

# Check whether Term::ANSIColor is installed and use it if it is
if ("require Term::ANSIColor") {
   use Term::ANSIColor;
   $Term::ANSIColor::AUTORESET = 1;
} else {
   eval "sub color { return ''; };"
} 

# Terminal control code sequences
my $SHOW = "\e[?25h";   # Show cursor

print color 'clear';
print $SHOW;
