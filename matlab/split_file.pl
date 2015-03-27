#!/usr/bin/perl

use strict;
use warnings;
use Getopt::Long;
use File::Basename;
use POSIX;
use Cwd;

my $numlines;
my $cutpoint;
my $cmd;
my $numargs = $#ARGV+1;

if ($numargs != 2) {
    print "Usage: split_file.pl <original file> <no. of splits>\n";
    exit;
}
my $infile = $ARGV[0];
my $numfiles = $ARGV[1];

my ($basename,$path,$ext) = fileparse($infile,qr/\.[^.]*/);

$numlines = `/usr/local/bin/numlines $infile`; chomp $numlines;
$cutpoint = ceil($numlines/$numfiles);

`rm -f input/*`;
foreach my $i (1 .. $numfiles) {
    my $begin = ($i-1)*$cutpoint + 1;
    my $end = $begin + $cutpoint - 1;
    if ($end > $numlines) {
	$end = $numlines;
    }
    $cmd = "sed -n '$begin,${end}p' $infile > input/${basename}${i}${ext}";
    print "$cmd\n";
    system($cmd);
}

