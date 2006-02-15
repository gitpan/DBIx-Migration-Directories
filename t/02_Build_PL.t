#!/usr/bin/perl

use strict;
use warnings;
use Test::More;
use Test::CPANpm;

plan tests => 2;

local $ENV{AUTOMATED_TESTING};

my $oldin;
open($oldin, "<&STDIN");
close(STDIN);

our @deps = qw(DBIx::Transaction File::Basename::Object Pod::Usage DBI Class::Driver Module::Build Data::Dumper);

$ENV{AUTOMATED_TESTING} = 0;
cpan_depends_ok(\@deps, "DBD::SQLite2 is not required by default");

$ENV{AUTOMATED_TESTING} = 1;
cpan_depends_ok(
    [ @deps, "DBD::SQLite2" ],
    "DBD::SQLite2 is required when doing automated testing"
);
