#!perl

package DBIx::Migration::Directories::Test;

use strict;
use warnings;
use Test::More;
use DBIx::Migration::Directories;
use Carp qw(croak);
use base q(DBIx::Migration::Directories);


return 1;

sub num_tests {
    return $_[0]->{num_tests};
}

sub extra_tests {
    return $_[0]->{extra_tests} || 0;
}

sub new_test {
    my $class = shift;
    my $self = $class->SUPER::new(@_);
    $self->{num_tests} = $self->plan_tests;
    return $self;
}

sub plan_tests {
    my $self = shift;
    my $n = 2 + $self->extra_tests;
    $n += scalar @{$self->{tests}} if($self->{tests});
    return $n;
}

sub run_custom_tests {
    my $self = shift;
    my $rv = 1;
    my $ran = 0;
    if(my $tests = $self->{tests}) {
        foreach my $test (@$tests) {
            $rv = 0 unless eval { $test->($self); };
            $ran++;
            return($rv, $ran) if $@;
        }
    }
    return($rv, $ran);
}

sub run_tests {
    my $self = shift;
    my $tests = $self->num_tests;
    my $rv;
    SKIP: {
        $rv = skip(qq{"$self->{schema}" is already installed}, $tests)
            if(defined $self->get_current_version);

        $rv = 1;
        $rv = 0 unless $self->schema_full_migrates_ok;
        my($crv, $crun);
        ($crv, $crun) = $self->run_custom_tests;
        if($@) {
            warn("Main test block failed with error: $@, attempting to delete schema");
            $rv = 0;
            $self->full_delete_schema;
        } else {
            $rv = 0 unless $self->schema_deletes_ok;
        }
    }
    
    return $rv;
}

sub schema_full_migrates_ok {
    my($self, $test_name) = @_;
    my($version, $desired) = map { defined $_ ? $_ : 'unknown' }
        @{$self}{'current_version','desired_version'};
    $test_name ||= qq{schema can full migrate from $version to $desired};
    return ok($self->full_migrate, $test_name);
}

sub schema_deletes_ok {
    my($self, $test_name) = @_;
    $test_name ||= 'Schema deletes OK';
    return ok($self->full_delete_schema, $test_name);
}
