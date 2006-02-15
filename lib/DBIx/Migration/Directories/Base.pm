#!perl

package DBIx::Migration::Directories::Base;

use strict;
use warnings;
use Carp qw(croak);
use Class::Driver;
use base q(Class::Driver);

our $number = qr{[0-9]+(?:\.[0-9]+)?};

return 1;

sub driver_required { 0; }

sub driver_required_here { 0; }

sub new {
    my($class, %args) = @_;
    ($class, %args) = ($class->set_preinit_defaults(%args));
    if(my $self = $class->driver_load($args{driver}, %args)) {
        $self->set_postinit_defaults();
        return $self;
    } else {
        return;
    }
}

sub set_postinit_defaults {}

sub set_preinit_defaults {
    my($class, %args) = @_;
    $class = ref($class) if ref($class);
    
    croak qq{$class\->new\() requires "dbh" parameter}
        unless defined $args{dbh};

    $args{driver} = $args{dbh}->{Driver}->{Name}
        unless($args{driver});
        
    return($class, %args);
}

sub driver_new {
    my($class, %args) = @_;
    my $self = bless \%args, $class;
    return $self;
}

sub read_file {
    my($self, $file) = @_;
    if(open(my $fh, '<', $file)) {
        my $data = join('', <$fh>);
        close($fh);
        return $data;
    } else {
        croak qq{open("$file") failed: $!};
    }
}

sub direction {
    my($self, $from, $to) = @_;
    return $to <=> $from;
}

sub version_as_number {
    my($self, $version) = @_;
    return ($version || 0) + 0;
}

sub versions {
    my($self, $string) = @_;
    if($string =~ m{^($number)$}) {
        return($self->version_as_number(0), $self->version_as_number($1));
    } elsif($string =~ m{^($number)-($number)$}) {
        return($self->version_as_number($1), $self->version_as_number($2));
    } else {
        return;
    }
}

sub run_sql {
    my($self, @sql) = @_;
    my $dbh = $self->{dbh};
    
    return $dbh->transaction(sub {
        my $marker = '';
        my $good = 1;
        my $qn = 0;
        
        while($good && (my $query = shift(@sql))) {
            if(ref($query)) {
                $marker = $$query;
                $qn = 0;
            } else {
                $qn++;
                eval { $good = $dbh->do($query); };
            
                if($@) {
                    die "[$marker#$qn]$@";
                } elsif(!$good) {
                    $dbh->set_err(undef, '');
                    $dbh->set_err(
                        $dbh->err,
                        join('', "[$marker#$qn] ", $dbh->errstr || ''),
                        $dbh->state
                    );
                }
            }
        }
        
        return $good;
    });
}

sub sql_insert_migration_schema_version {
    my($self, $myschema, $to) = @_;
    return sprintf(
        q{INSERT INTO migration_schema_version (name, version) VALUES (%s, %f)},
        $self->{dbh}->quote($myschema), $to
    );
}

sub sql_update_migration_schema_version {
    my($self, $myschema, $to) = @_;
    return sprintf(
        q{UPDATE migration_schema_version SET version = %f WHERE name = %s},
        $to, $self->{dbh}->quote($myschema)
    )
}

sub sql_insert_migration_schema_log {
    my($self, $myschema, $from, $to) = @_;
    return sprintf(
        q{
            INSERT INTO migration_schema_log 
                (schema_name, event_time, old_version, new_version)
            VALUES (%s, now(), %f, %f)
        },
        $self->{dbh}->quote($myschema), $from, $to
    );
}

sub sql_table_exists {
    my($self, $table) = @_;
    return sprintf(
        q{SELECT 1 FROM information_schema.tables WHERE table_name = %s},
        $self->{dbh}->quote($table)
    );
}

sub table_exists {
    my($self, $table) = @_;
    
    my $dbh = $self->{dbh};
    my $rv;
    $dbh->begin_work;
    my $query = $self->sql_table_exists($table);
    my $sth = $dbh->prepare($query);
    if($sth->execute()) {
        if($sth->fetchrow_arrayref()) {
            $rv = 1;
        } else {
            $rv = 0;
        }
        $sth->finish();
        if($dbh->transaction_error) {
            $dbh->rollback();
        } else {
            $dbh->commit();
        }
    } else {
        my $err = $dbh->errstr;
        $dbh->rollback();
        warn "table_exists query $query failed: $err";
        $rv = undef;
    }
    return $rv;
}

sub schema_version_log {
    my $self = shift;
    my $myschema = shift || $self->{schema} ||
        croak "schema_version_log() called without a schema name";

    my $dbh = $self->{dbh};
    $dbh->begin_work;
    if($self->table_exists('migration_schema_log')) {
        if(my $sth = $dbh->prepare_cached(q{
            SELECT
                schema_name, event_time, old_version, new_version
            FROM
                migration_schema_log
            WHERE
                schema_name = ?
            ORDER BY
                id
        })) {
            if($sth->execute($myschema)) {
                if(my $result = $sth->fetchall_arrayref({})) {
                    $sth->finish();
                    $dbh->commit();
                    return $result;
                } else {
                    $sth->finish();
                    $dbh->rollback();
                    return;
                }
            }
        } else {
            my $err = $dbh->errstr;
            $dbh->rollback();
            croak "query for versions of $myschema failed: ", $err;
        }
    } else {
        $dbh->commit();
        return;
    }
}

sub schemas {
    my $self = shift;
    my $dbh = $self->{dbh};
    $dbh->begin_work;
    if($self->table_exists('migration_schema_version')) {
        if(my $sth = $dbh->prepare_cached(
            "SELECT * FROM migration_schema_version"
        )) {
            if($sth->execute()) {
                if(my $result = $sth->fetchall_hashref('name')) {
                    $sth->finish;
                    $dbh->commit;
                    return $result;
                } else {
                    $sth->finish;
                    $dbh->rollback;
                    return;
                }
            
            } else {
                my $err = $dbh->errstr;
                $dbh->rollback;
                croak "Failed to run query to obtain schemas: $err";
            }
        } else {
            my $err = $dbh->errstr;
            $dbh->rollback;
            croak "Failed to prepare query to obtain schemas: $err";
        }
    } else {
        $dbh->commit;
        return;
    }
}

sub require_schema {
    my($self, $schema, $version) = @_;
    my $schemas = $self->schemas;
    die qq{Schema "$schema" not installed!\n}
        unless($schemas->{$schema});
    if($version) {
        die qq{Schema "$schema" is version $schemas->{$schema}{version}, we want $version.\n}
            unless($schemas->{$schema}{version} == $version);
    }
    return 1;
}

__END__

=pod

=head1 NAME

DBIx::Migration::Directories::Base - Schema-independant migration operations

=head1 SYNOPSIS

  my $object = DBIx::Migration::Directories::Base->new(
    $dbh => $some_database_handle
  );

  my $schemas = $object->schemas;
  
  if(my $schema = $schemas->{'Foo-Schema'}) {
     print "Foo-Schema is installed at version #$schema->{version}.\n";
  }

=head1 DESCRIPTION

C<DBIx::Migration::Directories::Base> is the base class to
C<DBIx::Migration::Directories>.

The methods in this class do not care if you are currently operating on
a schema, or if you have a valid schema directory to work with.

The main reason to create C<DBIx::Migration::Directories::Base> object
on it's own is to obtain general information about migrations, such as
currently installed schemas and their version history.

=head1 METHODS

=head2 Constructor

=over

=item new(%args)

Creates a new DBIx::Migration::Directories::Base object. C<%args> is a
hash of properties to set on the object; the following properties are
used by C<DBIx::Migration::Directories::Base>:

=over

=item dbh

B<Required.> The C<DBIx::Transaction> database handle to use for queries.
This handle should already be connected to the database that you wish to manage.

=item driver

The name of the DBD driver we are using. You normally don't want to
specify this option; C<DBIx::Migration::Directories::Base> will automatically
pull the driver name out of the database handle you pass along.

=item schema

The schema we wish to operate on. This option is only ever used by the
L<schema_version_log|/item_schema_version_log> method, and only if you
do not send that method a schema argument.

=back

=back

=head2 High-Level Methods

=over

=item schemas

Queries the migration schema, and returns a hash reference containing
all of the schemas currently registered in this database. The keys in
this hash are the schemas' names, and the values are hash references,
containing the contents of that schema's row in the database as key/value
pairs:

=over

=item name

The schema's name, again.

=item version

The current version of this schema.

=back

=item schema_version_log($schema)

Queries the migration schema, and returns an array reference containing
the specified schema's version history. If a schema is not specified,
defaults to the "schema" property if it has been set, otherwise an
exception is raised.

Each entry in the array reference returned is a hash reference,
containing the contents of that schema's log rows in the database as
key/value pairs:

=over

=item id

A unique integer identifier for this log entry.

=item schema_name

Schema this log entry refers to.

=item event_time

Time this migration action took place.

=item old_version

Schema version before this migration action took place.

=item new_version

Schema version after this migration took place.

=back

=head2 Low-Level Methods

=over

=item table_exists($table)

Queries the database and returns 1 if the named table exists, 0 otherwise.

=item direction($from, $to)

Given two version numbers, determine whether this is an upgrade or a
downgrade. All this does is:

   $to <=> $from

=item versions($string)

Given the name of a directory, determine what versions it is migrating
from and to. Returns an array of two numbers: the "from" version and the
"two" version.

If this directory has two version numbers in it, you'll get those two
(normalized) version numbers back. If this directory only has one version
number in it, you'll get C<0> as the "from" version, and the (normalized)
version number in the directory name as the "to" version.

=item run_sql(@sql)

Begin a transaction, and run all of the SQL statements specified in @sql.
If any of them fail, roll the transaction back and return 0. If they all
succeed, commit the transaction and return 1.

=item read_file($path)

Reads a file on the filesystem and returns it's contents as a scalar.
Raises an exception if the file could not be read. Exciting, huh?

=item version_as_number($version)

Normalize a version number. Currently, this is just equivalent to:

  return $version + 0
  
But in future releases it may do fancier stuff, like dealing with "double-dot"
version numbers or the like.

=item sql_insert_migration_schema_version($schema, $version)

Returns an SQL query used to add a new schema to the database.
This is called by L</version_update_sql>.

=item sql_update_migration_schema_version($schema, $version)

Returns an SQL query used to change the version number we have
on record for a schema.
This is called by L</version_update_sql>.

=item sql_insert_migration_schema_log($schema, $from, $to)

Returns an SQL query used to add a new schema log record to the database
indicating a migration between schema versions.
This is called by L</version_update_sql>.

=item sql_table_exists($table)

Returns an SQL query used to detect if a table exists. If this query
returns any rows, it is assumed the named table exists; if there are
no rows returns, it is assumed the table does not exist.

=back

=cut
