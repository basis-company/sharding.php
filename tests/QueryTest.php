<?php

namespace Basis\Sharding\Test;

use Basis\Sharding\Database;
use Basis\Sharding\Driver\Doctrine;
use Basis\Sharding\Driver\Tarantool;
use Basis\Sharding\Entity\Bucket;
use Basis\Sharding\Interface\Driver;
use PHPUnit\Framework\Attributes\DataProviderExternal;
use PHPUnit\Framework\TestCase;

class QueryTest extends TestCase
{
    #[DataProviderExternal(TestProvider::class, 'drivers')]
    public function test(Driver $driver)
    {
        $database = new Database($driver);
        if ($driver instanceof Doctrine) {
            $currentDates = $driver->query("select current_date date");
            $this->assertIsArray($currentDates);
            $this->assertCount(1, $currentDates);
            $this->assertArrayHasKey('date', $currentDates[0]);
            $this->assertSame(date('Y-m-d'), $currentDates[0]['date']);

            $tables = $driver->query("select * from pg_catalog.pg_tables");
            $this->assertNotCount(0, $tables);
            $this->assertArrayHasKey('tablename', $tables[0]);
            $tables = $driver->query("select * from pg_catalog.pg_tables where schemaname = :schema and tablename = :table", [
                'table' => $tables[0]['tablename'],
                'schema' => $tables[0]['schemaname'],
            ]);
            $this->assertCount(1, $tables);

            $tables = $driver->query(
                "select * from pg_catalog.pg_tables where schemaname = ? and tablename = ?",
                [$tables[0]['schemaname'], $tables[0]['tablename']]
            );
            $this->assertCount(1, $tables);

            $tables = $database->fetch()
                ->from($database->getBuckets(Bucket::class))
                ->query(
                    "select * from pg_catalog.pg_tables where schemaname = ? and tablename = ?",
                    [$tables[0]['schemaname'], $tables[0]['tablename']]
                );

            $this->assertCount(1, $tables);
            $this->assertArrayHasKey('tablename', $tables[0]);

            $table = $database->fetchOne()
                ->from($database->getBuckets(Bucket::class))
                ->query(
                    "select * from pg_catalog.pg_tables where schemaname = ? and tablename = ?",
                    [$tables[0]['schemaname'], $tables[0]['tablename']]
                );
            $this->assertArrayHasKey('tablename', $table);
        } elseif ($driver instanceof Tarantool) {
            [$spaces] = $driver->query("return box.space._vspace:select({box.space._vspace.id})");
            $this->assertIsArray($spaces);
            $this->assertCount(1, $spaces);
            $this->assertIsArray($spaces[0]);

            [$space] = $driver->query("return box.space._vspace:get({box.space._vspace.id}):tomap({names_only = true})");
            $this->assertIsArray($space);
            $this->assertArrayHasKey('name', $space);
            $this->assertSame('_vspace', $space['name']);

            [$space] = $driver->query("return box.space._vspace:get({id}):tomap({names_only = true})", [
                'id' => $space['id'],
            ]);
            $this->assertIsArray($space);
            $this->assertArrayHasKey('name', $space);
            $this->assertSame('_vspace', $space['name']);

            $spaces = $database->fetch()
                ->from($database->getBuckets(Bucket::class))
                ->query("return box.space._vspace:get({id}):tomap({names_only = true})", [
                    'id' => $space['id'],
                ]);
            $this->assertIsArray($spaces);
            $this->assertCount(1, $spaces);

            $space = $database->fetchOne()
                ->from($database->getBuckets(Bucket::class))
                ->query("return box.space._vspace:get({id}):tomap({names_only = true})", [
                    'id' => $space['id'],
                ]);
            $this->assertIsArray($space);
            $this->assertArrayHasKey('name', $space);
            $this->assertSame('_vspace', $space['name']);

        } else {
            $this->markTestSkipped(get_class($driver) . ' is not supported');
        }
    }
}
