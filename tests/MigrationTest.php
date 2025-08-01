<?php

declare(strict_types=1);

namespace Basis\Sharding\Test;

use Basis\Sharding\Database;
use Basis\Sharding\Driver\Runtime;
use Basis\Sharding\Entity\Bucket;
use Basis\Sharding\Entity\Storage;
use Basis\Sharding\Entity\Topology;
use Basis\Sharding\Job\Cleanup;
use Basis\Sharding\Schema;
use Basis\Sharding\Job\Configure;
use Basis\Sharding\Job\Upgrade;
use Basis\Sharding\Test\Entity\Activity;
use Exception;
use PHPUnit\Framework\TestCase;

class MigrationTest extends TestCase
{
    public function testMigration()
    {
        $schema = new Schema();
        $database = new Database(new Runtime(), $schema);
        $database->create(Storage::class, ['type' => 'runtime']);
        $database->create(Storage::class, ['type' => 'runtime']);
        $database->create(Storage::class, ['type' => 'runtime']);
        $database->create(Storage::class, ['type' => 'runtime']);
        $database->create(Storage::class, ['type' => 'runtime']);

        $schema->register(Activity::class);
        foreach (range(1, 10) as $_) {
            $database->create(Activity::class, []);
        }

        $this->assertCount(10, $database->find(Activity::class));

        $this->assertCount(1, $database->find(Topology::class));
        $this->assertCount(1, $database->getBuckets(Activity::class));

        $next = $database->dispatch(new Configure(Activity::class, shards: 2, replicas: 1));

        $this->assertSame($next->status, Topology::DRAFT_STATUS);
        $this->assertCount(2, $database->find(Topology::class));

        $this->assertCount(1, $database->getBuckets(Activity::class));
        $buckets = $database->getBuckets(Activity::class);
        $writable = $database->getBuckets(Activity::class, writable: true);
        $this->assertCount(1, $buckets);
        $this->assertCount(1, $writable);
        $this->assertSame($buckets[0]->id, $writable[0]->id);
        $this->assertSame($buckets[0]->version, 1);

        $this->assertSame(1, $database->locator->getTopology(Activity::class)->version);

        try {
            $database->dispatch(new Configure(Activity::class, shards: 3));
            $this->assertNull("Draft reconfigured");
        } catch (Exception $e) {
            $this->assertSame($e->getMessage(), "Topology is not ready");
        }

        $this->assertCount(2, $database->find(Topology::class));
        $database->dispatch(new Upgrade(Activity::class, pageSize: 1, iterations: 1));
        $buckets = $database->getBuckets(Activity::class);
        $this->assertSame($buckets[0]->version, 1);

        $writableStoragesKeys = array_map(
            fn($bucket) => $bucket->storage,
            $database->find(Bucket::class, ['name' => $buckets[0]->name, 'version' => 2, 'replica' => 0]),
        );

        $this->assertCount(2, $writableStoragesKeys);
        $writableStorages = array_map(fn($key) => $database->getStorage($key)->getDriver(), $writableStoragesKeys);

        $readableStorages = array_map(
            fn($bucket) => $database->getStorage($bucket->storage)->getDriver(),
            array_filter(
                $database->find(Bucket::class, ['name' => $buckets[0]->name, 'version' => 2]),
                fn($bucket) => !in_array($bucket->storage, $writableStoragesKeys),
            )
        );

        $this->assertCount(2, $readableStorages);

        $this->assertCount(1, array_merge(
            ...array_map(fn($storage) => $storage->find('telemetry_activity'), $writableStorages)
        ));

        $this->assertCount(1, array_merge(
            ...array_map(fn($storage) => $storage->find('telemetry_activity'), $readableStorages)
        ));

        [$first] = array_merge(
            ...array_map(fn($storage) => $storage->find('telemetry_activity'), $writableStorages)
        );

        $database->dispatch(new Upgrade(Activity::class, pageSize: 2, iterations: 1));

        $this->assertCount(3, array_merge(
            ...array_map(fn($storage) => $storage->find('telemetry_activity'), $writableStorages)
        ));
        $this->assertCount(3, array_merge(
            ...array_map(fn($storage) => $storage->find('telemetry_activity'), $readableStorages)
        ));

        $database->dispatch(new Upgrade(Activity::class, pageSize: 2, iterations: 2));
        $this->assertCount(7, array_merge(
            ...array_map(fn($storage) => $storage->find('telemetry_activity'), $writableStorages)
        ));
        $this->assertCount(7, array_merge(
            ...array_map(fn($storage) => $storage->find('telemetry_activity'), $readableStorages)
        ));

        // register change
        $database->update(Activity::class, $first['id'], ['type' => 27]);
        $this->assertSame($database->findOne(Activity::class, ['id' => $first['id']])->type, 27);

        // final migration
        $database->dispatch(new Upgrade(Activity::class));

        $buckets = $database->getBuckets(Activity::class);
        $writable = $database->getBuckets(Activity::class, writable: true);
        $this->assertSame(2, $database->locator->getTopology(Activity::class)->version);
        $this->assertCount(2, $buckets);
        $this->assertCount(2, $writable);
        $this->assertNotSame($buckets, $writable);
        $this->assertSame($buckets[0]->version, 2);
        $this->assertSame($buckets[1]->version, 2);
        $this->assertSame($writable[0]->version, 2);
        $this->assertSame($writable[1]->version, 2);

        $this->assertCount(0, $database->getStorage($buckets[0]->storage)->getDriver()->getListeners('telemetry_activity'));
        $this->assertCount(0, $database->getStorage($buckets[1]->storage)->getDriver()->getListeners('telemetry_activity'));
        $this->assertCount(1, $database->getStorage($writable[0]->storage)->getDriver()->getListeners('telemetry_activity'));
        $this->assertCount(1, $database->getStorage($writable[1]->storage)->getDriver()->getListeners('telemetry_activity'));

        $this->assertCount(10, $database->find(Activity::class));
        $this->assertSame($database->findOne(Activity::class, ['id' => $first['id']])->type, 27);

        $stale = $database->find(Bucket::class, ['name' => $buckets[0]->name, 'version' => 1]);
        $this->assertCount(1, $stale);
        $staleStorage = $database->getStorage($stale[0]->storage);
        $this->assertTrue($staleStorage->getDriver()->hasTable('telemetry_activity'));

        $database->dispatch(new Cleanup(Activity::class));
        $this->assertFalse($staleStorage->getDriver()->hasTable('telemetry_activity'));

        $stale = $database->find(Bucket::class, ['name' => $buckets[0]->name, 'version' => 1]);
        $this->assertCount(0, $stale);

        $this->expectExceptionMessage("No stale buckets found");
        $database->dispatch(new Cleanup(Activity::class));
    }
}
