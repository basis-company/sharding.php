<?php

declare(strict_types=1);

namespace Basis\Sharding\Test;

use Basis\Sharding\Database;
use Basis\Sharding\Driver\Runtime;
use Basis\Sharding\Entity\Bucket;
use Basis\Sharding\Entity\Change;
use Basis\Sharding\Entity\Storage;
use Basis\Sharding\Entity\Topology;
use Basis\Sharding\Schema;
use Basis\Sharding\Job\Configure;
use Basis\Sharding\Job\Replicate;
use Basis\Sharding\Test\Entity\Activity;
use Basis\Sharding\Test\Entity\Stage;
use Basis\Sharding\Test\Entity\User;
use PHPUnit\Framework\TestCase;

class ShardingTest extends TestCase
{
    public function testReplicaSelection()
    {
        $database = new Database(new Runtime());
        $database->schema->register(Activity::class);
        $database->create(Storage::class, ['type' => 'runtime']);
        $database->create(Storage::class, ['type' => 'runtime']);

        $database->dispatch(new Configure(Activity::class, 0, 2));
        $buckets = $database->getBuckets(Activity::class);
        $this->assertCount(1, $buckets);

        foreach (range(1, 5) as $_) {
            $buckets = array_merge($buckets, $database->getBuckets(Activity::class));
        }
        $this->assertCount(2, array_unique(array_map(fn ($bucket) => $bucket->id, $buckets)));
    }

    public function testTopology()
    {
        $schema = new Schema();
        $database = new Database(new Runtime(), $schema);

        $schema->register(Activity::class);
        $segment = $schema->getClassSegment(Activity::class);
        $this->assertTrue($segment->isSharded());

        [$bucket] = $database->locator->getBuckets(Activity::class, writable: true);
        $database->delete($bucket);

        $this->assertSame($bucket->version, 1);
        $this->assertCount(1, $database->find(Topology::class));
        $topology = $database->findOrFail(Topology::class, []);
        $this->assertSame($topology->version, 1);
        $this->assertSame($topology->status, Topology::READY_STATUS);

        $schema->register(Stage::class);
        $segment = $schema->getClassSegment(Stage::class);
        $this->assertTrue($segment->isSharded());

        [$bucket] = $database->locator->getBuckets(Stage::class, writable: true);
        $this->assertSame($bucket->version, 1);
        $this->assertCount(2, $database->find(Topology::class));

        $topology = $database->dispatch((new Configure(Activity::class))->shards(2));
        $this->assertSame($topology->version, 2);
        $this->assertSame($topology->shards, 2);
        $this->assertSame($topology->status, Topology::READY_STATUS);
        $this->assertCount(3, $database->find(Topology::class));

        $topology = $database->dispatch((new Configure(Activity::class))->replicas(1)->shards(1));
        $this->assertSame($topology->version, 3);
        $this->assertSame($topology->shards, 1);
        $this->assertSame($topology->replicas, 1);
        $this->assertSame($topology->status, Topology::READY_STATUS);
        $this->assertCount(4, $database->find(Topology::class));
    }

    public function testReplication()
    {
        $schema = new Schema();
        $database = new Database(new Runtime(), $schema);

        $schema->register(Activity::class);
        $topology = $database->dispatch((new Configure(Activity::class))->replicas(1));
        $database->create(Storage::class, ['type' => 'runtime']);

        array_map($database->delete(...), $database->find(Bucket::class, ['name' => $topology->name]));
        $this->assertCount(1, $database->getBuckets(Activity::class, []));
        $this->assertCount(1, $database->getBuckets(Activity::class, [], writable: true));
        [$source] = $database->getBuckets(Activity::class, [], writable: true);
        [$destination] = array_values(
            array_filter($database->getBuckets(Activity::class, []), fn($bucket) => $bucket->id != $source->id)
        );

        $activity = $database->create(Activity::class, []);
        $this->assertCount(1, $database->getStorageDriver($source->storage)->find('telemetry_activity', []));
        $this->assertCount(0, $database->getStorageDriver($destination->storage)->find('telemetry_activity', []));
        $this->assertCount(0, $database->find(Activity::class));

        $this->assertCount(1, $database->getStorageDriver($source->storage)->find(Change::getSpaceName()));
        $database->dispatch(new Replicate($source->storage, limit:1));
        $this->assertCount(0, $database->getStorageDriver($source->storage)->find(Change::getSpaceName()));
        $this->assertCount(1, $database->getStorageDriver($destination->storage)->find('telemetry_activity'));

        $database->update($activity, ['type' => 27]);
        $this->assertCount(1, $database->getStorageDriver($source->storage)->find(Change::getSpaceName()));

        $database->dispatch(new Replicate($source->storage, limit:1));
        $this->assertCount(0, $database->getStorageDriver($source->storage)->find(Change::getSpaceName()));
        $this->assertCount(1, $database->getStorageDriver($destination->storage)->find('telemetry_activity'));
        [$replicated] = $database->getStorageDriver($destination->storage)->find('telemetry_activity');
        $this->assertSame($replicated['type'], 27);

        $database->delete($activity);
        $this->assertCount(1, $database->getStorageDriver($source->storage)->find(Change::getSpaceName()));

        $database->dispatch(new Replicate($source->storage, limit:1));
        $this->assertCount(0, $database->getStorageDriver($source->storage)->find(Change::getSpaceName()));
        $this->assertCount(0, $database->getStorageDriver($destination->storage)->find('telemetry_activity'));
    }

    public function testUuidDistribution()
    {
        $schema = new Schema();
        $database = new Database(new Runtime(), $schema);
        $database->create(Storage::class, ['type' => 'runtime']);

        $schema->register(Activity::class);
        $database->dispatch(new Configure(Activity::class, shards: 2));

        $this->assertCount(1, $database->find(Topology::class));
        $database->create(Activity::class, []);
        $this->assertFalse($database->getStorageDriver(1)->hasTable('telemetry_activity'));
        $this->assertTrue($database->getStorageDriver(2)->hasTable('telemetry_activity'));
        $this->assertCount(1, $database->find(Activity::class));

        $this->assertCount(2, array_filter($database->getBuckets(Activity::class), fn ($bucket) => $bucket->storage));
        foreach (range(1, 9) as $_) {
            $database->create(Activity::class, []);
        }
        $this->assertCount(2, array_filter($database->getBuckets(Activity::class), fn ($bucket) => $bucket->storage));
        $this->assertCount(10, $database->find(Activity::class));

        $this->assertTrue($database->getStorageDriver(1)->hasTable('telemetry_activity'));
        $this->assertTrue($database->getStorageDriver(2)->hasTable('telemetry_activity'));
        $this->assertNotCount(0, $database->getStorageDriver(1)->find('telemetry_activity'));
        $this->assertNotCount(0, $database->getStorageDriver(2)->find('telemetry_activity'));
    }

    public function testIntegerKeyDistribution()
    {
        $schema = new Schema();
        $database = new Database(new Runtime(), $schema);
        $database->create(Storage::class, ['type' => 'runtime']);
        $schema->register(User::class);

        $database->dispatch(new Configure(User::class, shards: 2));

        foreach (range(1, 10) as $n) {
            $database->create(User::class, ['name' => 'user ' . $n]);
        }

        $this->assertCount(10, $database->find(User::class, []));
        $this->assertCount(2, array_filter($database->getBuckets(User::class, []), fn($bucket) => $bucket->storage));
        $this->assertCount(5, $database->driver->find('test_user'));
    }

    public function testCustomDistribution()
    {
        $schema = new Schema();
        $database = new Database(new Runtime(), $schema);
        $database->create(Storage::class, ['type' => 'runtime']);
        $schema->register(Stage::class);
        $database->dispatch(new Configure(Stage::class, shards: 2));

        $this->assertCount(1, $database->find(Topology::class));

        $first = $database->create(Stage::class, ['year' => (int) date('Y'), 'month' => 1]);
        $database->create(Stage::class, ['year' => (int) date('Y'), 'month' => 2]);
        $database->create(Stage::class, ['year' => (int) date('Y'), 'month' => 3]);

        $this->assertCount(2, array_filter($database->getBuckets(Stage::class, []), fn($bucket) => $bucket->storage));
        $this->assertCount(3, $database->find(Stage::class, []));

        $last = $database->create(Stage::class, ['year' => (int) date('Y') - 1, 'month' => 12]);

        $this->assertCount(2, array_filter($database->getBuckets(Stage::class, []), fn($bucket) => $bucket->storage));

        $this->assertCount(1, $database->getBuckets(Stage::class, ['year' => date('Y')]));
        $this->assertCount(3, $database->find(Stage::class, ['year' => date('Y')]));

        $this->assertCount(1, $database->getBuckets(Stage::class, ['year' => date('Y') - 1]));
        $this->assertCount(1, $database->find(Stage::class, ['year' => date('Y') - 1]));

        $this->assertCount(2, $database->getBuckets(Stage::class, []));
        $this->assertCount(4, $database->find(Stage::class, []));

        $database->update($first, ['month' => 7]);
        $database->update($last, ['month' => 7]);
        $this->assertCount(2, $database->find(Stage::class, ['month' => 7]));

        $database->delete($last);
    }
}
