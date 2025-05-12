<?php

declare(strict_types=1);

namespace Basis\Sharding\Test;

use Basis\Sharding\Database;
use Basis\Sharding\Driver\Runtime;
use Basis\Sharding\Entity\Bucket;
use Basis\Sharding\Entity\Storage;
use Basis\Sharding\Entity\Topology;
use Basis\Sharding\Schema;
use Basis\Sharding\Job\Configure;
use Basis\Sharding\Job\Migrate;
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

        $schema->register(Activity::class);
        foreach (range(1, 10) as $_) {
            $database->create(Activity::class, []);
        }

        $this->assertCount(10, $database->find(Activity::class));

        $this->assertCount(1, $database->find(Topology::class));
        $this->assertCount(1, $database->getBuckets(Activity::class));

        $next = $database->dispatch(new Configure(Activity::class, shards: 2));

        $this->assertSame($next->status, Topology::DRAFT_STATUS);
        $this->assertCount(2, $database->find(Topology::class));

        $this->assertCount(1, $database->getBuckets(Activity::class));
        [$bucket] = $database->getBuckets(Activity::class);
        $this->assertSame($bucket->version, 1);

        $this->assertSame(1, $database->locator->getTopology(Activity::class)->version);

        try {
            $database->dispatch(new Configure(Activity::class, shards: 3));
            $this->assertNull("Draft reconfigured");
        } catch (Exception $e) {
            $this->assertSame($e->getMessage(), "Topology is not ready");
        }

        $this->assertCount(2, $database->find(Topology::class));

        $database->dispatch(new Migrate('telemetry'));

        $buckets = $database->getBuckets(Activity::class);
        $this->assertSame(2, $database->locator->getTopology(Activity::class)->version);
        $this->assertCount(2, $buckets);
        $this->assertSame($buckets[0]->version, 2);
        $this->assertSame($buckets[1]->version, 2);

        $this->assertCount(10, $database->find(Activity::class));
    }
}
