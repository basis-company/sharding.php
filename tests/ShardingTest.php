<?php

declare(strict_types=1);

namespace Basis\Sharded\Test;

use Basis\Sharded\Database;
use Basis\Sharded\Driver\Runtime;
use Basis\Sharded\Entity\Storage;
use Basis\Sharded\Entity\Topology;
use Basis\Sharded\Schema;
use Basis\Sharded\Task\Configure;
use Basis\Sharded\Test\Entity\Activity;
use Basis\Sharded\Test\Entity\Stage;
use PHPUnit\Framework\TestCase;

class ShardingTest extends TestCase
{
    public function testTopology()
    {
        $schema = new Schema();
        $database = new Database(new Runtime(), $schema);

        $schema->register(Activity::class);
        $segment = $schema->getClassSegment(Activity::class);
        $this->assertTrue($segment->isSharded());

        [$bucket] = $database->locator->getBuckets(Activity::class, create: true);
        $this->assertSame($bucket->version, 1);
        $this->assertCount(1, $database->find(Topology::class));

        $schema->register(Stage::class);
        $segment = $schema->getClassSegment(Stage::class);
        $this->assertTrue($segment->isSharded());

        [$bucket] = $database->locator->getBuckets(Stage::class, create: true);
        $this->assertSame($bucket->version, 1);
        $this->assertCount(2, $database->find(Topology::class));

        $topology = $database->dispatch((new Configure('telemetry'))->shards(2));
        $this->assertSame($topology->version, 2);
        $this->assertSame($topology->shards, 2);
        $this->assertCount(3, $database->find(Topology::class));

        $topology = $database->dispatch((new Configure('telemetry'))->replicas(1));
        $this->assertSame($topology->version, 3);
        $this->assertSame($topology->shards, 2);
        $this->assertSame($topology->replicas, 1);
        $this->assertCount(4, $database->find(Topology::class));
    }
}
