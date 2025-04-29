<?php

namespace Basis\Sharded\Test;

use Basis\Sharded\Database;
use Basis\Sharded\Driver\Runtime;
use Basis\Sharded\Entity\Storage;
use Basis\Sharded\Schema;
use Basis\Sharded\Test\Entity\Activity;
use Basis\Sharded\Test\Entity\Stage;
use PHPUnit\Framework\TestCase;

class ShardingTest extends TestCase
{
    public function testCasting()
    {
        $schema = new Schema();
        $database = new Database(new Runtime(), $schema);
        $database->create(Storage::class, ['type' => 'runtime']);
        $database->create(Storage::class, ['type' => 'runtime']);
        $database->create(Storage::class, ['type' => 'runtime']);
        $this->assertCount(4, $database->find(Storage::class));

        $schema->register(Activity::class);
        $segment = $schema->getClassSegment(Activity::class);
        $this->assertTrue($segment->isSharded());

        [$bucket] = $database->locator->getBuckets(Activity::class, create: true);
        $this->assertNotSame($bucket->version, 0);

        $schema->register(Stage::class);
        $segment = $schema->getClassSegment(Stage::class);
        $this->assertTrue($segment->isSharded());

        [$bucket] = $database->locator->getBuckets(Stage::class, create: true);
        $this->assertNotSame($bucket->version, 0);
    }
}
