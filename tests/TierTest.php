<?php

namespace Basis\Sharding\Test;

use Basis\Sharding\Database;
use Basis\Sharding\Driver\Runtime;
use Basis\Sharding\Entity\Storage;
use Basis\Sharding\Entity\Tier;
use Basis\Sharding\Job\Configure;
use Basis\Sharding\Job\Migrate;
use Basis\Sharding\Test\Entity\Stage;
use PHPUnit\Framework\TestCase;

class TierTest extends TestCase
{
    public function test()
    {
        $database = new Database(new Runtime());
        $default = $database->getStorage(1);
        $this->assertCount(1, $database->find(Tier::class));
        $this->assertSame(1, $database->findOne(Storage::class, [])->tier);

        $cold = $database->create(Tier::class, ['name' => 'cold']);
        $database->create(Storage::class, ['type' => 'runtime', 'tier' => $cold->id]);

        $database->schema->register(Stage::class);
        $database->create(Stage::class, []);
        $this->assertCount(1, $database->find(Stage::class));

        [$bucket] = $database->getBuckets(Stage::class, []);
        $this->assertSame($bucket->storage, $default->id);

        $database->dispatch(new Configure(Stage::class, tier: $cold->id));
        $database->dispatch(new Migrate(Stage::class));

        [$bucket] = $database->getBuckets(Stage::class, []);
        $this->assertSame($bucket->storage, $cold->id);
        $this->assertCount(1, $database->find(Stage::class));
    }
}
