<?php

namespace Basis\Sharding\Test;

use Basis\Sharding\Database;
use Basis\Sharding\Driver\Runtime;
use Basis\Sharding\Entity\Storage;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Cache\Adapter\ArrayAdapter;

class CachingTest extends TestCase
{
    public function test()
    {
        $database = new Database(new Runtime(), cache: new ArrayAdapter());
        $database->locator->stats = [];
        $database->find(Storage::class);
        $this->assertNotCount(0, $database->locator->stats);
        $stats = $database->locator->stats;
        $database->find(Storage::class);
        $database->find(Storage::class);
        $database->find(Storage::class);
        $database->find(Storage::class);

        $this->assertSame($stats, $database->locator->stats);
    }
}
