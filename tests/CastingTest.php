<?php

namespace Basis\Sharded\Test;

use Basis\Sharded\Database;
use Basis\Sharded\Driver\Runtime;
use Basis\Sharded\Entity\Storage;
use Basis\Sharded\Schema;
use Basis\Sharded\Test\Entity\Post;
use Basis\Sharded\Test\Entity\User;
use PHPUnit\Framework\TestCase;

class CastingTest extends TestCase
{
    public function testCasting()
    {
        $schema = new Schema();
        $schema->register(Post::class);
        $schema->register(User::class);

        $database = new Database(new Runtime(), $schema);
        $database->create(Storage::class, ['type' => 'runtime']);
        $this->assertCount(2, $database->find(Storage::class));

        // first storage was casted using class locator interface implementation (always first)
        $database->locator->getBuckets(Post::class, init: true);
        $this->assertTrue($database->getStorageDriver(1)->hasTable('test_post'));
        $this->assertFalse($database->getStorageDriver(2)->hasTable('test_post'));

        // empty storage is casted using standard locator (driver usage api)
        $database->locator->getBuckets(User::class, init: true);
        $this->assertFalse($database->getStorageDriver(1)->hasTable('test_user'));
        $this->assertTrue($database->getStorageDriver(2)->hasTable('test_user'));
    }
}
