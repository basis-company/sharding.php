<?php

namespace Basis\Sharding\Test;

use Basis\Sharding\Database;
use Basis\Sharding\Driver\Runtime;
use Basis\Sharding\Entity\Storage;
use Basis\Sharding\Schema;
use Basis\Sharding\Test\Entity\Post;
use Basis\Sharding\Test\Entity\User;
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
        $database->locator->getBuckets($database->schema->getModel(Post::class), writable: true);
        $this->assertTrue($database->getStorage(1)->getDriver()->hasTable('test_post'));
        $this->assertFalse($database->getStorage(2)->getDriver()->hasTable('test_post'));

        // empty storage is casted using standard locator (driver usage api)
        $database->locator->getBuckets($database->schema->getModel(User::class), writable: true);
        $this->assertFalse($database->getStorage(1)->getDriver()->hasTable('test_user'));
        $this->assertTrue($database->getStorage(2)->getDriver()->hasTable('test_user'));
    }
}
