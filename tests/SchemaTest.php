<?php

namespace Basis\Sharding\Test;

use Basis\Sharding\Schema;
use Basis\Sharding\Test\Entity\Post;
use Basis\Sharding\Test\Entity\User;
use Exception;
use PHPUnit\Framework\TestCase;

class SchemaTest extends TestCase
{
    public function testSystemBucket()
    {
        $schema = new Schema();
        $this->assertSame($schema->getModel('sharding_bucket')->segment, 'sharding_core');
        $this->assertSame($schema->getModel('sharding_sequence')->segment, 'sharding_sequence');
        $this->assertSame($schema->getModel('sharding_storage')->segment, 'sharding_core');
    }

    public function testModelCasting()
    {
        $schema = new Schema();
        $schema->register(User::class);
        $model = $schema->getModel(User::class);
        $this->assertSame($schema->getModel($model->table), $model);
        $this->assertSame($schema->getModel(str_replace('_', '.', $model->table)), $model);

        $model = $schema->register(Post::class);
    }

    public function testSerialization()
    {
        $schema = new Schema();
        $schema->register(User::class);
        $model = $schema->getModel(User::class);
        $serialized = serialize($model);
        $unserialized = unserialize($serialized);
        $this->assertEquals($model, $unserialized);
    }

    public function testDuplicateClass()
    {
        $schema = new Schema();
        $this->expectException(Exception::class);
        $this->expectExceptionMessage('Model ' . User::class . ' already registered');
        $schema->register(User::class);
        $schema->register(User::class);
    }

    public function testDomainCasting()
    {
        $schema = new Schema();
        $schema->register(User::class);
        $this->assertSame(array_map(fn ($model) => $model->class, $schema->getModels('test')), [User::class]);
        $this->assertSame(array_map(fn ($model) => $model->segment, $schema->getModels('test')), ['test']);
        $this->assertSame($schema->getTable(User::class), 'test_user');
    }

    public function testSegments()
    {
        $schema = new Schema();
        $schema->register(Post::class);
        $this->assertSame(array_map(fn ($model) => $model->class, $schema->getModels('test_posts')), [Post::class]);
        $this->assertSame($schema->getTable(Post::class), 'test_post');
    }
}
