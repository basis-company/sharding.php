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
        $this->assertSame(array_keys($schema->segments), ['sharding_core', 'sharding_sequence']);
        $this->assertSame($schema->getTableSegment('sharding_bucket')->fullname, 'sharding_core');
        $this->assertSame($schema->getTableSegment('sharding_sequence')->fullname, 'sharding_sequence');
        $this->assertSame($schema->getTableSegment('sharding_storage')->fullname, 'sharding_core');
    }

    public function testSerialization()
    {
        $schema = new Schema();
        $schema->register(User::class);
        $model = $schema->getClassModel(User::class);
        $serialized = serialize($model);
        $unserialized = unserialize($serialized);
        $this->assertEquals($model, $unserialized);
    }

    public function testDuplicateClass()
    {
        $schema = new Schema();
        $this->expectException(Exception::class);
        $this->expectExceptionMessage('Class ' . User::class . ' already registered');
        $schema->register(User::class);
        $schema->register(User::class);
    }

    public function testDomainCasting()
    {
        $schema = new Schema();
        $schema->register(User::class);
        $this->assertSame(array_keys($schema->segments), ['sharding_core', 'sharding_sequence', 'test']);
        $this->assertSame($schema->getSegmentByName('test')->getClasses(), [User::class]);
        $this->assertSame($schema->getClassTable(User::class), 'test_user');
    }

    public function testSegments()
    {
        $schema = new Schema();
        $schema->register(Post::class);
        $this->assertSame(array_keys($schema->segments), ['sharding_core', 'sharding_sequence', 'test_posts']);
        $this->assertSame($schema->segments['test_posts']->getClasses(), [Post::class]);
        $this->assertSame($schema->getClassTable(Post::class), 'test_post');
    }
}
